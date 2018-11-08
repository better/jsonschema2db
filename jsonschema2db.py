import change_case
import csv
import datetime
import iso8601
import json
import os
import random
import sys
import tempfile
import warnings


class JSONSchemaToDatabase:
    '''JSONSchemaToDatabase is the mother class for everything

    :param schema: The JSON schema, as a native Python dict
    :param database_flavor: Either "postgres" or "redshift"
    :param postgres_schema: (optional) A string denoting a postgres schema (namespace) under which all tables will be created
    :param debug: (optional) Set this to True if you want all queries to be printed to stderr
    :param item_col_name: (optional) The name of the main object key (default is 'item_id')
    :param item_col_type: (optional) Type of the main object key (uses the type identifiers from JSON Schema). Default is 'integer'
    :param prefix_col_name: (optional) Postgres column name identifying the subpaths in the object (default is 'prefix')
    :param abbreviations: (optional) A string to string mapping containing replacements applied to each part of the path
    :param s3_client: (optional, Redshift only) A boto3 client object used for copying data through S3 (if not provided then it will use INSERT statements, which can be very slow)
    :param s3_bucket: (optional, Redshift only) Required with s3_client
    :param s3_prefix: (optional, Redshift only) Optional subdirectory within the S3 bucket
    :param s3_iam_arn: (optional, Redshift only) Extra IAM argument

    Typically you want to instantiate a `JSONSchemaToPostgres` object, and run :func:`create_tables` to create all the tables. After that, insert all data using :func:`insert_items`. Once you're done inserting, run :func:`create_links` to populate all references properly and add foreign keys between tables. Optionally you can run :func:`analyze` finally which optimizes the tables.
    '''
    def __init__(self, schema, database_flavor, postgres_schema=None, debug=False,
                 item_col_name='item_id', item_col_type='integer', prefix_col_name='prefix',
                 abbreviations={},
                 s3_client=None, s3_bucket=None, s3_prefix='jsonschema2db', s3_iam_arn=None):
        self._database_flavor = database_flavor
        self._debug = debug
        self._table_definitions = {}
        self._links = {}
        self._backlinks = {}
        self._postgres_schema = postgres_schema
        self._item_col_name = item_col_name
        self._item_col_type = item_col_type
        self._prefix_col_name = prefix_col_name
        self._abbreviations = abbreviations
        self._table_comments = {}
        self._column_comments = {}

        # Redshift-specific properties
        self._s3_client = s3_client
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._s3_iam_arn = s3_iam_arn

        # Various counters used for diagnostics during insertions
        self.failure_count = {}  # path -> count
        self.json_path_count = {}  # json path -> count

        # Walk the schema and build up the translation tables
        self._translation_tree = self._traverse(schema, schema, comment=schema.get('comment'))

        # Need to compile all the backlinks that uniquely identify a parent and add columns for them
        for child_table in self._backlinks:
            if len(self._backlinks[child_table]) != 1:
                # Need a unique path on the parent table for this to make sense
                continue
            parent_table, ref_col_name, _ = list(self._backlinks[child_table])[0]
            self._backlinks[child_table] = (parent_table, ref_col_name)
            self._table_definitions[child_table][ref_col_name] = 'link'
            self._links.setdefault(child_table, {})[ref_col_name] = (None, parent_table)

        # Construct tables and columns
        self._table_columns = {}
        max_column_length = {'postgres': 63, 'redshift': 127}[self._database_flavor]
        for table, column_types in self._table_definitions.items():
            for column in column_types.keys():
                if len(column) > max_column_length:
                    warnings.warn('Ignoring_column because it is too long: %s.%s' % (table, column))
            columns = sorted(col for col in column_types.keys() if 0 < len(col) <= max_column_length)
            self._table_columns[table] = columns

    def _table_name(self, path):
        return '__'.join(change_case.ChangeCase.camel_to_snake(self._abbreviations.get(p, p)) for p in path)

    def _column_name(self, path):
        return self._table_name(path)  # same

    def _execute(self, cursor, query, args=None, query_ok_to_print=True):
        if self._debug and query_ok_to_print:
            print(query, file=sys.stderr)
        cursor.execute(query, args)

    def _traverse(self, schema, tree, path=tuple(), table='root', parent=None, comment=None, json_path=tuple()):
        # Computes a bunch of stuff
        # 1. A list of tables and columns (used to create tables dynamically)
        # 2. A tree (dicts of dicts) with a mapping for each fact into tables (used to map data)
        # 3. Links between entities
        if type(tree) != dict:
            warnings.warn('%s.%s: Broken subtree' % (table, self._column_name(path)))
            return

        if parent is not None:
            self._backlinks.setdefault(table, set()).add(parent)

        if table not in self._table_definitions:
            self._table_definitions[table] = {}
            if comment:
                self._table_comments[table] = comment

        definition = None
        new_json_path = json_path
        while '$ref' in tree:
            p = tree['$ref'].lstrip('#').lstrip('/').split('/')
            # TODO: I actually don't think it's required JSON schema that all definitions have to go under #/definitions
            if len(p) != 2 and p[0] != 'definitions':
                warnings.warn('%s.%s: Broken reference: %s' % (table, self._column_name(path), tree['$ref']))
                return
            _, definition = p
            if definition not in schema['definitions']:
                warnings.warn('%s.%s: Broken definition: %s' % (table, self._column_name(path), definition))
                return
            tree = schema['definitions'][definition]
            new_json_path = ('#', 'definitions', definition)

        special_keys = set(tree.keys()).intersection(['oneOf', 'allOf', 'anyOf'])
        if special_keys:
            res = {}
            for p in special_keys:
                for q in tree[p]:
                    res.update(self._traverse(schema, q, path, table, json_path=new_json_path))
            return res  # This is a special node, don't store any more information
        elif 'enum' in tree:
            self._table_definitions[table][self._column_name(path)] = 'enum'
            if 'comment' in tree:
                self._column_comments.setdefault(table, {})[self._column_name(path)] = tree['comment']
            res = {'_column': self._column_name(path), '_type': 'enum'}
        elif 'type' not in tree:
            res = {}
            warnings.warn('%s.%s: Type info missing' % (table, self._column_name(path)))
        elif tree['type'] == 'object':
            res = {}
            if 'patternProperties' in tree:
                # Always create a new table for the pattern properties
                if len(tree['patternProperties']) > 1:
                    warnings.warn('%s.%s: Multiple patternProperties, will ignore all except first' % (table, self._column_name(path)))
                for p in tree['patternProperties']:
                    ref_col_name = table + '_id'
                    res['*'] = self._traverse(schema, tree['patternProperties'][p], tuple(), self._table_name(path), (table, ref_col_name, self._column_name(path)), tree.get('comment'), new_json_path + (p,))
                    break
            elif 'properties' in tree:
                if definition:
                    # This is a shared definition, so create a new table (if not already exists)
                    if path == tuple():
                        ref_col_name = self._table_name([definition]) + '_id'
                    else:
                        ref_col_name = self._column_name(path) + '_id'
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], (p, ), self._table_name([definition]), (table, ref_col_name, self._column_name(path)), tree.get('comment'), new_json_path + (p,))
                    self._table_definitions[table][ref_col_name] = 'link'
                    self._links.setdefault(table, {})[ref_col_name] = ('/'.join(path), self._table_name([definition]))
                else:
                    # Standard object, just traverse recursively
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], path + (p,), table, parent, tree.get('comment'), new_json_path + (p,))
            else:
                warnings.warn('%s.%s: Object with neither properties nor patternProperties' % (table, self._column_name(path)))
        else:
            if tree['type'] == 'null':
                res = {}
            elif tree['type'] not in ['string', 'boolean', 'number', 'integer']:
                warnings.warn('%s.%s: Type error: %s' % (table, self._column_name(path), tree['type']))
                res = {}
            else:
                if definition in ['date', 'timestamp']:
                    t = definition
                else:
                    t = tree['type']
                self._table_definitions[table][self._column_name(path)] = t
                if 'comment' in tree:
                    self._column_comments.setdefault(table, {})[self._column_name(path)] = tree['comment']
                res = {'_column': self._column_name(path), '_type': t}

        res['_table'] = table
        res['_suffix'] = '/'.join(path)
        res['_json_path'] = '/'.join(json_path)
        self.json_path_count['/'.join(json_path)] = 0

        return res

    def _coerce_type(self, t, value):
        ''' Returns a two-tuple (is_valid, new_value) where new_value is properly coerced. '''
        try:
            if t == 'number':
                return type(value) != bool, float(value)
            elif t == 'integer':
                return type(value) != bool, int(value)
            elif t == 'boolean':
                return type(value) == bool, value
            elif t == 'timestamp':
                if type(value) == datetime.datetime:
                    return True, value
                return True, iso8601.parse_date(value)
            elif t == 'date':
                if type(value) == datetime.date:
                    return True, value
                return True, datetime.date(*(int(z) for z in value.split('-')))
            elif t == 'string':
                # Allow coercing ints/floats, but nothing else
                return type(value) in [str, int, float], str(value)
            elif t == 'enum':
                return type(value) == str, str(value)
        except:
            pass
        return False, None

    def _flatten_dict(self, data, res=None, path=tuple()):
        if res is None:
            res = []
        if type(data) == dict:
            for k, v in data.items():
                self._flatten_dict(v, res, path+(k,))
        else:
            res.append((path, data))
        return res

    def _postgres_table_name(self, table):
        if self._postgres_schema is None:
            return '"%s"' % table
        else:
            return '"%s"."%s"' % (self._postgres_schema, table)

    def create_tables(self, con):
        '''Creates tables

        :param con: psycopg2 connection object
        '''
        postgres_types = {'boolean': 'bool', 'number': 'float', 'string': 'text', 'enum': 'text', 'integer': 'bigint', 'timestamp': 'timestamptz', 'date': 'date', 'link': 'integer'}
        with con.cursor() as cursor:
            if self._postgres_schema is not None:
                self._execute(cursor, 'drop schema if exists %s cascade' % self._postgres_schema)
                self._execute(cursor, 'create schema %s' % self._postgres_schema)
            for table, columns in self._table_columns.items():
                types = [self._table_definitions[table][column] for column in columns]
                id_data_type = {'postgres': 'serial', 'redshift': 'int identity(1, 1) not null'}[self._database_flavor]

                create_q = 'create table %s (id %s, "%s" %s not null, "%s" text not null, %s unique ("%s", "%s"), unique (id))' % \
                           (self._postgres_table_name(table), id_data_type, self._item_col_name, postgres_types[self._item_col_type], self._prefix_col_name,
                            ''.join('"%s" %s, ' % (c, postgres_types[t]) for c, t in zip(columns, types)),
                            self._item_col_name, self._prefix_col_name)
                self._execute(cursor, create_q)
                if table in self._table_comments:
                    self._execute(cursor, 'comment on table %s is %%s' % self._postgres_table_name(table), (self._table_comments[table],))
                for c in columns:
                    if c in self._column_comments.get(table, {}):
                        self._execute(cursor, 'comment on column %s."%s" is %%s' % (self._postgres_table_name(table), c), (self._column_comments[table][c],))

    def _insert_items_generate_rows(self, items, count):
        # Helper function to generate data row by row for insertion
        for item_id, data in items:
            if type(data) == dict:
                data = self._flatten_dict(data)
            res = {}
            for path, value in data:
                if value is None:
                    continue

                subtree = self._translation_tree
                res.setdefault(subtree['_table'], {}).setdefault('', {})
                if count:
                    self.json_path_count[subtree['_json_path']] += 1

                for index, path_part in enumerate(path):
                    if '*' in subtree:
                        subtree = subtree['*']
                    elif not subtree.get(path_part):
                        if count:
                            self.failure_count[path] = self.failure_count.get(path, 0) + 1
                        break
                    else:
                        subtree = subtree[path_part]

                    # Compute the prefix, add an empty entry (TODO: should make the prefix customizeable)
                    table, suffix = subtree['_table'], subtree['_suffix']
                    prefix_suffix = '/' + '/'.join(path[:(index+1)])
                    assert prefix_suffix.endswith(suffix)
                    prefix = prefix_suffix[:len(prefix_suffix)-len(suffix)].rstrip('/')
                    res.setdefault(table, {}).setdefault(prefix, {})
                    if count:
                        self.json_path_count[subtree['_json_path']] += 1

                # Leaf node with value, validate and prepare for insertion
                if '_column' not in subtree:
                    if count:
                        self.failure_count[path] = self.failure_count.get(path, 0) + 1
                    continue
                col, t = subtree['_column'], subtree['_type']
                if table not in self._table_columns:
                    if count:
                        self.failure_count[path] = self.failure_count.get(path, 0) + 1
                    continue
                is_valid, new_value = self._coerce_type(t, value)
                if not is_valid:
                    if count:
                        self.failure_count[path] = self.failure_count.get(path, 0) + 1
                    continue

                res.setdefault(table, {}).setdefault(prefix, {})[col] = new_value

            # Compile table rows for this item
            for table, table_values in res.items():
                for prefix, row_values in table_values.items():
                    row_array = [item_id, prefix] + [row_values.get(t) for t in self._table_columns[table]]
                    yield (table, row_array)

    def insert_items(self, con, items, mutate=True, count=False):
        ''' Inserts data into database.

        :param con: psycopg2 connection object
        :param items: is an iterable of tuples `(item id, values)` where `values` is either:

        - A nested dict conforming to the JSON spec
        - A list (or iterator) of pairs where the first item in the pair is a tuple specifying the path, and the second value in the pair is the value.

        :param mutate: If this is set to `False`, nothing is actually inserted. This might be useful if you just want to validate data.
        :param count: if set to `True`, it will count some things. Defaults to `False`.

        Updates `self.failure_count`, a dict counting the number of failures for paths (keys are tuples, values are integers).

        This function has an optimized strategy for Redshift, where it writes the data to temporary files, copies those to S3, and uses the `COPY`
        command to ingest the data into Redshift. However this strategy is only used if the `s3_client` is provided to the constructor.
        Otherwise, it will fall back to the Postgres-based method of running batched insertions.

        Note that the Postgres-based insertion builds up huge intermediary datastructures, so it will take a lot more memory.
        '''
        rows = self._insert_items_generate_rows(items=items, count=count)

        if not mutate:
            for table, row in rows:
                # Just exhaust the iterator
                pass

        elif self._database_flavor == 'redshift' and self._s3_client:
            with tempfile.TemporaryDirectory() as tmpdirname, con.cursor() as cursor:
                # Flush the iterator to temporary files on disk
                temp_files, writers, file_objs = {}, {}, []
                for table, row in rows:
                    if table not in temp_files:
                        fn = temp_files[table] = os.path.join(tmpdirname, table + '.csv')
                        f = open(fn, 'wt')
                        writer = csv.writer(f)
                        if self._debug:
                            print('Creating temp file for table', table, 'at', fn, file=sys.stderr)
                        writers[table] = writer
                        file_objs.append(f)

                    writers[table].writerow(row)

                # Close local temp files so all data gets flushed to disk
                for f in file_objs:
                    f.close()

                # Upload all files to S3 and load into Redshift
                # TODO: might want to use a thread pool for this
                batch_random = '%012d' % random.randint(0, 999999999999)
                for table, fn in temp_files.items():
                    s3_path = '/%s/%s/%s.csv' % (self._s3_prefix, batch_random, table)
                    if self._debug:
                        print('Uploading data for table %s from %s (%d bytes) to %s' % (table, fn, os.path.getsize(fn), s3_path), file=sys.stderr)
                    self._s3_client.upload_file(Filename=fn, Bucket=self._s3_bucket, Key=s3_path)

                    query = 'copy %s from \'s3://%s/%s\' csv %s truncatecolumns compupdate off statupdate off' % (
                        self._postgres_table_name(table),
                        self._s3_bucket, s3_path, self._s3_iam_arn and 'iam_role \'%s\'' % self._s3_iam_arn or '')
                    self._execute(cursor, query)
        else:
            # Postgres-based insertion
            with con.cursor() as cursor:
                data_by_table = {}
                for table, row in rows:
                    # Note that this flushes the iterator into an in-memory datastructure, so it will be far less memory efficient than the Redshift strategy
                    data_by_table.setdefault(table, []).append(row)
                for table, data in data_by_table.items():
                    cols = '("%s","%s"%s)' % (self._item_col_name, self._prefix_col_name, ''.join(',"%s"' % c for c in self._table_columns[table]))
                    pattern = '(' + ','.join(['%s'] * len(data[0])) + ')'
                    args = b','.join(cursor.mogrify(pattern, tup) for tup in data)
                    self._execute(cursor, b'insert into %s %s values %s' % (self._postgres_table_name(table).encode(), cols.encode(), args), query_ok_to_print=False)

    def create_links(self, con):
        '''Adds foreign keys between tables.'''
        for from_table, cols in self._links.items():
            for ref_col_name, (prefix, to_table) in cols.items():
                if from_table not in self._table_columns or to_table not in self._table_columns:
                    continue
                args = {
                    'from_table': self._postgres_table_name(from_table),
                    'to_table': self._postgres_table_name(to_table),
                    'ref_col': ref_col_name,
                    'item_col': self._item_col_name,
                    'prefix_col': self._prefix_col_name,
                    'prefix': prefix,
                    }
                update_q = 'update %(from_table)s set "%(ref_col)s" = to_table.id from (select "%(item_col)s", "%(prefix_col)s", id from %(to_table)s) to_table' % args
                if prefix:
                    # Forward reference from table to a definition
                    update_q += ' where %(from_table)s."%(item_col)s" = to_table."%(item_col)s" and %(from_table)s."%(prefix_col)s" || \'/%(prefix)s\' = to_table."%(prefix_col)s"' % args
                else:
                    # Backward definition from a table to its patternProperty parent
                    update_q += ' where %(from_table)s."%(item_col)s" = to_table."%(item_col)s" and strpos(%(from_table)s."%(prefix_col)s", to_table."%(prefix_col)s") = 1' % args

                alter_q = 'alter table %(from_table)s add constraint fk_%(ref_col)s foreign key ("%(ref_col)s") references %(to_table)s (id)' % args
                with con.cursor() as cursor:
                    self._execute(cursor, update_q)
                    self._execute(cursor, alter_q)

    def analyze(self, con):
        '''Runs `analyze` on each table. This improves performance.

        See the `Postgres documentation for Analyze <https://www.postgresql.org/docs/9.1/static/sql-analyze.html>`_
        '''
        with con.cursor() as cursor:
            for table in self._table_columns.keys():
                self._execute(cursor, 'analyze %s' % self._postgres_table_name(table))


class JSONSchemaToPostgres(JSONSchemaToDatabase):
    '''Shorthand for JSONSchemaToDatabase(..., database_flavor='postgres')'''
    def __init__(self, *args, **kwargs):
        kwargs['database_flavor'] = 'postgres'
        return super(JSONSchemaToPostgres, self).__init__(*args, **kwargs)


class JSONSchemaToRedshift(JSONSchemaToDatabase):
    '''Shorthand for JSONSchemaToDatabase(..., database_flavor='redshift')'''
    def __init__(self, *args, **kwargs):
        kwargs['database_flavor'] = 'redshift'
        return super(JSONSchemaToRedshift, self).__init__(*args, **kwargs)
