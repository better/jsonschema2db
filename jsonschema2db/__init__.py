import change_case
import datetime
import iso8601
import json
import warnings


class JSONSchemaToPostgres:
    def __init__(self, schema, postgres_schema=None, item_col_name='item_id', item_col_type='integer', prefix_col_name='prefix', abbreviations={}):
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
        for table, column_types in self._table_definitions.items():
            for column in column_types.keys():
                if len(column) >= 64:
                    warnings.warn('Ignoring_column because it is too long: %s.%s' % (table, column))
            columns = sorted(col for col in column_types.keys() if 0 < len(col) < 64)
            self._table_columns[table] = columns

    def _table_name(self, path):
        return '__'.join(change_case.ChangeCase.camel_to_snake(self._abbreviations.get(p, p)) for p in path)

    def _column_name(self, path):
        return self._table_name(path)  # same

    def _traverse(self, schema, tree, path=tuple(), table='root', parent=None, comment=None):
        # Computes a bunch of stuff
        # 1. A list of tables and columns (used to create tables dynamically)
        # 2. A tree (dicts of dicts) with a mapping for each fact into tables (used to map data)
        # 3. Links between entities
        if type(tree) != dict:
            warnings.warn('Broken subtree: /%s' % '/'.join(path))
            return

        if parent is not None:
            self._backlinks.setdefault(table, set()).add(parent)

        if table not in self._table_definitions:
            self._table_definitions[table] = {}
            if comment:
                self._table_comments[table] = comment

        definition = None
        while '$ref' in tree:
            p = tree['$ref'].lstrip('#').lstrip('/').split('/')
            if len(p) != 2 and p[0] != 'definitions':
                warnings.warn('Broken reference: %s' % tree['$ref'])
                return
            _, definition = p
            if definition not in schema['definitions']:
                warnings.warn('Broken definitions: %s' % definition)
                return
            tree = schema['definitions'][definition]

        special_keys = set(tree.keys()).intersection(['oneOf', 'allOf', 'anyOf'])
        if special_keys:
            res = {}
            for p in special_keys:
                for q in tree[p]:
                    res.update(self._traverse(schema, q, path, table))
        elif 'enum' in tree:
            self._table_definitions[table][self._column_name(path)] = 'enum'
            if 'comment' in tree:
                self._column_comments.setdefault(table, {})[self._column_name(path)] = tree['comment']
            res = {'_column': self._column_name(path), '_type': 'enum'}
        elif 'type' not in tree:
            res = {}
            warnings.warn('Type info missing: %s' % '/'.join(path))
        elif tree['type'] == 'object':
            res = {}
            if 'patternProperties' in tree:
                # Always create a new table for the pattern properties
                assert len(tree['patternProperties']) == 1
                for p in tree['patternProperties']:
                    ref_col_name = table + '_id'
                    res['*'] = self._traverse(schema, tree['patternProperties'][p], tuple(), self._table_name(path), (table, ref_col_name, self._column_name(path)), tree.get('comment'))
            elif 'properties' in tree:
                if definition:
                    # This is a shared definition, so create a new table (if not already exists)
                    if path == tuple():
                        ref_col_name = self._table_name([definition]) + '_id'
                    else:
                        ref_col_name = self._column_name(path) + '_id'
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], (p, ), self._table_name([definition]), (table, ref_col_name, self._column_name(path)), tree.get('comment'))
                    self._table_definitions[table][ref_col_name] = 'link'
                    self._links.setdefault(table, {})[ref_col_name] = ('/'.join(path), self._table_name([definition]))
                else:
                    # Standard object, just traverse recursively
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], path + (p,), table, parent, tree.get('comment'))
            else:
                warnings.warn('Type error: %s' % ','.join(path))
        else:
            if tree['type'] not in ['string', 'boolean', 'number', 'integer']:
                warnings.warn('Type error: %s: %s' % (tree['type'], '/'.join(path)))
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

        return res

    def _is_valid_type(self, t, value):
        try:
            if t == 'number':
                float(value)
            elif t == 'integer':
                int(value)
            elif t == 'boolean':
                assert type(value) == bool
            elif t == 'timestamp':
                iso8601.parse_date(value)
            elif t == 'date':
                iso8601.parse_date(value + 'T00:00:00Z')
        except:
            return False
        return True

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
        postgres_types = {'boolean': 'bool', 'number': 'float', 'string': 'text', 'enum': 'text', 'integer': 'bigint', 'timestamp': 'timestamptz', 'date': 'date', 'link': 'integer'}
        with con.cursor() as cursor:
            if self._postgres_schema is not None:
                cursor.execute('drop schema if exists %s cascade' % self._postgres_schema)
                cursor.execute('create schema %s' % self._postgres_schema)
            for table, columns in self._table_columns.items():
                types = [self._table_definitions[table][column] for column in columns]
                create_q = 'create table %s (id serial primary key, "%s" %s not null, "%s" text not null, %s unique ("%s", "%s"))' % \
                           (self._postgres_table_name(table), self._item_col_name, postgres_types[self._item_col_type], self._prefix_col_name,
                            ''.join('"%s" %s, ' % (c, postgres_types[t]) for c, t in zip(columns, types)),
                            self._item_col_name, self._prefix_col_name)
                cursor.execute(create_q)
                cursor.execute('create index on %s ("%s")' % (self._postgres_table_name(table), self._item_col_name))
                if table in self._table_comments:
                    cursor.execute('comment on table %s is %%s' % self._postgres_table_name(table), (self._table_comments[table],))
                for c in columns:
                    if c in self._column_comments.get(table, {}):
                        cursor.execute('comment on column %s.%s is %%s' % (self._postgres_table_name(table), c), (self._column_comments[table][c],))

    def insert_items(self, con, items, failure_count={}):
        ''' Inserts data into database.

        `items` can be either a nested dict conforming to the JSON spec, or a list/iterator of pairs where the first item in the pair
        is a tuple specifying the path, and the second value in the pair is the value.'''
        res = {}
        failure_count = {}
        for item_id, data in items.items():
            if type(data) == dict:
                data = self._flatten_dict(data)
            for path, value in data:
                if value is None:
                    continue

                res.setdefault(item_id, {}).setdefault(self._translation_tree['_table'], {}).setdefault('', {})
                subtree = self._translation_tree
                for index, path_part in enumerate(path):
                    if '*' in subtree:
                        subtree = subtree['*']
                    elif path_part not in subtree:
                        failure_count[path] = failure_count.get(path, 0) + 1
                        break
                    else:
                        subtree = subtree[path_part]

                    # Compute the prefix, add an empty entry (TODO: should make the prefix customizeable)
                    table, suffix = subtree['_table'], subtree['_suffix']
                    prefix_suffix = '/' + '/'.join(path[:(index+1)])
                    assert prefix_suffix.endswith(suffix)
                    prefix = prefix_suffix[:len(prefix_suffix)-len(suffix)].rstrip('/')
                    res.setdefault(item_id, {}).setdefault(table, {}).setdefault(prefix, {})

                # Leaf node with value, validate and prepare for insertion
                if '_column' not in subtree:
                    failure_count[path] = failure_count.get(path, 0) + 1
                    continue
                col, t = subtree['_column'], subtree['_type']
                if table not in self._table_columns:
                    failure_count[path] = failure_count.get(path, 0) + 1
                    continue
                if not self._is_valid_type(t, value):
                    failure_count[path] = failure_count.get(path, 0) + 1
                    continue

                res.setdefault(item_id, {}).setdefault(table, {}).setdefault(prefix, {})[col] = value

        data_by_table = {}
        for item_id, item_data in res.items():
            for table, table_values in item_data.items():
                for prefix, row_values in table_values.items():
                    row_array = [item_id, prefix] + [row_values.get(t) for t in self._table_columns[table]]
                    data_by_table.setdefault(table, []).append(row_array)

        with con.cursor() as cursor:
            for table, data in data_by_table.items():
                cols = '("%s","%s"%s)' % (self._item_col_name, self._prefix_col_name, ''.join(',"%s"' % c for c in self._table_columns[table]))
                pattern = '(' + ','.join(['%s'] * len(data[0])) + ')'
                args = b','.join(cursor.mogrify(pattern, tup) for tup in data)
                cursor.execute(b'insert into %s %s values %s' % (self._postgres_table_name(table).encode(), cols.encode(), args))

    def create_links(self, con):
        # Add foreign keys between tables
        for from_table, cols in self._links.items():
            for ref_col_name, (prefix, to_table) in cols.items():
                if from_table not in self._table_columns or to_table not in self._table_columns:
                    continue
                update_q = 'update %s as from_table set "%s" = to_table.id from (select "%s", "%s", id from %s) to_table' \
                           % (self._postgres_table_name(from_table), ref_col_name, self._item_col_name, self._prefix_col_name, self._postgres_table_name(to_table))
                if prefix:
                    # Forward reference from table to a definition
                    update_q += ' where from_table."%s" = to_table."%s" and from_table."%s" || \'/%s\' = to_table."%s"' % (
                        self._item_col_name, self._item_col_name, self._prefix_col_name, prefix, self._prefix_col_name)
                else:
                    # Backward definition from a table to its patternProperty parent
                    update_q += ' where from_table."%s" = to_table."%s" and strpos(from_table."%s", to_table."%s") = 1' % (
                        self._item_col_name, self._item_col_name, self._prefix_col_name, self._prefix_col_name)

                alter_q = 'alter table %s add constraint fk_%s foreign key ("%s") references %s (id)' % \
                          (self._postgres_table_name(from_table), ref_col_name, ref_col_name, self._postgres_table_name(to_table))
                with con.cursor() as cursor:
                    cursor.execute(update_q)
                    cursor.execute(alter_q)

    def analyze(self, con):
        with con.cursor() as cursor:
            for table in self._table_columns.keys():
                cursor.execute('analyze %s' % self._postgres_table_name(table))
