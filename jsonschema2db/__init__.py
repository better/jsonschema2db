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

        # Walk the schema and build up the translation tables
        self._translation_tree = self._traverse(schema, schema)

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

    def _traverse(self, schema, tree, path=tuple(), table='root', parent=None):
        # Computes a bunch of stuff
        # 1. A list of tables and columns (used to create tables dynamically)
        # 2. A tree (dicts of dicts) with a mapping for each fact into tables (used to map data)
        # 3. Links between entities
        if type(tree) != dict:
            warnings.warn('Broken subtree: /%s' % '/'.join(path))
            return

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

        self._table_definitions.setdefault(table, {})

        special_keys = set(tree.keys()).intersection(['oneOf', 'allOf', 'anyOf'])
        if special_keys:
            res = {}
            for p in special_keys:
                for q in tree[p]:
                    res.update(self._traverse(schema, q, path, table))
        elif 'enum' in tree:
            self._table_definitions[table][self._column_name(path)] = 'enum'
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
                    res['*'] = self._traverse(schema, tree['patternProperties'][p], tuple(), self._table_name(path), (table, ref_col_name, self._column_name(path)))
            elif 'properties' in tree:
                if definition:
                    # This is a shared definition, so create a new table (if not already exists)
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], (p, ), self._table_name([definition]), parent)
                    if path != tuple():
                        ref_col_name = self._column_name(path) + '_id'
                        self._table_definitions[table][ref_col_name] = 'link'
                        self._links.setdefault(table, {})[ref_col_name] = ('/'.join(path), self._table_name([definition]))
                else:
                    # Standard object, just traverse recursively
                    for p in tree['properties']:
                        res[p] = self._traverse(schema, tree['properties'][p], path + (p,), table, parent)
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
                    if parent is not None:
                        self._backlinks.setdefault(table, set()).add(parent)
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

    def _traverse_for_insertion(self, item_id, data, subtree, res, failure_count, path=tuple()):
        table, suffix = subtree['_table'], subtree['_suffix']

        # TODO: make the path prefix thing customizeable
        prefix_suffix = '/' + '/'.join(path)
        assert prefix_suffix.endswith(suffix)
        prefix = prefix_suffix[:len(prefix_suffix)-len(suffix)].rstrip('/')

        res.setdefault(item_id, {}).setdefault(table, {}).setdefault(prefix, {})

        if type(data) == dict:
            for k, v in data.items():
                # intermediate node
                if '*' in subtree:
                    self._traverse_for_insertion(item_id, data[k], subtree['*'], res, failure_count, path + (k,))
                elif k not in subtree:
                    failure_count[path] = failure_count.get(path, 0) + 1
                else:
                    self._traverse_for_insertion(item_id, data[k], subtree[k], res, failure_count, path + (k,))

        else:
            # value type
            if data is None:
                return
            if '_column' not in subtree:
                failure_count[path] = failure_count.get(path, 0) + 1
                return
            col, t = subtree['_column'], subtree['_type']
            if table not in self._table_columns:
                return
            if not self._is_valid_type(t, data):
                failure_count[path] = failure_count.get(path, 0) + 1
                return

            res.setdefault(item_id, {}).setdefault(table, {}).setdefault(prefix, {})[col] = data

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

    def insert_items(self, con, items, failure_count={}):
        res = {}
        for item_id, data in items.items():
            self._traverse_for_insertion(item_id, data, self._translation_tree, res, failure_count)

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
