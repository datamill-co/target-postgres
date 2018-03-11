import io
import csv
import uuid

import arrow
from psycopg2 import sql

## TODO: full table rep? - _sdc_table_version

class TransformStream(object):
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()

class PostgresTarget(object):
    SINGER_RECEIVED_AT = '_sdc_received_at'
    SINGER_SEQUENCE = '_sdc_sequence'
    SINGER_TABLE_VERSION = '_sdc_table_version'
    SINGER_PK = '_sdc_primary_key'
    SINGER_SOURCE_PK_PREFIX = '_sdc_source_key_'
    SINGER_LEVEL = '_sdc_level_{}_id'

    NESTED_SEPARATOR = '__'

    def __init__(self, connection, logger, *args, postgres_schema='public', **kwargs):
        self.conn = connection
        self.logger = logger
        self.postgres_schema = postgres_schema

    def write_records(self, stream_buffer):
        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                self.add_singer_columns(stream_buffer.schema, stream_buffer.key_properties)

                subtables = {}
                key_prop_schemas = {}
                for key in stream_buffer.key_properties:
                    key_prop_schemas[key] = stream_buffer.schema['properties'][key]
                self.denest_schema(stream_buffer.stream, stream_buffer.schema, key_prop_schemas, subtables)

                root_temp_table_name, root_table_upsert = self.upsert_table_schema(cur,
                                                                                   stream_buffer.stream,
                                                                                   stream_buffer.schema)

                nested_upsert_tables = []
                for table_name, json_schema in subtables.items():
                    temp_table_name, upsert = self.upsert_table_schema(cur,
                                                                       table_name,
                                                                       json_schema)
                    nested_upsert_tables.append({
                        'table_name': table_name,
                        'json_schema': json_schema,
                        'temp_table_name': temp_table_name,
                        'upsert': upsert
                    })

                ## TODO: populate_singer_columns - top level singe columns
                ## TODO: dedup records by sequence_field here, so that nested records are dedupped too
                records = stream_buffer.peek_buffer()


                records_map = {}
                self.denest_records(stream_buffer.stream, records, records_map, stream_buffer.key_properties)

                self.persist_rows(cur,
                                  stream_buffer.stream,
                                  root_temp_table_name,
                                  root_table_upsert,
                                  stream_buffer.schema,
                                  stream_buffer.key_properties,
                                  stream_buffer.sequence_field,
                                  records_map[stream_buffer.stream])

                for nested_upsert_table in nested_upsert_tables:
                    key_properties = []
                    for key in stream_buffer.key_properties:
                        key_properties.append(self.SINGER_SOURCE_PK_PREFIX + key)
                    for prop in nested_upsert_table['json_schema']['properties'].keys():
                        if prop.startswith(self.SINGER_LEVEL[:11]): ## TODO: get rid of hardcoded startswith
                            key_properties.append(prop)
                    ## TODO: should persist_rows delete all nested rows with
                    ##       top level SINGER_SOURCE_PK_PREFIX columns?
                    self.persist_rows(cur,
                                      nested_upsert_table['table_name'],
                                      nested_upsert_table['temp_table_name'],
                                      nested_upsert_table['upsert'],
                                      nested_upsert_table['json_schema'],
                                      key_properties,
                                      None,
                                      records_map[nested_upsert_table['table_name']])

                cur.execute('COMMIT;')
            except:
                cur.execute('ROLLBACK;')
                self.logger.exception('Exception writing records')
                raise

        stream_buffer.flush_buffer()

    def add_singer_columns(self, schema, key_properties):
        properties = schema['properties']

        if self.SINGER_RECEIVED_AT not in properties:
            properties[self.SINGER_RECEIVED_AT] = {
                'type': ['null', 'string'],
                'format': 'date-time'
            }

        if self.SINGER_SEQUENCE not in properties:
            properties[self.SINGER_SEQUENCE] = {
                'type': ['null', 'integer']
            }

        if self.SINGER_TABLE_VERSION not in properties:
            properties[self.SINGER_TABLE_VERSION] = {
                'type': ['null', 'integer']
            }

        if len(key_properties) == 0:
            properties[self.SINGER_PK] = {
                'type': ['string']
            }

    def populate_singer_columns(self, record, stream_buffer):
        if self.SINGER_TABLE_VERSION not in record:
            record[self.SINGER_TABLE_VERSION] = 0
        if stream_buffer.use_uuid_pk and record.get(self.SINGER_PK) is None:
            record[self.SINGER_PK] = uuid.uuid4()

    def denest_schema_helper(self,
                             table_name,
                             json_schema,
                             not_null,
                             top_level_schema,
                             current_path,
                             key_prop_schemas,
                             subtables,
                             level):
        for prop, json_schema in json_schema['properties'].items():
            next_path = current_path + self.NESTED_SEPARATOR + prop
            if json_schema['type'] == 'object':
                self.denest_schema_helper(table_name,
                                          json_schema,
                                          not_null,
                                          top_level_schema,
                                          next_path,
                                          key_prop_schemas,
                                          subtables,
                                          level)
            elif 'array' in json_schema['type']:
                self.create_subtable(table_name + self.NESTED_SEPARATOR + prop,
                                     json_schema,
                                     key_prop_schemas,
                                     subtables,
                                     level + 1)
            else:
                if not_null and 'null' in json_schema['type']:
                    json_schema['type'].remove('null')
                elif 'null' not in json_schema['type']:
                    json_schema['type'].append('null')
                top_level_schema[next_path] = json_schema

    def create_subtable(self, table_name, json_schema, key_prop_schemas, subtables, level):
        array_type = json_schema['items']['type']
        if 'object' in array_type:
            new_properties = json_schema['items']['properties']
        else:
            new_properties = {'value': json_schema['items']}

        for pk, json_schema in key_prop_schemas.items():
            new_properties[self.SINGER_SOURCE_PK_PREFIX + pk] = json_schema

        for i in range(0, level + 1):
            new_properties[self.SINGER_LEVEL.format(i)] = {
                'type': ['integer']
            }

        new_schema = {'type': ['object'], 'properties': new_properties}

        self.denest_schema(table_name, new_schema, key_prop_schemas, subtables, level=level)

        subtables[table_name] = new_schema

    def denest_schema(self, table_name, schema, key_prop_schemas, subtables, current_path=None, level=-1):
        new_properties = {}
        for prop, json_schema in schema['properties'].items():
            if current_path:
                next_path = current_path + self.NESTED_SEPARATOR + prop
            else:
                next_path = prop

            if 'object' in json_schema['type']:
                not_null = 'null' not in json_schema['type']
                self.denest_schema_helper(table_name + self.NESTED_SEPARATOR + next_path,
                                          json_schema,
                                          not_null,
                                          new_properties,
                                          next_path,
                                          key_prop_schemas,
                                          subtables,
                                          level)
            elif 'array' in json_schema['type']:
                self.create_subtable(table_name + self.NESTED_SEPARATOR + next_path,
                                     json_schema,
                                     key_prop_schemas,
                                     subtables,
                                     level + 1)
            else:
                new_properties[prop] = json_schema
        schema['properties'] = new_properties

    def denest_subrecord(self,
                         table_name,
                         current_path,
                         parent_record,
                         record,
                         records_map,
                         key_properties,
                         pk_fks,
                         level):
        for prop, value in record.items():
            next_path = current_path + self.NESTED_SEPARATOR + prop
            if isinstance(value, dict):
                self.denest_subrecord(table_name, next_path, parent_record, value, pk_fks, level)
            elif isinstance(value, list):
                self.denest_records(table_name + self.NESTED_SEPARATOR + next_path,
                                    value,
                                    records_map,
                                    key_properties,
                                    pk_fks=pk_fks,
                                    level=level + 1)
            else:
                parent_record[next_path] = value

    def denest_record(self, table_name, current_path, record, records_map, key_properties, pk_fks, level):
        denested_record = {}
        for prop, value in record.items():
            if current_path:
                next_path = current_path + self.NESTED_SEPARATOR + prop
            else:
                next_path = prop

            if isinstance(value, dict):
                self.denest_subrecord(table_name,
                                      next_path,
                                      denested_record,
                                      value,
                                      records_map,
                                      key_properties,
                                      pk_fks,
                                      level)
            elif isinstance(value, list):
                self.denest_records(table_name + self.NESTED_SEPARATOR + next_path,
                                    value,
                                    records_map,
                                    key_properties,
                                    pk_fks=pk_fks,
                                    level=level + 1)
            else:
                denested_record[next_path] = value

        if table_name not in records_map:
            records_map[table_name] = []
        records_map[table_name].append(denested_record)

    def denest_records(self, table_name, records, records_map, key_properties, pk_fks=None, level=-1):
        row_index = 0
        for record in records:
            if pk_fks:
                record_pk_fks = pk_fks.copy()
                record_pk_fks[self.SINGER_LEVEL.format(level)] = row_index
                for key, value in record_pk_fks.items():
                    record[key] = value
                row_index += 1
            else: ## top level
                record_pk_fks = {}
                for key in key_properties:
                    record_pk_fks[self.SINGER_SOURCE_PK_PREFIX + key] = record[key]
            self.denest_record(table_name, None, record, records_map, key_properties, record_pk_fks, level)

    def upsert_table_schema(self, cur, table_name, schema):
        existing_table_schema = self._get_schema(cur, self.postgres_schema, table_name)

        if existing_table_schema:
            schema = self._merge_put_schemas(cur,
                                             self.postgres_schema,
                                             table_name, 
                                             existing_table_schema,
                                             schema)
            target_table_name = self._get_temp_table_name(table_name)
        else:
            schema = schema
            target_table_name = table_name

        self._create_table(cur,
                           self.postgres_schema,
                           target_table_name,
                           schema)

        return target_table_name, existing_table_schema is not None

    def get_update_sql(self, target_table_name, temp_table_name, key_properties, sequence_field):
        full_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(target_table_name))
        full_temp_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name))

        pk_temp_select_list = []
        pk_where_list = []
        cxt_where_list = []
        for pk in key_properties:
            pk_temp_select_list.append(sql.SQL('{}.{}').format(full_temp_table_name,
                                                               sql.Identifier(pk)))

            pk_where_list.append(
                sql.SQL('{table}.{pk} = {temp_table}.{pk}').format(
                    table=full_table_name,
                    temp_table=full_temp_table_name,
                    pk=sql.Identifier(pk)))

            cxt_where_list.append(
                sql.SQL('{table}.{pk} = "pks".{pk}').format(
                    table=full_table_name,
                    pk=sql.Identifier(pk)))
        pk_temp_select = sql.SQL(', ').join(pk_temp_select_list)
        pk_where = sql.SQL(' AND ').join(pk_where_list)
        cxt_where = sql.SQL(' AND ').join(cxt_where_list)

        if sequence_field:
            distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
                pk_temp_select,
                full_temp_table_name,
                sql.Identifier(sequence_field))
        else:
            distinct_order_by = sql.SQL('')

        return sql.SQL('''
            WITH "pks" AS (
                SELECT DISTINCT ON ({pk_temp_select}) {pk_temp_select}
                FROM {temp_table}
                JOIN {table} ON {pk_where}{distinct_order_by}
            )
            DELETE FROM {table} USING "pks" WHERE {cxt_where};
            INSERT INTO {table} (
                SELECT DISTINCT ON ({pk_temp_select}) *
                FROM {temp_table}{distinct_order_by}
            );
            DROP TABLE {temp_table};
            ''').format(table=full_table_name,
                        temp_table=full_temp_table_name,
                        pk_temp_select=pk_temp_select,
                        pk_where=pk_where,
                        cxt_where=cxt_where,
                        distinct_order_by=distinct_order_by)

    def persist_rows(self,
                     cur,
                     target_table_name,
                     temp_table_name,
                     upsert,
                     json_schema,
                     key_properties,
                     sequence_field,
                     records):
        headers = list(json_schema['properties'].keys())

        datetime_fields = [k for k,v in json_schema['properties'].items()
                           if v.get('format') == 'date-time']

        rows = iter(records)

        def transform():
            try:
                row = next(rows)
                with io.StringIO() as out:
                    ## TODO: move to main record processing above?
                    for prop in datetime_fields:
                        if prop in row:
                            row[prop] = arrow.get(row[prop]).format(
                                                              'YYYY-MM-DD HH:mm:ss.SSSSZZ')
                    writer = csv.DictWriter(out, headers)
                    writer.writerow(row)
                    return out.getvalue()
            except StopIteration:
                return ''

        csv_rows = TransformStream(transform)

        copy = sql.SQL('COPY {}.{} ({}) FROM STDIN CSV').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, headers)))
        cur.copy_expert(copy, csv_rows)

        if upsert:
            update_sql = self.get_update_sql(target_table_name, temp_table_name, key_properties, sequence_field)
            cur.execute(update_sql)

    def _create_table(self, cur, table_schema, table_name, schema):
        create_table_sql = sql.SQL('CREATE TABLE {}.{}').format(
                sql.Identifier(table_schema),
                sql.Identifier(table_name))

        columns_sql = []
        for prop, json_schema in schema['properties'].items():
            sql_type = self._json_schema_to_sql(json_schema)
            columns_sql.append(sql.SQL('{} {}').format(sql.Identifier(prop),
                                                       sql.SQL(sql_type)))

        cur.execute(sql.SQL('{} ({});').format(create_table_sql,
                                               sql.SQL(', ').join(columns_sql)))

    def _get_temp_table_name(self, stream_name):
        return stream_name + '_' + str(uuid.uuid4()).replace('-', '')

    def _sql_to_json_schema(self, sql_type, nullable):
        _format = None
        if sql_type == 'timestamp with time zone':
            json_type = 'string'
            _format = 'date-time'
        elif sql_type == 'bigint':
            json_type = 'integer'
        elif sql_type == 'double precision':
            json_type = 'number'
        elif sql_type == 'boolean':
            json_type = 'boolean'
        elif sql_type == 'text':
            json_type = 'string'
        else:
            raise Exception('Unsupported type `{}` in existing target table'.format(sql_type))

        if nullable:
            json_type = ['null', json_type]

        json_schema = {'type': json_type}
        if _format:
            json_schema['format'] = _format

        return json_schema

    def _json_schema_to_sql(self, json_schema):
        _type = json_schema['type']
        not_null = True
        if isinstance(_type, list):
            ln = len(_type)
            if ln == 1:
                _type = _type[0]
            if ln == 2 and 'null' in _type:
                not_null = False
                if _type.index('null') == 0:
                    _type = _type[1]
                else:
                    _type = _type[0]
            elif ln > 2:
                raise Exception('Multiple types per column not supported')

        sql_type = 'text'

        if 'format' in json_schema and \
           json_schema['format'] == 'date-time' and \
           _type == 'string':
            sql_type = 'timestamp with time zone'
        elif _type == 'boolean':
            sql_type = 'boolean'
        elif _type == 'integer':
            sql_type = 'bigint'
        elif _type == 'number':
            sql_type = 'double precision'

        if not_null:
            sql_type += ' NOT NULL'

        return sql_type

    def _get_schema(self, cur, table_schema, table_name):
        cur.execute(
            sql.SQL('SELECT column_name, data_type, is_nullable FROM information_schema.columns ') +
            sql.SQL('WHERE table_schema = {} and table_name = {};').format(
                sql.Literal(table_schema), sql.Literal(table_name)))
        columns = cur.fetchall()

        if len(columns) == 0:
            return None

        properties = {}
        for column in columns:
            properties[column[0]] = self._sql_to_json_schema(column[1], column[2] == 'YES')

        schema = {'properties': properties}

        return schema

    def _get_null_default(self, column, json_schema):
        if 'default' in json_schema:
            return json_schema['default']

        json_type = json_schema['type']
        if 'null' in json_type:
            return None

        if len(json_type) == 1 or json_type != 'null':
            _type = json_type[0]
        else:
            _type = json_type[1]

        if _type == 'string':
            return ''
        if _type == 'boolean':
            return 'FALSE'

        raise Exception('Non-trival default needed on new non-null column `{}`'.format(column))

    def _add_column(self, cur, table_schema, table_name, column_name, data_type, default_value):
        if default_value is not None:
            default_value = sql.SQL(' DEFAULT {}').format(sql.Literal(default_value))
        else:
            default_value = sql.SQL('')

        cur.execute(
            sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                    'ADD COLUMN {column_name} {data_type}{default_value};').format(
                    table_schema=sql.Identifier(table_schema),
                    table_name=sql.Identifier(table_name),
                    column_name=sql.Identifier(column_name),
                    data_type=sql.SQL(data_type),
                    default_value=default_value))

    def _merge_put_schemas(self, cur, table_schema, table_name, existing_schema, new_schema):
        new_properties = new_schema['properties']
        existing_properties = existing_schema['properties']
        for prop, json_schema in new_properties.items():
            if prop not in existing_properties:
                existing_properties[prop] = new_properties[prop]
                data_type = self._json_schema_to_sql(new_properties[prop])
                default_value = self._get_null_default(prop, new_properties[prop])
                self._add_column(cur,
                                 table_schema,
                                 table_name,
                                 prop,
                                 data_type,
                                 default_value)
            ## TODO: types do not match

        return existing_schema

