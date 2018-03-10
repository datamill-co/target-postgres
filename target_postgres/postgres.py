import io
import csv
import uuid

import arrow
from psycopg2 import sql

## TODO: nested arrays

## TODO: full table rep? - _sdc_table_version

class TransformStream(object):
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()

class PostgresTarget(object):
    def __init__(self, connection, logger, *args, postgres_schema='public', **kwargs):
        self.conn = connection
        self.logger = logger
        self.postgres_schema = postgres_schema

    def write_records(self, stream_buffer):
        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                self.denest_schema(stream_buffer.schema)

                table_name, use_temp = self.upsert_table_schema(cur, stream_buffer)
                self.persist_rows(
                    cur,
                    use_temp,
                    self.postgres_schema,
                    table_name,
                    stream_buffer)

                cur.execute('COMMIT;')
            except:
                cur.execute('ROLLBACK;')
                self.logger.exception('Exception writing records')
                raise

        stream_buffer.flush_buffer()

    def denest_schema_helper(self, json_schema, not_null, top_level_schema, current_path, sep):
        for prop, json_schema in json_schema['properties'].items():
            next_path = current_path + sep + prop
            if json_schema['type'] == 'object':
                self.denest_schema_helper(json_schema, not_null, top_level_schema, next_path, sep)
            else:
                if not_null and 'null' in json_schema['type']:
                    json_schema['type'].remove('null')
                elif 'null' not in json_schema['type']:
                    json_schema['type'].append('null')
                top_level_schema[next_path] = json_schema

    def denest_schema(self, schema, sep='__'):
        new_properties = {}
        for prop, json_schema in schema['properties'].items():
            if 'object' in json_schema['type']:
                not_null = 'null' not in json_schema['type']
                self.denest_schema_helper(json_schema, not_null, new_properties, prop, sep)
            else:
                new_properties[prop] = json_schema
        schema['properties'] = new_properties

    def denest_record_helper(self, value, top_level_value, current_path, sep):
        for prop, value in value.items():
            next_path = current_path + sep + prop
            if isinstance(value, dict):
                self.denest_record_helper(value, top_level_value, next_path, sep)
            else:
                top_level_value[next_path] = value

    def denest_record(self, record, sep='__'):
        denested_record = {}
        for prop, value in record.items():
            if isinstance(value, dict):
                self.denest_record_helper(value, denested_record, prop, sep)
            else:
                denested_record[prop] = value
        return denested_record

    def upsert_table_schema(self, cur, stream_buffer):
        existing_table_schema = self._get_schema(cur, self.postgres_schema, stream_buffer.stream)

        if existing_table_schema:
            schema = self._merge_put_schemas(cur,
                                             self.postgres_schema,
                                             stream_buffer.stream, 
                                             existing_table_schema,
                                             stream_buffer.schema)
            table_name = self._get_temp_table_name(stream_buffer.stream)
        else:
            schema = stream_buffer.schema
            table_name = stream_buffer.stream

        self._create_table(cur,
                           self.postgres_schema,
                           table_name,
                           schema,
                           stream_buffer.key_properties)

        return table_name, existing_table_schema is not None

    def get_update_sql(self, table_schema, stream_buffer, table_name):
        full_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(table_schema),
            sql.Identifier(stream_buffer.stream))
        full_temp_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(table_schema),
            sql.Identifier(table_name))

        pk_temp_select_list = []
        pk_where_list = []
        cxt_where_list = []
        for pk in stream_buffer.key_properties:
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

        if stream_buffer.sequence_field:
            distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
                pk_temp_select,
                full_temp_table_name,
                sql.Identifier(stream_buffer.sequence_field))
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

    def persist_rows(self, cur, use_temp, table_schema, table_name, stream_buffer):
        records = iter(stream_buffer.peek_buffer())
        headers = list(stream_buffer.schema['properties'].keys())

        datetime_fields = [k for k,v in stream_buffer.schema['properties'].items()
                           if v.get('format') == 'date-time']

        def transform():
            try:
                record = next(records)
                with io.StringIO() as out:
                    denested_record = self.denest_record(record)
                    for prop in datetime_fields:
                        if prop in denested_record:
                            denested_record[prop] = arrow.get(denested_record[prop]).format(
                                                              'YYYY-MM-DD HH:mm:ss.SSSSZZ')
                    writer = csv.DictWriter(out, headers)
                    writer.writerow(denested_record)
                    return out.getvalue()
            except StopIteration:
                return ''

        csv_rows = TransformStream(transform)

        copy = sql.SQL('COPY {}.{} ({}) FROM STDIN CSV').format(
            sql.Identifier(table_schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, headers)))
        cur.copy_expert(copy, csv_rows)

        if use_temp:
            update_sql = self.get_update_sql(table_schema, stream_buffer, table_name)
            cur.execute(update_sql)

    def _create_table(self, cur, table_schema, table_name, schema, key_properties):
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
            return None, None

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
            default_value = " DEFAULT {}".format(default_value)
        else:
            default_value = ''

        cur.execute(
            sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                    'ADD COLUMN {column_name} {data_type}{default_value};').format(
                    table_schema=sql.Identifier(table_schema),
                    table_name=sql.Identifier(table_name),
                    column_name=sql.Identifier(column_name),
                    data_type=sql.SQL(data_type),
                    default_value=sql.SQL(default_value)))

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

