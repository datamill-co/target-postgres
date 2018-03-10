import io
import csv
import uuid

import arrow
from psycopg2 import sql

## TODO: nested records
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
        rows = iter(stream_buffer.peek_buffer())
        headers = list(stream_buffer.schema['properties'].keys())

        datetime_fields = [k for k,v in stream_buffer.schema['properties'].items()
                           if v.get('format') == 'date-time']

        def transform():
            try:
                row = next(rows)
                with io.StringIO() as out:
                    for prop in datetime_fields:
                        if prop in row:
                            row[prop] = arrow.get(row[prop]).format('YYYY-MM-DD HH:mm:ss.SSSSZZ')
                    writer = csv.DictWriter(out, headers)
                    writer.writerow(row)
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
            'SELECT column_name, data_type, is_nullable FROM information_schema.columns ' +
            'WHERE table_schema = %s and table_name = %s;',
            (table_schema, table_name))
        columns = cur.fetchall()

        if len(columns) == 0:
            return None, None

        properties = {}
        for column in columns:
            properties[column[0]] = self._sql_to_json_schema(column[1], column[2] == 'YES')

        schema = {'properties': properties}

        return schema

    def _add_column(self, cur, table_schema, table_name, column_name, data_type):
        cur.execute(
            'ALTER TABLE "{table_schema}"."{table_name}" ADD COLUMN "{column_name}" {data_type};'.format(
                table_schema=table_schema,
                table_name=table_name,
                column_name=column_name,
                data_type=data_type))

    def _merge_put_schemas(self, cur, table_schema, table_name, existing_schema, new_schema):
        new_properties = new_schema['properties']
        existing_properties = existing_schema['properties']
        for prop, json_schema in new_properties.items():
            if prop not in existing_properties:
                existing_properties[prop] = new_properties[prop]
                data_type = self._json_schema_to_sql(new_properties[prop])
                self._add_column(cur,
                                 table_schema,
                                 table_name,
                                 prop,
                                 data_type)
            ## TODO: types do not match

        return existing_schema

