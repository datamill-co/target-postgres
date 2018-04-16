import psycopg2
from psycopg2 import sql

from target_sql.target_sql import TargetSQL, TransformStream

class TargetPostgres(TargetSQL):
    def create_connection(self, config):
        connection = config.get('target_connection')

        self.conn = psycopg2.connect(
            host=connection.get('host', 'localhost'),
            port=connection.get('port', 5432),
            dbname=connection.get('database'),
            user=connection.get('username'),
            password=connection.get('password'))

    def destroy_connection(self):
        self.conn.close()

    def sql_to_json_schema(self, sql_type, nullable):
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

    def json_schema_to_sql(self, json_schema):
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

    def copy_rows(self, cur, table_name, headers, row_fn):
        rows = TransformStream(row_fn)

        copy = sql.SQL('COPY {}.{} ({}) FROM STDIN CSV').format(
            sql.Identifier(self.catalog),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, headers)))
        cur.copy_expert(copy, rows)
