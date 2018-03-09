import io
import csv
import uuid

## TODO: support postgres schemas
## TODO: nested records
## TODO: nested arrays

## TODO: full table rep?

class TransformStream(object):
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()

class PostgresTarget(object):
    def __init__(self, connection, *args, **kwargs):
        self.conn = connection

    def write_records(self, stream_buffer):
        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                table_name, use_temp = self.upsert_table_schema(cur, stream_buffer)
                self.persist_rows(
                    cur,
                    use_temp,
                    'public',
                    table_name,
                    stream_buffer)

                cur.execute('COMMIT;')
            except Exception as e:
                ## TODO: log error
                print(e)
                cur.execute('ROLLBACK;')

        stream_buffer.flush_buffer()

    def upsert_table_schema(self, cur, stream_buffer):
        existing_table_schema, key_properties = self._get_schema(cur, 'public', stream_buffer.stream)
        ## TODO: compare key_properties and stream_buffer.key_properties
        if existing_table_schema:
            schema = self._merge_put_schemas(cur,
                                             'public',
                                             stream_buffer.stream, 
                                             existing_table_schema,
                                             stream_buffer.schema)
            table_name = self._get_temp_table_name(stream_buffer.stream)
        else:
            schema = stream_buffer.schema
            table_name = stream_buffer.stream

        self._create_table(cur, 'public', table_name, schema, stream_buffer.key_properties)

        return table_name, existing_table_schema is not None

    def persist_rows(self, cur, use_temp, table_schema, table_name, stream_buffer):
        rows = iter(stream_buffer.peek_buffer())
        headers = list(stream_buffer.schema['properties'].keys())

        def transform():
            try:
                row = next(rows)
                with io.StringIO() as out:
                    writer = csv.DictWriter(out, headers)
                    writer.writerow(row)
                    return out.getvalue()
            except StopIteration:
                return ''

        csv_rows = TransformStream(transform)

        ## TODO: SQL injection? - "...you may use the objects provided by the psycopg2.sql module"
        copy = 'COPY "{}"."{}" ("{}") FROM STDIN CSV'.format(table_schema, table_name, '","'.join(headers))
        cur.copy_expert(copy, csv_rows)

        if use_temp:
            table_name_str = '"{}"."{}"'.format(table_schema, stream_buffer.stream)
            temp_table_name_str = '"{}"."{}"'.format(table_schema, table_name)
            columns = '"{}"'.format('","'.join(headers))
            ## TODO: SQL injection?
            cur.execute(
            '''
            WITH pks AS (
                SELECT {temp_table}."{pk}" FROM {temp_table}
                JOIN {table} ON {table}."{pk}" = {temp_table}."{pk}"
            )
            DELETE FROM {table} USING pks WHERE {table}."{pk}" = pks."{pk}";
            INSERT INTO {table} ({columns}) (SELECT * FROM {temp_table});
            DROP TABLE {temp_table};
            '''.format(table=table_name_str,
                       temp_table=temp_table_name_str,
                       columns=columns,
                       pk=stream_buffer.key_properties[0])) ## TODO: compound keys

    def _create_table(self, cur, table_schema, table_name, schema, key_properties):
        sql = 'CREATE TABLE "{}"."{}" ('.format(table_schema, table_name)
        for prop, json_schema in schema['properties'].items():
            sql += '"{}" {},'.format(prop, self._json_schema_to_sql(json_schema))
        sql += 'PRIMARY KEY ({}));'.format(','.join(key_properties))
        cur.execute(sql)

    def _get_temp_table_name(self, stream_name):
        return stream_name + '_' + str(uuid.uuid4()).replace('-', '')

    def _sql_to_json_schema(self, sql_type, nullable):
        _format = None
        ## TODO: only support ones that can be revered - json->sql?
        if sql_type in ['timestamp without time zone', 'timestamp with time zone']:
            json_type = 'string'
            _format = 'date-time'
        elif sql_type in ['integer', 'bigint', 'smallint']:
            json_type = 'integer'
        elif sql_type in ['real', 'double precision']:
            json_type = 'number'
        elif sql_type == 'boolean':
            json_type = 'boolean'
        elif sql_type in ['text', 'character varying']:
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
        ## TODO: fetch column and PK data in one query
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

        cur.execute(
            '''
            SELECT
              pg_attribute.attname
            FROM pg_index, pg_class, pg_attribute, pg_namespace
            WHERE
              nspname = %s AND 
              pg_class.oid = %s::regclass AND
              indrelid = pg_class.oid AND
              pg_class.relnamespace = pg_namespace.oid AND
              pg_attribute.attrelid = pg_class.oid AND
              pg_attribute.attnum = any(pg_index.indkey)
             AND indisprimary;''',
            (table_schema, table_name))
        keys = cur.fetchall()

        key_properties = list(map(lambda row: row[0], keys))

        return schema, key_properties

    def _add_column(self, cur, table_schema, table_name, column_name, data_type):
        cur.execute(
            'ALTER TABLE %(table_schema)s.%(table_name)s ADD COLUMN %(column_name) %(data_type)s;',
            (table_schema, table_name, column_name, data_type))

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

