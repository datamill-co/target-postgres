import uuid

import boto3
from psycopg2 import sql

from target_sql.target_postgres import TargetPostgres, TransformStream

class TargetRedshift(TargetPostgres):
    def __init__(self, config, *args, **kwargs):
        s3_config = config.get('target_s3')
        if not s3_config:
            raise Exception('`target_s3` required')
        self.s3_config = s3_config

        super(TargetRedshift, self).__init__(config, *args, **kwargs)

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
        elif sql_type[:7] == 'varchar':
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

        sql_type = 'varchar(65535)'

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
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.s3_config.get('aws_access_key_id'),
            aws_secret_access_key=self.s3_config.get('aws_secret_access_key'))

        bucket = self.s3_config.get('bucket')
        if not bucket:
            raise Exception('`target_s3.bucket` required')
        prefix = self.s3_config.get('key_prefix', '')
        key = prefix + table_name + self.NESTED_SEPARATOR + str(uuid.uuid4()).replace('-', '')

        rows = TransformStream(row_fn, binary=True)

        s3_client.upload_fileobj(
            rows,
            bucket,
            key)

        source = 's3://{}/{}'.format(bucket, key)
        credentials = 'aws_access_key_id={};aws_secret_access_key={};'.format(
            self.s3_config.get('aws_access_key_id'),
            self.s3_config.get('aws_secret_access_key'))

        copy_sql = sql.SQL('COPY {}.{} ({}) FROM {} CREDENTIALS {} FORMAT AS CSV').format(
            sql.Identifier(self.catalog),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, headers)),
            sql.Literal(source),
            sql.Literal(credentials))

        cur.execute(copy_sql)
