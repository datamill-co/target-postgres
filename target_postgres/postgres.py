from copy import deepcopy
import csv
import io
import json
import logging
import re
import time
import uuid

import arrow
from psycopg2 import sql
from psycopg2.extras import LoggingConnection, LoggingCursor

from target_postgres import json_schema
from target_postgres.sql_base import SEPARATOR, SQLInterface
from target_postgres.singer_stream import (
    SINGER_SEQUENCE,
    SINGER_LEVEL
)

RESERVED_NULL_DEFAULT = 'NULL'


def _update_schema_0_to_1(table_schema):
    """
    Given a `table_schema` of version 0, update it to version 1.

    NOTE: This transformation is purely informational. There is no _actual_ schema update that needs to happen,
    nor any data migration.

    :param table_schema: TABLE_SCHEMA
    :return: TABLE_SCHEMA
    """

    remote_schema = deepcopy(table_schema)

    for field, property in remote_schema['schema']['properties'].items():
        if json_schema.is_datetime(property):
            remote_schema['mappings'][field]['format'] = json_schema.DATE_TIME_FORMAT

    remote_schema['schema_version'] = 1

    return remote_schema


class _MillisLoggingCursor(LoggingCursor):
    """
    An implementation of LoggingCursor which tracks duration of queries.
    """

    def execute(self, query, vars=None):
        self.timestamp = time.monotonic()
        return super(_MillisLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.monotonic()
        return super(_MillisLoggingCursor, self).callproc(procname, vars)


class MillisLoggingConnection(LoggingConnection):
    """
    An implementation of LoggingConnection which tracks duration of queries.
    """

    def filter(self, msg, curs):
        return "MillisLoggingConnection: {} millis spent executing: {}".format(
            int((time.monotonic() - curs.timestamp) * 1000),
            msg
        )

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', _MillisLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


class PostgresError(Exception):
    """
    Raise this when there is an error with regards to Postgres streaming
    """


class TransformStream:
    def __init__(self, fun):
        self.fun = fun

    def read(self, *args, **kwargs):
        return self.fun()


class PostgresTarget(SQLInterface):
    ## NAMEDATALEN _defaults_ to 64 in PostgreSQL. The maxmimum length for an identifier is
    ## NAMEDATALEN - 1.
    # TODO: Figure out way to `SELECT` value from commands
    IDENTIFIER_FIELD_LENGTH = 63

    def __init__(self, connection, *args, postgres_schema='public', logging_level=None, persist_empty_tables=False, **kwargs):
        self.LOGGER.info(
            'PostgresTarget created with established connection: `{}`, PostgreSQL schema: `{}`'.format(connection.dsn,
                                                                                                       postgres_schema))

        if logging_level:
            level = logging.getLevelName(logging_level)
            self.LOGGER.setLevel(level)

        try:
            connection.initialize(self.LOGGER)
            self.LOGGER.debug('PostgresTarget set to log all queries.')
        except AttributeError:
            self.LOGGER.debug('PostgresTarget disabling logging all queries.')

        self.conn = connection
        self.postgres_schema = postgres_schema
        self.persist_empty_tables = persist_empty_tables
        if self.persist_empty_tables:
            self.LOGGER.debug('PostgresTarget is persisting empty tables')

    def write_batch(self, stream_buffer):
        if not self.persist_empty_tables and stream_buffer.count == 0:
            return None

        with self.conn.cursor() as cur:
            try:
                self._validate_identifier(stream_buffer.stream)

                cur.execute('BEGIN;')

                current_table_schema = self.get_table_schema(cur,
                                                             (stream_buffer.stream,),
                                                             stream_buffer.stream)

                current_table_version = None

                if current_table_schema:
                    current_table_version = current_table_schema.get('version', None)

                    if set(stream_buffer.key_properties) \
                            != set(current_table_schema.get('key_properties')):
                        raise PostgresError(
                            '`key_properties` change detected. Existing values are: {}. Streamed values are: {}'.format(
                                current_table_schema.get('key_properties'),
                                stream_buffer.key_properties
                            ))

                    for key_property in stream_buffer.key_properties:
                        canonicalized_key, remote_column_schema = self.fetch_column_from_path((key_property,),
                                                                                              current_table_schema)
                        if self.json_schema_to_sql_type(remote_column_schema) \
                                != self.json_schema_to_sql_type(stream_buffer.schema['properties'][key_property]):
                            raise PostgresError(
                                ('`key_properties` type change detected for "{}". ' +
                                 'Existing values are: {}. ' +
                                 'Streamed values are: {}, {}, {}').format(
                                    key_property,
                                    json_schema.get_type(current_table_schema['schema']['properties'][key_property]),
                                    json_schema.get_type(stream_buffer.schema['properties'][key_property]),
                                    self.json_schema_to_sql_type(
                                        current_table_schema['schema']['properties'][key_property]),
                                    self.json_schema_to_sql_type(stream_buffer.schema['properties'][key_property])
                                ))

                root_table_name = stream_buffer.stream
                target_table_version = current_table_version or stream_buffer.max_version

                if current_table_version is not None and \
                        stream_buffer.max_version is not None:
                    if stream_buffer.max_version < current_table_version:
                        self.LOGGER.warning('{} - Records from an earlier table version detected.'
                                            .format(stream_buffer.stream))
                        cur.execute('ROLLBACK;')
                        return None

                    elif stream_buffer.max_version > current_table_version:
                        root_table_name = stream_buffer.stream + SEPARATOR + str(stream_buffer.max_version)
                        target_table_version = stream_buffer.max_version

                self._validate_identifier(root_table_name)
                written_batches_details = self.write_batch_helper(cur,
                                                                  root_table_name,
                                                                  stream_buffer.schema,
                                                                  stream_buffer.key_properties,
                                                                  stream_buffer.get_batch(),
                                                                  {'version': target_table_version})

                cur.execute('COMMIT;')

                return written_batches_details
            except Exception as ex:
                cur.execute('ROLLBACK;')
                message = 'Exception writing records'
                self.LOGGER.exception(message)
                raise PostgresError(message, ex)

    def activate_version(self, stream_buffer, version):
        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                table_metadata = self._get_table_metadata(cur,
                                                          stream_buffer.stream)

                if not table_metadata:
                    self.LOGGER.error('{} - Table for stream does not exist'.format(
                        stream_buffer.stream))
                elif table_metadata.get('version') is not None and table_metadata.get('version') >= version:
                    self.LOGGER.warning('{} - Table version {} already active'.format(
                        stream_buffer.stream,
                        version))
                else:
                    versioned_root_table = stream_buffer.stream + SEPARATOR + str(version)

                    cur.execute(
                        sql.SQL('''
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = {} AND tablename like {};
                        ''').format(
                            sql.Literal(self.postgres_schema),
                            sql.Literal(versioned_root_table + '%')))

                    for versioned_table_name in map(lambda x: x[0], cur.fetchall()):
                        table_name = stream_buffer.stream + versioned_table_name[len(versioned_root_table):]
                        cur.execute(
                            sql.SQL('''
                            ALTER TABLE {table_schema}.{stream_table} RENAME TO {stream_table_old};
                            ALTER TABLE {table_schema}.{version_table} RENAME TO {stream_table};
                            DROP TABLE {table_schema}.{stream_table_old};
                            COMMIT;''').format(
                                table_schema=sql.Identifier(self.postgres_schema),
                                stream_table_old=sql.Identifier(table_name +
                                                                SEPARATOR +
                                                                'old'),
                                stream_table=sql.Identifier(table_name),
                                version_table=sql.Identifier(versioned_table_name)))
            except Exception as ex:
                cur.execute('ROLLBACK;')
                message = '{} - Exception activating table version {}'.format(
                    stream_buffer.stream,
                    version)
                self.LOGGER.exception(message)
                raise PostgresError(message, ex)

    def _validate_identifier(self, identifier):
        if not identifier:
            raise PostgresError('Identifier must be non empty.')

        if self.IDENTIFIER_FIELD_LENGTH < len(identifier):
            raise PostgresError('Length of identifier must be less than or equal to {}. Got {} for `{}`'.format(
                self.IDENTIFIER_FIELD_LENGTH,
                len(identifier),
                identifier
            ))

        if not re.match(r'^[a-z_].*', identifier):
            raise PostgresError(
                'Identifier must start with a lower case letter, or underscore. Got `{}` for `{}`'.format(
                    identifier[0],
                    identifier
                ))

        if not re.match(r'^[a-z0-9_$]+$', identifier):
            raise PostgresError(
                'Identifier must only contain lower case letters, numbers, underscores, or dollar signs. Got `{}` for `{}`'.format(
                    re.findall(r'[^0-9]', '1234a567')[0],
                    identifier
                ))

        return True

    def canonicalize_identifier(self, identifier):
        if not identifier:
            identifier = '_'

        return re.sub(r'[^\w\d_$]', '_', identifier.lower())

    def add_key_properties(self, cur, table_name, key_properties):
        if not key_properties:
            return None

        metadata = self._get_table_metadata(cur, table_name)

        if not 'key_properties' in metadata:
            metadata['key_properties'] = key_properties
            self._set_table_metadata(cur, table_name, metadata)

    def add_table(self, cur, name, metadata):
        self._validate_identifier(name)

        create_table_sql = sql.SQL('CREATE TABLE {}.{}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(name))

        cur.execute(sql.SQL('{} ();').format(create_table_sql))

        self._set_table_metadata(cur, name, {'version': metadata.get('version', None),
                                             'schema_version': metadata['schema_version']})

    def add_table_mapping(self, cur, from_path, metadata):
        root_table = from_path[0]
        cur.execute(
            sql.SQL('''
            SELECT EXISTS(
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = {}
                AND table_name = {}
            );
            ''').format(
                sql.Literal(self.postgres_schema),
                sql.Literal(root_table)))

        # No root table present
        ## Table mappings are hung off of the root table's metadata
        ## SQLInterface's helpers do not guarantee order of table creation
        if not cur.fetchone()[0]:
            self.add_table(cur, root_table, metadata)

        metadata = self._get_table_metadata(cur, root_table)
        if not metadata:
            metadata = {}

        if not 'table_mappings' in metadata:
            metadata['table_mappings'] = []

        mapping = self.add_table_mapping_helper(from_path, metadata['table_mappings'])

        if not mapping['exists']:
            metadata['table_mappings'].append({'type': 'TABLE',
                                               'from': from_path,
                                               'to': mapping['to']})
            self._set_table_metadata(cur, root_table, metadata)

        return mapping['to']

    def _get_update_sql(self, target_table_name, temp_table_name, key_properties, columns, subkeys):
        full_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(target_table_name))
        full_temp_table_name = sql.SQL('{}.{}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name))

        pk_temp_select_list = []
        pk_where_list = []
        pk_null_list = []
        cxt_where_list = []
        for pk in key_properties:
            pk_identifier = sql.Identifier(pk)
            pk_temp_select_list.append(sql.SQL('{}.{}').format(full_temp_table_name,
                                                               pk_identifier))

            pk_where_list.append(
                sql.SQL('{table}.{pk} = "dedupped".{pk}').format(
                    table=full_table_name,
                    temp_table=full_temp_table_name,
                    pk=pk_identifier))

            pk_null_list.append(
                sql.SQL('{table}.{pk} IS NULL').format(
                    table=full_table_name,
                    pk=pk_identifier))

            cxt_where_list.append(
                sql.SQL('{table}.{pk} = "pks".{pk}').format(
                    table=full_table_name,
                    pk=pk_identifier))
        pk_temp_select = sql.SQL(', ').join(pk_temp_select_list)
        pk_where = sql.SQL(' AND ').join(pk_where_list)
        pk_null = sql.SQL(' AND ').join(pk_null_list)
        cxt_where = sql.SQL(' AND ').join(cxt_where_list)

        sequence_join = sql.SQL(' AND "dedupped".{} >= {}.{}').format(
            sql.Identifier(SINGER_SEQUENCE),
            full_table_name,
            sql.Identifier(SINGER_SEQUENCE))

        distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
            pk_temp_select,
            full_temp_table_name,
            sql.Identifier(SINGER_SEQUENCE))

        if len(subkeys) > 0:
            pk_temp_subkey_select_list = []
            for pk in (key_properties + subkeys):
                pk_temp_subkey_select_list.append(sql.SQL('{}.{}').format(full_temp_table_name,
                                                                          sql.Identifier(pk)))
            insert_distinct_on = sql.SQL(', ').join(pk_temp_subkey_select_list)

            insert_distinct_order_by = sql.SQL(' ORDER BY {}, {}.{} DESC').format(
                insert_distinct_on,
                full_temp_table_name,
                sql.Identifier(SINGER_SEQUENCE))
        else:
            insert_distinct_on = pk_temp_select
            insert_distinct_order_by = distinct_order_by

        insert_columns_list = []
        dedupped_columns_list = []
        for column in columns:
            insert_columns_list.append(sql.SQL('{}').format(sql.Identifier(column)))
            dedupped_columns_list.append(sql.SQL('{}.{}').format(sql.Identifier('dedupped'),
                                                                 sql.Identifier(column)))
        insert_columns = sql.SQL(', ').join(insert_columns_list)
        dedupped_columns = sql.SQL(', ').join(dedupped_columns_list)

        return sql.SQL('''
            DELETE FROM {table} USING (
                    SELECT "dedupped".*
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY {pk_temp_select}
                                                  {distinct_order_by}) AS "pk_ranked"
                        FROM {temp_table}
                        {distinct_order_by}) AS "dedupped"
                    JOIN {table} ON {pk_where}{sequence_join}
                    WHERE pk_ranked = 1
                ) AS "pks" WHERE {cxt_where};
            INSERT INTO {table}({insert_columns}) (
                SELECT {dedupped_columns}
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {insert_distinct_on}
                                              {insert_distinct_order_by}) AS "pk_ranked"
                    FROM {temp_table}
                    {insert_distinct_order_by}) AS "dedupped"
                LEFT JOIN {table} ON {pk_where}
                WHERE pk_ranked = 1 AND {pk_null}
            );
            DROP TABLE {temp_table};
            ''').format(table=full_table_name,
                        temp_table=full_temp_table_name,
                        pk_temp_select=pk_temp_select,
                        pk_where=pk_where,
                        cxt_where=cxt_where,
                        sequence_join=sequence_join,
                        distinct_order_by=distinct_order_by,
                        pk_null=pk_null,
                        insert_distinct_on=insert_distinct_on,
                        insert_distinct_order_by=insert_distinct_order_by,
                        insert_columns=insert_columns,
                        dedupped_columns=dedupped_columns)

    def serialize_table_record_null_value(self, remote_schema, streamed_schema, field, value):
        if value is None:
            return RESERVED_NULL_DEFAULT
        return value

    def serialize_table_record_datetime_value(self, remote_schema, streamed_schema, field, value):
        return arrow.get(value).format('YYYY-MM-DD HH:mm:ss.SSSSZZ')

    def persist_csv_rows(self,
                         cur,
                         remote_schema,
                         temp_table_name,
                         columns,
                         csv_rows):

        copy = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH CSV NULL AS {}').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.Literal(RESERVED_NULL_DEFAULT))
        cur.copy_expert(copy, csv_rows)

        pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
        subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

        canonicalized_key_properties = [self.fetch_column_from_path((key_property,), remote_schema)[0]
                                        for key_property in remote_schema['key_properties']]

        update_sql = self._get_update_sql(remote_schema['name'],
                                          temp_table_name,
                                          canonicalized_key_properties,
                                          columns,
                                          subkeys)
        cur.execute(update_sql)

    def write_table_batch(self, cur, table_batch, metadata):
        remote_schema = table_batch['remote_schema']

        target_table_name = 'tmp_' + str(uuid.uuid4()).replace('-', '_')

        ## Create temp table to upload new data to
        target_schema = deepcopy(remote_schema)
        target_schema['path'] = (target_table_name,)
        self.upsert_table_helper(cur,
                                 target_schema,
                                 {'version': remote_schema['version']},
                                 log_schema_changes=False)

        ## Make streamable CSV records
        csv_headers = list(remote_schema['schema']['properties'].keys())
        rows_iter = iter(table_batch['records'])

        def transform():
            try:
                row = next(rows_iter)

                with io.StringIO() as out:
                    writer = csv.DictWriter(out, csv_headers)
                    writer.writerow(row)
                    return out.getvalue()
            except StopIteration:
                return ''

        csv_rows = TransformStream(transform)

        ## Persist csv rows
        self.persist_csv_rows(cur,
                              remote_schema,
                              target_table_name,
                              csv_headers,
                              csv_rows)

        return len(table_batch['records'])

    def add_column(self, cur, table_name, column_name, column_schema):

        cur.execute(sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                            'ADD COLUMN {column_name} {data_type};').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            column_name=sql.Identifier(column_name),
            data_type=sql.SQL(self.json_schema_to_sql_type(column_schema))))

    def migrate_column(self, cur, table_name, from_column, to_column):
        cur.execute(sql.SQL('UPDATE {table_schema}.{table_name} ' +
                            'SET {to_column} = {from_column};').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            to_column=sql.Identifier(to_column),
            from_column=sql.Identifier(from_column)))

    def drop_column(self, cur, table_name, column_name):
        cur.execute(sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                            'DROP COLUMN {column_name};').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            column_name=sql.Identifier(column_name)))

    def make_column_nullable(self, cur, table_name, column_name):
        cur.execute(sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                            'ALTER COLUMN {column_name} DROP NOT NULL;').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            column_name=sql.Identifier(column_name)))

    def _set_table_metadata(self, cur, table_name, metadata):
        """
        Given a Metadata dict, set it as the comment on the given table.
        :param self: Postgres
        :param cur: Pscyopg.Cursor
        :param table_name: String
        :param metadata: Metadata Dict
        :return: None
        """
        cur.execute(sql.SQL('COMMENT ON TABLE {}.{} IS {};').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(table_name),
            sql.Literal(json.dumps(metadata))))

    def _get_table_metadata(self, cur, table_name):
        cur.execute(sql.SQL('''
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = {} AND
                      tablename = {});''').format(
            sql.Literal(self.postgres_schema),
            sql.Literal(table_name)))
        table_exists = cur.fetchone()[0]

        if not table_exists:
            return None

        cur.execute(
            sql.SQL('SELECT description FROM pg_description WHERE objoid = {}::regclass;').format(
                sql.Literal(
                    '"{}"."{}"'.format(self.postgres_schema, table_name))))
        comment = cur.fetchone()[0]

        if comment:
            try:
                comment_meta = json.loads(comment)
            except:
                self.LOGGER.exception('Could not load table comment metadata')
                raise
        else:
            comment_meta = None

        return comment_meta

    def add_column_mapping(self, cur, table_name, from_path, to_name, mapped_schema):
        metadata = self._get_table_metadata(cur, table_name)

        if not metadata:
            metadata = {}

        if not 'mappings' in metadata:
            metadata['mappings'] = {}

        mapping = {'type': json_schema.get_type(mapped_schema),
                   'from': from_path}

        if 't' == json_schema.shorthand(mapped_schema):
            mapping['format'] = 'date-time'

        metadata['mappings'][to_name] = mapping

        self._set_table_metadata(cur, table_name, metadata)

    def drop_column_mapping(self, cur, table_name, mapped_name):
        metadata = self._get_table_metadata(cur, table_name)

        if not metadata:
            metadata = {}

        if not 'mappings' in metadata:
            metadata['mappings'] = {}

        metadata['mappings'].pop(mapped_name, None)

        self._set_table_metadata(cur, table_name, metadata)

    def is_table_empty(self, cur, table_name):
        cur.execute(sql.SQL('SELECT COUNT(1) FROM {}.{};').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(table_name)))

        return cur.fetchall()[0][0] == 0

    def get_table_schema(self, cur, path, name):
        cur.execute(
            sql.SQL('SELECT column_name, data_type, is_nullable FROM information_schema.columns ') +
            sql.SQL('WHERE table_schema = {} and table_name = {};').format(
                sql.Literal(self.postgres_schema), sql.Literal(name)))

        properties = {}
        for column in cur.fetchall():
            properties[column[0]] = self.sql_type_to_json_schema(column[1], column[2] == 'YES')

        metadata = self._get_table_metadata(cur, name)

        if metadata is None and not properties:
            return None

        if metadata is None:
            metadata = {'version': None}

        if len(path) > 1:
            table_mappings = self.get_table_schema(cur, path[:1], path[0])['table_mappings']
        else:
            table_mappings = []
            for mapping in metadata.get('table_mappings', []):
                if mapping['type'] == 'TABLE':
                    table_mappings.append(mapping)

        metadata['name'] = name
        metadata['path'] = path
        metadata['type'] = 'TABLE_SCHEMA'
        metadata['schema'] = {'properties': properties}
        metadata['table_mappings'] = table_mappings

        if 0 == metadata.get('schema_version', 0):
            return _update_schema_0_to_1(metadata)

        return metadata

    def sql_type_to_json_schema(self, sql_type, is_nullable):
        """
        Given a string representing a SQL column type, and a boolean indicating whether
        the associated column is nullable, return a compatible JSONSchema structure.
        :param sql_type: string
        :param is_nullable: boolean
        :return: JSONSchema
        """
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
            raise PostgresError('Unsupported type `{}` in existing target table'.format(sql_type))

        json_type = [json_type]
        if is_nullable:
            json_type.append(json_schema.NULL)

        ret_json_schema = {'type': json_type}
        if _format:
            ret_json_schema['format'] = _format

        return ret_json_schema

    def json_schema_to_sql_type(self, schema):
        _type = json_schema.get_type(schema)
        not_null = True
        ln = len(_type)
        if ln == 1:
            _type = _type[0]
        if ln == 2 and json_schema.NULL in _type:
            not_null = False
            if _type.index(json_schema.NULL) == 0:
                _type = _type[1]
            else:
                _type = _type[0]
        elif ln > 2:
            raise PostgresError('Multiple types per column not supported')

        sql_type = 'text'

        if 'format' in schema and \
                schema['format'] == 'date-time' and \
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
