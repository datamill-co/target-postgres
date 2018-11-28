import io
import re
import csv
import uuid
import json
from functools import partial

import arrow
from psycopg2 import sql

from target_postgres import json_schema
from target_postgres.sql_base import SQLInterface, SEPARATOR
from target_postgres.singer_stream import (
    SINGER_RECEIVED_AT,
    SINGER_BATCHED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION,
    SINGER_PK,
    SINGER_LEVEL
)

RESERVED_NULL_DEFAULT = 'NULL'


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
    def __init__(self, connection, logger, *args, postgres_schema='public', **kwargs):
        self.conn = connection
        self.logger = logger
        self.postgres_schema = postgres_schema

    def write_batch(self, stream_buffer):
        if stream_buffer.count == 0:
            return

        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                processed_records = list(map(partial(self.process_record_message,
                                                     stream_buffer.use_uuid_pk,
                                                     self.get_postgres_datetime()),
                                             stream_buffer.peek_buffer()))
                versions = set()
                max_version = None
                for record in processed_records:
                    record_version = record.get(SINGER_TABLE_VERSION)
                    if record_version is not None and \
                       (max_version is None or record_version > max_version):
                        max_version = record_version
                    versions.add(record_version)

                current_table_schema = self.get_table_schema(cur,
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

                if max_version is not None:
                    target_table_version = max_version
                else:
                    target_table_version = None

                if current_table_version is not None and \
                        min(versions) < current_table_version:
                    self.logger.warning('{} - Records from an earlier table version detected.'
                                        .format(stream_buffer.stream))
                if len(versions) > 1:
                    self.logger.warning('{} - Multiple table versions in stream, only using the latest.'
                                        .format(stream_buffer.stream))

                if current_table_version is not None and \
                   target_table_version > current_table_version:
                    root_table_name = stream_buffer.stream + SEPARATOR + str(target_table_version)
                else:
                    root_table_name = stream_buffer.stream

                if target_table_version is not None:
                    records = list(filter(lambda x: x.get(SINGER_TABLE_VERSION) == target_table_version,
                                          processed_records))
                else:
                    records = processed_records

                for key in stream_buffer.key_properties:
                    if current_table_schema \
                            and json_schema.get_type(current_table_schema['schema']['properties'][key]) \
                            != json_schema.get_type(stream_buffer.schema['properties'][key]):
                        raise PostgresError(
                            ('`key_properties` type change detected for "{}". ' +
                             'Existing values are: {}. ' +
                             'Streamed values are: {}').format(
                                key,
                                json_schema.get_type(current_table_schema['schema']['properties'][key]),
                                json_schema.get_type(stream_buffer.schema['properties'][key])
                            ))

                written_batches_details = self.write_batch_helper(cur,
                                                                  root_table_name,
                                                                  stream_buffer.schema,
                                                                  stream_buffer.key_properties,
                                                                  records,
                                                                  {'version': target_table_version})

                cur.execute('COMMIT;')

                stream_buffer.flush_buffer()

                return written_batches_details
            except Exception as ex:
                cur.execute('ROLLBACK;')
                message = 'Exception writing records'
                self.logger.exception(message)
                raise PostgresError(message, ex)

    def activate_version(self, stream_buffer, version):
        with self.conn.cursor() as cur:
            try:
                cur.execute('BEGIN;')

                table_metadata = self.get_table_metadata(cur,
                                                         stream_buffer.stream)

                if not table_metadata:
                    self.logger.error('{} - Table for stream does not exist'.format(
                        stream_buffer.stream))
                elif table_metadata.get('version') == version:
                    self.logger.warning('{} - Table version {} already active'.format(
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
                self.logger.exception(message)
                raise PostgresError(message, ex)

    def process_record_message(self, use_uuid_pk, batched_at, record_message):
        record = record_message['record']

        if 'version' in record_message:
            record[SINGER_TABLE_VERSION] = record_message['version']

        if 'time_extracted' in record_message and record.get(SINGER_RECEIVED_AT) is None:
            record[SINGER_RECEIVED_AT] = record_message['time_extracted']

        if use_uuid_pk and record.get(SINGER_PK) is None:
            record[SINGER_PK] = uuid.uuid4()

        record[SINGER_BATCHED_AT] = batched_at

        if 'sequence' in record_message:
            record[SINGER_SEQUENCE] = record_message['sequence']
        else:
            record[SINGER_SEQUENCE] = arrow.get().timestamp

        return record

    def _is_valid_identifier(self, identifier):
        ## NAMEDATALEN _defaults_ to 64 in PostgreSQL. The maxmimum length for an identifier is
        ## NAMEDATALEN - 1.
        # TODO: Figure out way to `SELECT` value from commands
        NAMEDATALEN = 63

        if not re.match(r'^[a-z_][a-z0-9_$]*', identifier):
            return False

        if NAMEDATALEN < len(identifier):
            return False

        return True

    def _validate_identifier(self, identifier):
        if not self._is_valid_identifier(identifier):
            raise PostgresError()

    def canonicalize_identifier(self, identifier):
        new_idenfitier = re.sub(r'[^\w\d_$]', '_', identifier.lower())
        self._validate_identifier(new_idenfitier)
        return new_idenfitier

    def update_table_schema(self, cur, remote_table_json_schema, table_json_schema, metadata):
        self._validate_identifier(table_json_schema['name'])

        if remote_table_json_schema is not None:
            self.merge_put_schemas(cur,
                                   remote_table_json_schema['name'],
                                   remote_table_json_schema,
                                   table_json_schema)
        else:
            self.create_table(cur,
                              table_json_schema['name'],
                              table_json_schema,
                              metadata.get('version', None))

        return self.get_table_schema(cur, table_json_schema['name'])

    def get_update_sql(self, target_table_name, temp_table_name, key_properties, subkeys):
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
                sql.SQL('{table}.{pk} = {temp_table}.{pk}').format(
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

        sequence_join = sql.SQL(' AND {}.{} >= {}.{}').format(
            full_temp_table_name,
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

        return sql.SQL('''
            WITH "pks" AS (
                SELECT DISTINCT ON ({pk_temp_select}) {pk_temp_select}
                FROM {temp_table}
                JOIN {table} ON {pk_where}{sequence_join}{distinct_order_by}
            )
            DELETE FROM {table} USING "pks" WHERE {cxt_where};
            INSERT INTO {table} (
                SELECT DISTINCT ON ({insert_distinct_on}) {temp_table}.*
                FROM {temp_table}
                LEFT JOIN {table} ON {pk_where}
                WHERE {pk_null}
                {insert_distinct_order_by}
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
                        insert_distinct_order_by=insert_distinct_order_by)

    def serialize_table_record_field_name(self, remote_schema, streamed_schema, field, value):
        if field in streamed_schema['schema']['properties']:
            return self.get_mapping(remote_schema,
                                    field,
                                    streamed_schema['schema']['properties'][field]) \
                   or field
        return field

    def serialize_table_record_null_value(self, remote_schema, streamed_schema, field, value):
        if value is None:
            return RESERVED_NULL_DEFAULT
        return value

    def serialize_table_record_datetime_value(self, remote_schema, streamed_schema, field, value):
        return self.get_postgres_datetime(value)

    def persist_csv_rows(self,
                         cur,
                         remote_schema,
                         temp_table_name,
                         columns,
                         csv_rows):

        copy = sql.SQL('COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, NULL {})').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(temp_table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.Literal(RESERVED_NULL_DEFAULT))
        cur.copy_expert(copy, csv_rows)

        pattern = re.compile(SINGER_LEVEL.format('[0-9]+'))
        subkeys = list(filter(lambda header: re.match(pattern, header) is not None, columns))

        update_sql = self.get_update_sql(remote_schema['name'],
                                         temp_table_name,
                                         remote_schema['key_properties'],
                                         subkeys)
        cur.execute(update_sql)

    def write_table_batch(self, cur, table_batch, metadata):
        remote_schema = table_batch['remote_schema']

        target_table_name = self.get_temp_table_name(remote_schema['name'])

        ## Create temp table to upload new data to
        self.create_table(cur,
                          target_table_name,
                          remote_schema,
                          remote_schema['version'])

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

    def get_postgres_datetime(self, *args):
        if len(args) > 0:
            parsed_datetime = arrow.get(args[0])
        else:
            parsed_datetime = arrow.get() # defaults to UTC now
        return parsed_datetime.format('YYYY-MM-DD HH:mm:ss.SSSSZZ')

    def add_column(self, cur, table_name, column_name, column_schema):
        canonicalized_name = self.canonicalize_identifier(column_name)

        if canonicalized_name != column_name:
            self.add_column_mapping(cur, table_name, column_name, canonicalized_name, column_schema)

        data_type = json_schema.to_sql(column_schema)

        if not json_schema.is_nullable(column_schema) \
                and not self.is_table_empty(cur, table_name):
            self.logger.warning('Forcing new column `{}.{}.{}` to be nullable due to table not empty.'.format(
                self.postgres_schema,
                table_name,
                canonicalized_name))
            data_type = json_schema.to_sql(json_schema.make_nullable(column_schema))

        to_execute = sql.SQL('ALTER TABLE {table_schema}.{table_name} ' +
                             'ADD COLUMN {column_name} {data_type};').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            column_name=sql.Identifier(canonicalized_name),
            data_type=sql.SQL(data_type))

        try:
            cur.execute(to_execute)
        except Exception as ex:
            ## Name collision with non mapped, non canonicalized name
            if column_name != canonicalized_name:
                raise PostgresError(
                    'Cannot add canonicalized column `{}` as `{}` to table `{}` with schema {}. Name collision likely'.format(
                        column_name,
                        canonicalized_name,
                        table_name,
                        str(column_schema)
                    ))
            else:
                raise ex

    def migrate_column(self, cur, table_name, column_name, mapped_name):
        cur.execute(sql.SQL('UPDATE {table_schema}.{table_name} ' +
                            'SET {mapped_name} = {column_name};').format(
            table_schema=sql.Identifier(self.postgres_schema),
            table_name=sql.Identifier(table_name),
            mapped_name=sql.Identifier(mapped_name),
            column_name=sql.Identifier(column_name)))

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

    def set_table_metadata(self, cur, table_name, metadata):
        """
        Given a Metadata dict, set it as the comment on the given table.
        :param self: Postgres
        :param cur: Pscyopg.Cursor
        :param table_name: String
        :param metadata: Metadata Dict
        :return: None
        """

        parsed_metadata = {'key_properties': metadata.get('key_properties', []),
                           'version': metadata.get('version'),
                           'mappings': metadata.get('mappings', {})}

        cur.execute(sql.SQL('COMMENT ON TABLE {}.{} IS {};').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(table_name),
            sql.Literal(json.dumps(parsed_metadata))))


    def create_table(self, cur, table_name, schema, table_version):
        create_table_sql = sql.SQL('CREATE TABLE {}.{}').format(
                sql.Identifier(self.postgres_schema),
                sql.Identifier(table_name))

        cur.execute(sql.SQL('{} ();').format(create_table_sql))

        if 'key_properties' in schema:
            self.set_table_metadata(cur, table_name,
                                    {'key_properties': schema['key_properties'],
                                     'version': table_version})

        remote_table_json_schema = self.get_table_schema(cur, table_name)

        self.merge_put_schemas(cur,
                               remote_table_json_schema['name'],
                               remote_table_json_schema,
                               schema)

    def get_temp_table_name(self, stream_name):
        return stream_name + SEPARATOR + str(uuid.uuid4()).replace('-', '')

    def get_table_metadata(self, cur, table_name):
        cur.execute(
            sql.SQL('SELECT obj_description(to_regclass({}));').format(
                sql.Literal('{}.{}'.format(self.postgres_schema, table_name))))
        comment = cur.fetchone()[0]

        if comment:
            try:
                comment_meta = json.loads(comment)
            except Exception as ex:
                message = 'Could not load table comment metadata'
                self.logger.exception(message)
                raise PostgresError(message, ex)
        else:
            comment_meta = None

        return comment_meta


    def add_column_mapping(self, cur, table_name, column_name, mapped_name, mapped_schema):
        metadata = self.get_table_metadata(cur, table_name)

        if not metadata:
            metadata = {}

        if not 'mappings' in metadata:
            metadata['mappings'] = {}

        metadata['mappings'][mapped_name] = {'type': json_schema.get_type(mapped_schema),
                                             'from': column_name}

        self.set_table_metadata(cur, table_name, metadata)


    def remove_column_mapping(self, cur, table_name, mapped_name):
        metadata = self.get_table_metadata(cur, table_name)

        if not metadata:
            metadata = {}

        if not 'mappings' in metadata:
            metadata['mappings'] = {}

        metadata['mappings'].pop(mapped_name, None)

        self.set_table_metadata(cur, table_name, metadata)


    def is_table_empty(self, cur, table_name):
        cur.execute(sql.SQL('SELECT COUNT(1) FROM {}.{};').format(
            sql.Identifier(self.postgres_schema),
            sql.Identifier(table_name)))

        return cur.fetchall()[0][0] == 0

    def get_table_schema(self, cur, table_name):
        cur.execute(
            sql.SQL('SELECT column_name, data_type, is_nullable FROM information_schema.columns ') +
            sql.SQL('WHERE table_schema = {} and table_name = {};').format(
                sql.Literal(self.postgres_schema), sql.Literal(table_name)))

        properties = {}
        for column in cur.fetchall():
            properties[column[0]] = json_schema.from_sql(column[1], column[2] == 'YES')

        metadata = self.get_table_metadata(cur, table_name)

        if metadata is None and not properties:
            return None
        elif metadata is None:
            metadata = {'version': None}

        metadata['name'] = table_name
        metadata['type'] = 'TABLE_SCHEMA'
        metadata['schema'] = {'properties': properties}

        return metadata

    def mapping_name(self, field, schema):
        return field + SEPARATOR + json_schema.sql_shorthand(schema)

    def get_mapping(self, existing_schema, field, schema):
        canonicalized_field = self.canonicalize_identifier(field)
        typed_field = self.mapping_name(canonicalized_field, schema)

        if 'mappings' not in existing_schema:
            return None

        if canonicalized_field in existing_schema['mappings'] \
                and field == existing_schema['mappings'][canonicalized_field]['from'] \
                and json_schema.get_type(json_schema.make_nullable(schema)) \
                == json_schema.get_type(json_schema.make_nullable(existing_schema['mappings'][canonicalized_field])):

            return canonicalized_field

        if typed_field in existing_schema['mappings'] \
                and field == existing_schema['mappings'][typed_field]['from']:

            return typed_field

        return None

    def split_column(self, cur, table_name, raw_column_name, column_schema, existing_schema):
        existing_properties = existing_schema['schema']['properties']
        column_name = self.canonicalize_identifier(raw_column_name)

        if self.get_mapping(existing_schema, raw_column_name, existing_properties[column_name]):
            self.remove_column_mapping(cur, table_name, column_name)

        ## column_name -> column_name__<current-type>, column_name__<new-type>
        existing_column_mapping = self.mapping_name(column_name, existing_properties[column_name])
        new_column_mapping = self.mapping_name(column_name, column_schema)

        ## Update existing properties
        existing_properties[existing_column_mapping] = json_schema.make_nullable(existing_properties[column_name])
        existing_properties[new_column_mapping] = json_schema.make_nullable(column_schema)

        ## Add new columns
        ### NOTE: all migrated columns will be nullable and remain that way

        #### Table Metadata
        self.add_column_mapping(cur, table_name, raw_column_name,
                                existing_column_mapping,
                                existing_properties[existing_column_mapping])
        self.add_column_mapping(cur, table_name, raw_column_name,
                                new_column_mapping,
                                existing_properties[new_column_mapping])

        #### Columns
        self.add_column(cur,
                        table_name,
                        existing_column_mapping,
                        existing_properties[existing_column_mapping])

        self.add_column(cur,
                        table_name,
                        new_column_mapping,
                        existing_properties[new_column_mapping])

        ## Migrate existing data
        self.migrate_column(cur,
                            table_name,
                            column_name,
                            existing_column_mapping)

        ## Drop existing column
        self.drop_column(cur,
                         table_name,
                         column_name)

        ## Remove column (field) from existing_properties
        del existing_properties[column_name]

    def merge_put_schemas(self, cur, table_name, existing_schema, new_schema):
        new_properties = new_schema['schema']['properties']
        existing_properties = existing_schema['schema']['properties']
        for raw_name, schema in new_properties.items():
            name = self.canonicalize_identifier(raw_name)

            ## Mapping for this column exists
            if self.get_mapping(existing_schema, raw_name, schema) is not None:
                pass

            ## Mapping for similarly named column exists
            ### (ie, columns which have already been split due to type)
            elif 'mappings' in existing_schema \
                    and raw_name in [v['from'] for v in existing_schema['mappings'].values()]:

                existing_properties = existing_schema['schema']['properties']

                ## column_name -> column_name__<new-type>
                new_column_mapping = self.mapping_name(name, schema)

                ## Update existing properties
                existing_properties[new_column_mapping] = json_schema.make_nullable(schema)

                ## Add new column
                ### NOTE: all migrated columns will be nullable and remain that way

                #### Table Metadata
                self.add_column_mapping(cur, table_name, raw_name,
                                        new_column_mapping,
                                        existing_properties[new_column_mapping])

                #### Columns
                self.add_column(cur,
                                table_name,
                                new_column_mapping,
                                existing_properties[new_column_mapping])

            ## New column
            elif name not in existing_properties:

                self.add_column(cur,
                                table_name,
                                raw_name,
                                schema)
                existing_properties[name] = schema

            ## Existing column non-nullable, new column is nullable
            elif not json_schema.is_nullable(existing_properties[name]) \
                    and json_schema.get_type(schema) \
                    == json_schema.get_type(json_schema.make_nullable(existing_properties[name])):

                self.make_column_nullable(cur,
                                          table_name,
                                          name)
                existing_properties[name] = json_schema.make_nullable(existing_properties[name])

            ## Existing column, types compatible
            elif json_schema.to_sql(json_schema.make_nullable(schema)) \
                    == json_schema.to_sql(json_schema.make_nullable(existing_properties[name])):
                pass

            ## Column type change
            elif self.mapping_name(name, schema) not in existing_properties \
                and self.mapping_name(name, existing_properties[name]) not in existing_properties:

                self.split_column(cur,
                                  table_name,
                                  raw_name,
                                  schema,
                                  existing_schema)

            ## Error
            else:
                raise PostgresError(
                    'Cannot handle column type change for: {}.{} columns {} and {}. Name collision likely.'.format(
                        self.postgres_schema,
                        table_name,
                        name,
                        self.mapping_name(name, schema)
                    ))
