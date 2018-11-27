# SQL Base
## This module is the base implementation for Singer SQL target support.
## Expected usage of this module is to create a class representing your given
## SQL Target which overrides SQLInterface.
#
# Transition
## The given implementation here is in transition as we expand and add various
## targets. As such, there are many private helper functions which are providing
## the real support.
##
## The expectation is that these functions will be added to SQLInterface as we
## better understand how to make adding new targets simpler.
#

from copy import deepcopy

from target_postgres import json_schema
from target_postgres.singer_stream import (
    SINGER_RECEIVED_AT,
    SINGER_BATCHED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION,
    SINGER_PK,
    SINGER_SOURCE_PK_PREFIX,
    SINGER_LEVEL
)

SEPARATOR = '__'


def to_table_schema(name, level, keys, properties):
    for key in keys:
        if not key in properties:
            raise Exception('Unknown key "{}" found for table "{}"'.format(
                key, name
            ))

    return {'type': 'TABLE_SCHEMA',
            'name': name,
            'level': level,
            'key_properties': keys,
            'schema': {'type': 'object',
                       'additionalProperties': False,
                       'properties': properties}}


def _add_singer_columns(schema, key_properties):
    properties = schema['properties']

    if SINGER_RECEIVED_AT not in properties:
        properties[SINGER_RECEIVED_AT] = {
            'type': ['null', 'string'],
            'format': 'date-time'
        }

    if SINGER_SEQUENCE not in properties:
        properties[SINGER_SEQUENCE] = {
            'type': ['null', 'integer']
        }

    if SINGER_TABLE_VERSION not in properties:
        properties[SINGER_TABLE_VERSION] = {
            'type': ['null', 'integer']
        }

    if SINGER_BATCHED_AT not in properties:
        properties[SINGER_BATCHED_AT] = {
            'type': ['null', 'string'],
            'format': 'date-time'
        }

    if len(key_properties) == 0:
        properties[SINGER_PK] = {
            'type': ['string']
        }


def _denest_schema_helper(table_name,
                          table_json_schema,
                          not_null,
                          top_level_schema,
                          current_path,
                          key_prop_schemas,
                          subtables,
                          level):
    for prop, item_json_schema in table_json_schema['properties'].items():
        next_path = current_path + SEPARATOR + prop
        if json_schema.is_object(item_json_schema):
            _denest_schema_helper(table_name,
                                  item_json_schema,
                                  not_null,
                                  top_level_schema,
                                  next_path,
                                  key_prop_schemas,
                                  subtables,
                                  level)
        elif json_schema.is_iterable(item_json_schema):
            _create_subtable(table_name + SEPARATOR + prop,
                             item_json_schema,
                             key_prop_schemas,
                             subtables,
                             level + 1)
        else:
            if not_null and json_schema.is_nullable(item_json_schema):
                item_json_schema['type'].remove('null')
            elif not json_schema.is_nullable(item_json_schema):
                item_json_schema['type'].append('null')
            top_level_schema[next_path] = item_json_schema


def _create_subtable(table_name, table_json_schema, key_prop_schemas, subtables, level):
    if json_schema.is_object(table_json_schema['items']):
        new_properties = table_json_schema['items']['properties']
    else:
        new_properties = {'value': table_json_schema['items']}

    key_properties = []
    for pk, item_json_schema in key_prop_schemas.items():
        key_properties.append(SINGER_SOURCE_PK_PREFIX + pk)
        new_properties[SINGER_SOURCE_PK_PREFIX + pk] = item_json_schema

    new_properties[SINGER_SEQUENCE] = {
        'type': ['null', 'integer']
    }

    for i in range(0, level + 1):
        new_properties[SINGER_LEVEL.format(i)] = {
            'type': ['integer']
        }

    new_schema = {'type': ['object'],
                  'properties': new_properties,
                  'level': level,
                  'key_properties': key_properties}

    _denest_schema(table_name, new_schema, key_prop_schemas, subtables, level=level)

    subtables[table_name] = new_schema


def _denest_schema(table_name, table_json_schema, key_prop_schemas, subtables, current_path=None, level=-1):
    new_properties = {}
    for prop, item_json_schema in table_json_schema['properties'].items():
        if current_path:
            next_path = current_path + SEPARATOR + prop
        else:
            next_path = prop

        if json_schema.is_object(item_json_schema):
            not_null = 'null' not in item_json_schema['type']
            _denest_schema_helper(table_name + SEPARATOR + next_path,
                                  item_json_schema,
                                  not_null,
                                  new_properties,
                                  next_path,
                                  key_prop_schemas,
                                  subtables,
                                  level)
        elif json_schema.is_iterable(item_json_schema):
            _create_subtable(table_name + SEPARATOR + next_path,
                             item_json_schema,
                             key_prop_schemas,
                             subtables,
                             level + 1)
        else:
            new_properties[prop] = item_json_schema
    table_json_schema['properties'] = new_properties


def _denest_subrecord(table_name,
                      current_path,
                      parent_record,
                      record,
                      records_map,
                      key_properties,
                      pk_fks,
                      level):
    """"""
    """
    {...}
    """
    for prop, value in record.items():
        """
        str : {...} | [...] | ???None??? | <literal>
        """
        next_path = current_path + SEPARATOR + prop
        if isinstance(value, dict):
            """
            {...}
            """
            # TODO: Throws exception due to wrong number of args.
            _denest_subrecord(table_name, next_path, parent_record, value, pk_fks, level)
        elif isinstance(value, list):
            """
            [...]
            """
            _denest_records(table_name + SEPARATOR + next_path,
                            value,
                            records_map,
                            key_properties,
                            pk_fks=pk_fks,
                            level=level + 1)
        else:
            """
            None | <literal>
            """
            parent_record[next_path] = value


def _denest_record(table_name, current_path, record, records_map, key_properties, pk_fks, level):
    """"""
    """
    {...}
    """
    denested_record = {}
    for prop, value in record.items():
        """
        str : {...} | [...] | None | <literal>
        """
        if current_path:
            next_path = current_path + SEPARATOR + prop
        else:
            next_path = prop

        if isinstance(value, dict):
            """
            {...}
            """
            _denest_subrecord(table_name,
                              next_path,
                              denested_record,
                              value,
                              records_map,
                              key_properties,
                              pk_fks,
                              level)
        elif isinstance(value, list):
            """
            [...]
            """
            _denest_records(table_name + SEPARATOR + next_path,
                            value,
                            records_map,
                            key_properties,
                            pk_fks=pk_fks,
                            level=level + 1)
        elif value is None:  ## nulls mess up nested objects
            """
            None
            """
            continue
        else:
            """
            <literal>
            """
            denested_record[next_path] = value

    if table_name not in records_map:
        records_map[table_name] = []
    records_map[table_name].append(denested_record)


def _denest_records(table_name, records, records_map, key_properties, pk_fks=None, level=-1):
    row_index = 0
    """
    [{...} ...]
    """
    for record in records:
        if pk_fks:
            record_pk_fks = pk_fks.copy()
            record_pk_fks[SINGER_LEVEL.format(level)] = row_index
            for key, value in record_pk_fks.items():
                record[key] = value
            row_index += 1
        else:  ## top level
            record_pk_fks = {}
            for key in key_properties:
                record_pk_fks[SINGER_SOURCE_PK_PREFIX + key] = record[key]
            if SINGER_SEQUENCE in record:
                record_pk_fks[SINGER_SEQUENCE] = record[SINGER_SEQUENCE]

        """
        {...}
        """
        _denest_record(table_name, None, record, records_map, key_properties, record_pk_fks, level)


class SQLInterface:
    """
    Generic interface for handling SQL Targets in Singer.

    Provides reasonable defaults for:
    - nested schemas -> traditional SQL Tables and Columns
    - nested records -> traditional SQL Table rows

    Expected usage for use with your given target is to:
    - override all public _non-helper_ functions
    - use all public _helper_ functions inside of your _non-helper_ functions

    Function Syntax:
    - `_...` prefix : Private function
    - `..._helper` suffix : Helper function
    """

    def _get_streamed_table_schemas(self, root_table_name, schema, key_properties):
        """
        Given a `schema` and `key_properties` return the denested/flattened TABLE_SCHEMA of
        the root table and each sub table.
        :param root_table_name: string
        :param schema: SingerStreamSchema
        :param key_properties: [string, ...]
        :return: [TABLE_SCHEMA(denested_streamed_schema_0), ...]
        """
        root_table_schema = json_schema.simplify(schema)

        _add_singer_columns(root_table_schema, key_properties)

        subtables = {}
        key_prop_schemas = {}
        for key in key_properties:
            key_prop_schemas[key] = schema['properties'][key]
        _denest_schema(root_table_name, root_table_schema, key_prop_schemas, subtables)

        ret = [to_table_schema(root_table_name, None, key_properties, root_table_schema['properties'])]
        for name, schema in subtables.items():
            ret.append(to_table_schema(name, schema['level'], schema['key_properties'], schema['properties']))

        return ret

    def get_table_schema(self, connection, name):
        """
        Fetch the `table_schema` for `name`.
        :param connection: remote connection, type left to be determined by implementing class
        :param name: string
        :return: TABLE_SCHEMA(remote)
        """
        raise NotImplementedError('`get_table_schema` not implemented.')

    def update_table_schema(self, connection, remote_table_json_schema, table_json_schema, metadata):
        """
        Update the remote table schema based on the merged difference between
        `remote_table_json_schema` and `table_json_schema`.
        :param connection: remote connection, type left to be determined by implementing class
        :param remote_table_json_schema: get_table_schema
        :param table_json_schema: updates for get_table_schema
        :param metadata: additional metadata needed by implementing class
        :return: updated_remote_table_json_schema
        """
        raise NotImplementedError('`update_table_schema` not implemented.')

    def _get_streamed_table_records(self, root_table_name, key_properties, records):
        """
        Given `records` for the `root_table_name` and `key_properties`, flatten
        into `table_records`.
        :param root_table_name: string
        :param key_properties: [string, ...]
        :param records: [{...}, ...]
        :return: {TableName string: [{...}, ...],
                  ...}
        """

        records_map = {}
        _denest_records(root_table_name,
                        records,
                        records_map,
                        key_properties)
        return records_map

    def _get_table_batches(self, connection, root_table_name, schema, key_properties, records):
        """
        Given the streamed schema, and records, get all table schemas and records and prep them
        in a `table_batch`.
        :param connection: remote connection, type left to be determined by implementing class
        :param root_table_name: string
        :param schema: SingerStreamSchema
        :param key_properties: [string, ...]
        :param records: [{...}, ...]
        :return: [{'streamed_schema': TABLE_SCHEMA(local),
                   'remote_schema': TABLE_SCHEMA(remote),
                   'records': [{...}, ...]
        """

        table_schemas = self._get_streamed_table_schemas(root_table_name,
                                                         schema,
                                                         key_properties)

        table_records = self._get_streamed_table_records(root_table_name,
                                                         key_properties,
                                                         records)
        writeable_batches = []
        for table_json_schema in table_schemas:
            remote_schema = self.get_table_schema(connection, table_json_schema['name'])
            writeable_batches.append({'streamed_schema': table_json_schema,
                                      'remote_schema': remote_schema,
                                      'records': table_records.get(table_json_schema['name'], [])})

        return writeable_batches

    def serialize_table_record_field_name(
            self, remote_schema, streamed_schema, field, value):
        """
        Returns the appropriate remote field (column) name for `field`.
        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param field: string
        :param value: literal
        :return: string
        """
        raise NotImplementedError('`parse_table_record_serialize_field_name` not implemented.')

    def serialize_table_record_null_value(
            self, remote_schema, streamed_schema, field, value):
        """
        Returns the serialized version of `value` which is appropriate for the target's null
        implementation.
        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param field: string
        :param value: literal
        :return: literalg
        """
        raise NotImplementedError('`parse_table_record_serialize_null_value` not implemented.')

    def serialize_table_record_datetime_value(
            self, remote_schema, streamed_schema, field, value):
        """
        Returns the serialized version of `value` which is appropriate  for the target's datetime
        implementation.
        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param field: string
        :param value: literal
        :return: literal
        """

        raise NotImplementedError('`parse_table_record_serialize_datetime_value` not implemented.')

    def _serialize_table_records(
            self, remote_schema, streamed_schema, records):
        """
        Parse the given table's `records` in preparation for persistence to the remote target.

        Base implementation returns a list of dictionaries, where _every_ dictionary has the
        same keys as `remote_schema`'s properties.

        :param remote_schema: TABLE_SCHEMA(remote)
        :param streamed_schema: TABLE_SCHEMA(local)
        :param records: [{...}, ...]
        :return: [{...}, ...]
        """

        datetime_fields = [k for k, v in streamed_schema['schema']['properties'].items()
                           if v.get('format') == 'date-time']

        default_fields = {k: v.get('default') for k, v in streamed_schema['schema']['properties'].items()
                          if v.get('default') is not None}

        ## Get remote fields and streamed fields.
        ### `remote_fields` determine which keys are allowed to be serialized into `serialized_rows`
        ### but the `streamed_schema` might have fields which are not present in remote due to
        ### `parse_table_record_serialize_field_name`
        remote_fields = set(remote_schema['schema']['properties'].keys())
        fields = remote_fields.union(set(streamed_schema['schema']['properties'].keys()))

        ## Get the default NULL value so we can assign row values when value is _not_ NULL
        NULL_DEFAULT = self.serialize_table_record_null_value(remote_schema, streamed_schema, None, None)

        serialized_rows = []
        default_row = dict([(field, NULL_DEFAULT) for field in remote_fields])

        for record in records:
            row = deepcopy(default_row)

            for field in fields:
                value = record.get(field, None)

                ## Serialize fields which are not present but have default values set
                if field in default_fields \
                        and value is None:
                    value = default_fields[field]

                ## Serialize datetime to compatible format
                if field in datetime_fields \
                        and value is not None:
                    value = self.serialize_table_record_datetime_value(remote_schema, streamed_schema, field,
                                                                       value)

                ## Serialize NULL default value
                value = self.serialize_table_record_null_value(remote_schema, streamed_schema, field, value)

                field_name = self.serialize_table_record_field_name(remote_schema, streamed_schema, field, value)

                if field_name in remote_fields \
                        and not field_name in row \
                        or row[field_name] == NULL_DEFAULT:
                    row[field_name] = value

            serialized_rows.append(row)

        return serialized_rows

    def write_table_batch(self, connection, table_batch, metadata):
        """
        Update the remote for given table's schema, and write records. Returns the number of
        records persisted.

        :param connection: remote connection, type left to be determined by implementing class
        :param table_batch: {'remote_schema': TABLE_SCHEMA(remote),
                             'records': [{...}, ...]}
        :param metadata: additional metadata needed by implementing class
        :return: integer
        """
        raise NotImplementedError('`write_table_batch` not implemented.')

    def write_batch_helper(self, connection, root_table_name, schema, key_properties, records, metadata):
        """
        Write all `table_batch`s associated with the given `schema` and `records` to remote.

        :param connection: remote connection, type left to be determined by implementing class
        :param root_table_name: string
        :param schema: SingerStreamSchema
        :param key_properties: [string, ...]
        :param records: [{...}, ...]
        :param metadata: additional metadata needed by implementing class
        :return: {'records_persisted': int,
                  'rows_persisted': int}
        """
        records_persisted = len(records)
        rows_persisted = 0
        for table_batch in self._get_table_batches(connection, root_table_name, schema, key_properties, records):
            remote_schema = self.update_table_schema(connection,
                                                     table_batch['remote_schema'],
                                                     table_batch['streamed_schema'],
                                                     metadata)
            rows_persisted += self.write_table_batch(
                connection,
                {'remote_schema': remote_schema,
                 'records': self._serialize_table_records(remote_schema,
                                                          table_batch['streamed_schema'],
                                                          table_batch['records'])},
                metadata)

        return {
            'records_persisted': records_persisted,
            'rows_persisted': rows_persisted
        }

    def write_batch(self, stream_buffer):
        """
        Persist `stream_buffer.records` to remote.
        :param stream_buffer: SingerStreamBuffer
        :return: {'records_persisted': int,
                  'rows_persisted': int}
        """
        raise NotImplementedError('`write_batch` not implemented.')

    def activate_version(self, stream_buffer, version):
        """
        Activate the given `stream_buffer`'s remote to `version`
        :param stream_buffer: SingerStreamBuffer
        :param version: integer
        :return: boolean
        """
        raise NotImplementedError('`activate_version` not implemented.')
