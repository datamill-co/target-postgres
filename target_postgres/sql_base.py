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


def _flatten_schema(stream_buffer, root_table_name, schema):
    subtables = {}
    key_prop_schemas = {}
    for key in stream_buffer.key_properties:
        key_prop_schemas[key] = schema['properties'][key]
    _denest_schema(root_table_name, schema, key_prop_schemas, subtables)

    ret = []
    for name, schema in subtables.items():
        ret.append(to_table_schema(name, schema['level'], schema['key_properties'], schema['properties']))
    return ret


class SQLInterface:
    """
    Generic interface for handling SQL Targets in Singer.

    Provides reasonable defaults for:
    - nested schemas -> traditional SQL Tables and Columns

    Expected usage is to override necessary functions for your
    given target.
    """

    def parse_table_schemas(self, stream_buffer, root_table_name):
        """
        Given a `stream_buffer` return the denested/flattened TABLE_SCHEMA of
        the root table and each sub table.
        :param stream_buffer: SingerStreamBuffer
        :param root_table_name: string
        :return: [TABLE_SCHEMA(denested_streamed_schema_0), ...]
        """
        root_table_schema = json_schema.simplify(stream_buffer.schema)

        _add_singer_columns(root_table_schema, stream_buffer.key_properties)

        return _flatten_schema(stream_buffer, root_table_name, root_table_schema) \
               + [to_table_schema(root_table_name, None, stream_buffer.key_properties, root_table_schema['properties'])]

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
        :param metadata: additional metadata needed to implementing class
        :return: updated_remote_table_json_schema
        """
        raise NotImplementedError('`update_table_schema` not implemented.')

    def update_schema(self, connection, stream_buffer, root_table_name, metadata):
        """
        Update the remote schema based on the `stream_buffer.schema`.
        :param connection: remote connection, type left to be determined by implementing class
        :param stream_buffer: SingerStreamBuffer
        :param root_table_name: string
        :param metadata: additional data for downstream calls
        :return: [{'streamed_schema': TABLE_SCHEMA(denested_streamed_schema_0),
                   'remote_schema': TABLE_SCHEMA(remote),
                   'updated_remote_schema': TABLE_SCHEMA(remote)},
                  ...]
        """
        table_schemas = []
        for table_json_schema in self.parse_table_schemas(stream_buffer, root_table_name):
            remote_schema = self.get_table_schema(connection, table_json_schema['name'])
            table_schemas.append({'streamed_schema': table_json_schema,
                                  'remote_schema': remote_schema,
                                  'updated_remote_schema': self.update_table_schema(connection,
                                                                                    remote_schema,
                                                                                    table_json_schema,
                                                                                    metadata)})

        return table_schemas

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
