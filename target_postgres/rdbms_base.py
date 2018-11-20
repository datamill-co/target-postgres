# RDBMS Base
## This module is the base implementation for Singer RDBMS target support.
## Expected usage of this module is to create a class representing your given
## RDBMS Target which overrides RDBMSInterface.
#
# Transition
## The given implementation here is in transition as we expand and add various
## targets. As such, there are many private helper functions which are providing
## the real support.
##
## The expectation is that these functions will be added to RDBMSInterface as we
## better understand how to make adding new targets simpler.
#

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


class RDBMSInterface():
    """
    Generic interface for handling RDBMS in Singer.

    Provides reasonable defaults for:
    - nested schemas -> traditional SQL Tables and Columns

    Expected usage is to override necessary functions for your
    given target.
    """

    def parse_table_schemas(self, x_json_schema):
        """"""
        return []

    def get_table_schema(self, name):
        """"""
        return to_table_schema(name, 0, [], {})

    def update_table_schema(self, table_json_schema):
        """"""
        remote_json_schema = self.get_table_schema(table_json_schema['name'])
        return remote_json_schema

    def update_schema(self, stream_buffer):
        """
        Update the remote schema based on the stream_buffer.schema provided on init.
        :return: Updated table_schemas.
        """
        table_schemas = {}
        for table_json_schema in self.parse_table_schemas(x_json_schema):
            table_schemas[table_json_schema['name']] = self.update_table_schema(table_json_schema)

        return table_schemas

    def write_batch(self):
        """
        Persist stream_buffer.records to remote.
        :return: {'records_persisted': int,
                  'rows_persisted': int}
        """
        return {'records_persisted': 0, 'rows_persisted': 0}

    def activate_version(self, stream_buffer, version):
        """
        Activate the given stream_buffer to version
        :return: boolean
        """
        return False
