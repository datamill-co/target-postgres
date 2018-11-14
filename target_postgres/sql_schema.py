import target_postgres.json_schema as json_schema
from target_postgres.singer_stream import (
    SINGER_RECEIVED_AT,
    SINGER_BATCHED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION,
    SINGER_PK,
    SINGER_SOURCE_PK_PREFIX,
    SINGER_LEVEL
)

NESTED_SEPARATOR = '__'


def _denest_schema(table_name, table_json_schema, key_prop_schemas, subtables, current_path=None, level=-1):
    new_properties = {}
    for prop, item_json_schema in table_json_schema['properties'].items():
        if current_path:
            next_path = current_path + NESTED_SEPARATOR + prop
        else:
            next_path = prop

        if json_schema.is_object(item_json_schema):
            not_null = 'null' not in item_json_schema['type']
            _denest_schema_helper(table_name + NESTED_SEPARATOR + next_path,
                                  item_json_schema,
                                  not_null,
                                  new_properties,
                                  next_path,
                                  key_prop_schemas,
                                  subtables,
                                  level)
        elif json_schema.is_iterable(item_json_schema):
            _create_subtable(table_name + NESTED_SEPARATOR + next_path,
                             item_json_schema,
                             key_prop_schemas,
                             subtables,
                             level + 1)
        else:
            new_properties[prop] = item_json_schema
    table_json_schema['properties'] = new_properties


def _create_subtable(table_name, table_json_schema, key_prop_schemas, subtables, level):
    if json_schema.is_object(table_json_schema['items']):
        new_properties = table_json_schema['items']['properties']
    else:
        new_properties = {'value': table_json_schema['items']}

    nested_key_properties = []
    for pk, item_json_schema in key_prop_schemas.items():
        nested_pk = SINGER_SOURCE_PK_PREFIX + pk
        nested_key_properties.append(nested_pk)
        new_properties[nested_pk] = item_json_schema

    new_properties[SINGER_SEQUENCE] = {
        'type': ['null', 'integer']
    }

    for i in range(0, level + 1):
        new_properties[SINGER_LEVEL.format(i)] = {
            'type': ['integer']
        }

    new_schema = {'type': ['object'], 'properties': new_properties}

    _denest_schema(table_name, new_schema, key_prop_schemas, subtables, level=level)

    subtables.append(TableSchema(table_name, new_schema, nested_key_properties))


def _denest_schema_helper(
        table_name,
        table_json_schema,
        not_null,
        top_level_schema,
        current_path,
        key_prop_schemas,
        subtables,
        level):
    for prop, item_json_schema in table_json_schema['properties'].items():
        next_path = current_path + NESTED_SEPARATOR + prop
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
            _create_subtable(table_name + NESTED_SEPARATOR + prop,
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


class TableSchema(object):
    def __init__(self, name, schema, key_properties):
        self.name = name
        self.schema = schema
        self.key_properties = key_properties

    def get_name(self):
        return self.name

    def get_schema(self):
        return self.schema

    def get_key_properties(self):
        return self.key_properties


class SQLSchema(object):
    def __init__(self, table_name, stream_buffer):
        temp_json_schema = json_schema.simplify(stream_buffer.schema)
        _add_singer_columns(temp_json_schema, stream_buffer.key_properties)
        key_prop_schemas = {}
        for key in stream_buffer.key_properties:
            key_prop_schemas[key] = temp_json_schema['properties'][key]

        self.tables = [TableSchema(table_name, temp_json_schema, stream_buffer.key_properties)]
        _denest_schema(table_name,
                       temp_json_schema,
                       key_prop_schemas,
                       self.tables)

    def get_tables(self):
        """
        Return the table schemas for the given SQLSchema. The
        root table's schema will be first, with all other sub table
        schemas following.

        :return: [root_table_schema, nested_table_schema_0, ...]
        """

        return self.tables
