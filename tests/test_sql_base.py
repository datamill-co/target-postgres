from copy import deepcopy

import singer

from target_postgres import json_schema
from target_postgres.sql_base import SQLInterface

from tests.fixtures import CatStream, CONFIG, MultiTypeStream, NestedStream, assert_columns_equal
from target_postgres.sql_main import stream_to_target


class FakeLastWriteWinsTarget(SQLInterface):
    """
    Makes a simple SQLInterface Target which makes the records
    persisted the latest written each time.

    Does not support upserting records, etc.
    """
    IDENTIFIER_FIELD_LENGTH = 50

    def __init__(self):
        self.logger = singer.get_logger()
        self.tables = {}
        self.table_mappings = []

    def activate_version(self, stream_buffer, version):
        return None

    def write_batch(self, stream_buffer):
        return self.write_batch_helper(None,
                                       stream_buffer.stream,
                                       stream_buffer.schema,
                                       stream_buffer.key_properties,
                                       stream_buffer.get_batch(),
                                       {})

    def write_table_batch(self, _connection, table_batch, metadata):
        self.tables[table_batch['remote_schema']['name']]['records'] += table_batch['records']
        return len(table_batch['records'])

    def add_table(self, _connection, name, metadata):
        self.tables[name] = {'metadata': metadata,
                             'columns': {},
                             'records': [],
                             'mappings': {}}

    def add_table_mapping(self, _connection, from_path, metadata):
        mapping = self.add_table_mapping_helper(from_path, self.table_mappings)

        if not mapping['exists']:
            self.table_mappings.append({'type': 'TABLE',
                                        'from': from_path,
                                        'to': mapping['to']})

        return mapping['to']

    def add_key_properties(self, _connection, table_name, key_properties):
        if not key_properties:
            return None

        metadata = self.tables[table_name]['metadata']

        if not 'key_properties' in metadata:
            metadata['key_properties'] = key_properties

    def is_table_empty(self, _connection, table_name):
        return not bool(self.tables[table_name]['records'])

    def canonicalize_identifier(self, identifier):
        return identifier.lower()

    def serialize_table_record_null_value(self, remote_schema, streamed_schema, field, value):
        return value

    def serialize_table_record_datetime_value(self, remote_schema, streamed_schema, field, value):
        return value

    def add_column(self, _connection, table_name, column_name, column_schema):
        type = json_schema.get_type(column_schema)
        schema = {'type': type}
        if 'format' in column_schema and json_schema.STRING in type:
            schema['format'] = column_schema['format']

        self.tables[table_name]['columns'][column_name] = schema

        for record in self.tables[table_name]['records']:
            record[column_name] = None

    def migrate_column(self, _connection, table_name, from_column, to_column):
        for record in self.tables[table_name]['records']:
            record[to_column] = record[from_column]

    def drop_column(self, _connection, table_name, column_name):
        for record in self.tables[table_name]['records']:
            del record[column_name]

        del self.tables[table_name]['columns'][column_name]

    def make_column_nullable(self, _connection, table_name, column_name):
        column_schema = self.tables[table_name]['columns'][column_name]
        self.tables[table_name]['columns'][column_name] = json_schema.make_nullable(column_schema)

    def add_column_mapping(self, _connection, table_name, from_path, to_name, mapped_schema):
        self.tables[table_name]['mappings'][to_name] = {'type': json_schema.get_type(mapped_schema),
                                                        'from': from_path}

    def drop_column_mapping(self, _connection, table_name, mapped_name):
        del self.tables[table_name]['mappings'][mapped_name]

    def get_table_schema(self, _connection, path, name):
        if not name in self.tables:
            return None

        return {'name': name,
                'path': path,
                'type': 'TABLE_SCHEMA',
                'schema': {'properties': self.tables[name]['columns']},
                'table_mappings': self.table_mappings,
                'mappings': self.tables[name]['mappings']}


def assert_tables_equal(target, expected_table_names):
    assert set(target.tables.keys()) == set(expected_table_names)


def get_records(target, table_name):
    return target.tables[table_name]['records']


def get_count(target, table_name):
    return len(get_records(target, table_name))


def test_loading__simple():
    target = FakeLastWriteWinsTarget()

    stream_to_target(CONFIG, target, input_stream=CatStream(100))

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    assert_columns_equal(None,
                         target,
                         'cats__adoption__immunizations',
                         {
                             '_sdc_level_0_id': {'type': ['integer']},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_source_key_id': {'type': ['integer']},
                             'date_administered': {'type': ['string', 'null'],
                                                   'format': 'date-time'},
                             'type': {'type': ['string', 'null']}
                         })

    assert get_count(target, 'cats') == 100


## TODO: Complex types defaulted
# def test_loading__default__complex_type():
#     main(CONFIG, input_stream=NestedStream(10))
#
#     with psycopg2.connect(**TEST_DB) as conn:
#         with conn.cursor() as cur:
#             cur.execute(get_count_sql('root'))
#             assert 10 == cur.fetchone()[0]
#
#             cur.execute(get_count_sql('root__array_scalar_defaulted'))
#             assert 100 == cur.fetchone()[0]


def test_loading__nested_tables():
    target = FakeLastWriteWinsTarget()

    stream_to_target(CONFIG, target, input_stream=NestedStream(10))

    assert_tables_equal(target, ['root',
                                 'root__array_scalar',
                                 'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar'[:50],
                                 'root__array_of_array',
                                 'root__array_of_array___sdc_value',
                                 'root__array_of_array___sdc_value___sdc_value'])

    assert get_count(target, 'root') \
           == 10

    assert get_count(target, 'root__array_scalar') \
           == 50

    assert get_count(target,'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar'[:50]) \
           == 50

    assert get_count(target, 'root__array_of_array') \
           == 20

    assert get_count(target, 'root__array_of_array___sdc_value') \
           == 80

    assert get_count(target, 'root__array_of_array___sdc_value___sdc_value') \
           == 200

    assert_columns_equal(None,
                         target,
                         'root',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'null': {'type': ['integer', 'null']},
                             'nested_null__null': {'type': ['integer', 'null']},
                             'object_of_object_0__object_of_object_1__object_of_': {'type': ['integer']},
                             'object_of_object_0__object_of_object_1__object___1': {'type': ['integer']},
                             'object_of_object_0__object_of_object_1__object___2': {'type': ['integer']}
                         })

    assert_columns_equal(None,
                         target,
                         'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar'[:50],
                         {
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_source_key_id': {'type': ['integer']},
                             '_sdc_level_0_id': {'type': ['integer']},
                             '_sdc_value': {'type': ['boolean']}
                         })

    assert_columns_equal(None,
                         target,
                         'root__array_of_array',
                         {
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_source_key_id': {'type': ['integer']},
                             '_sdc_level_0_id': {'type': ['integer']}
                         })

    assert_columns_equal(None,
                         target,
                         'root__array_of_array___sdc_value',
                         {
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_source_key_id': {'type': ['integer']},
                             '_sdc_level_0_id': {'type': ['integer']},
                             '_sdc_level_1_id': {'type': ['integer']}
                         })

    assert_columns_equal(None,
                         target,
                         'root__array_of_array___sdc_value___sdc_value',
                         {
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_source_key_id': {'type': ['integer']},
                             '_sdc_level_0_id': {'type': ['integer']},
                             '_sdc_level_1_id': {'type': ['integer']},
                             '_sdc_level_2_id': {'type': ['integer']},
                             '_sdc_value': {'type': ['integer']}
                         })


def test_loading__new_non_null_column():
    cat_count = 50
    target = FakeLastWriteWinsTarget()
    stream_to_target(CONFIG, target, input_stream=CatStream(cat_count))

    class NonNullStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            return record

    non_null_stream = NonNullStream(cat_count)
    non_null_stream.schema = deepcopy(non_null_stream.schema)
    non_null_stream.schema['schema']['properties']['paw_toe_count'] = {'type': 'integer',
                                                                       'default': 5}

    stream_to_target(CONFIG, target, input_stream=non_null_stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'paw_toe_count': {'type': ['integer', 'null']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the split columns before/after new non-null data
    assert 2 * cat_count == get_count(target, 'cats')
    assert cat_count == len([x for x in get_records(target, 'cats') if x['paw_toe_count'] is None])
    assert cat_count == len([x for x in get_records(target, 'cats') if x['paw_toe_count'] is not None])


def test_loading__column_type_change():
    cat_count = 20
    target = FakeLastWriteWinsTarget()
    stream_to_target(CONFIG, target, input_stream=CatStream(cat_count))

    target_keys = set([(x['id'], x['name']) for x in get_records(target, 'cats')])

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    class NameBooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record['name'] = False
            return record

    stream = NameBooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'boolean'}

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name__s': {'type': ['string', 'null']},
                             'name__b': {'type': ['boolean', 'null']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    class NameIntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record['name'] = 314
            return record

    stream = NameIntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'integer'}

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name__s': {'type': ['string', 'null']},
                             'name__b': {'type': ['boolean', 'null']},
                             'name__i': {'type': ['integer', 'null']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the split columns migrated data/persisted new data
    assert not [x for x in get_records(target, 'cats') if 'name' in x]

    assert 3 * cat_count == get_count(target, 'cats')

    assert cat_count == len([[x['name__s'], x['name__b'], x['name__i']]
                             for x in get_records(target, 'cats')
                             if x['name__s'] is not None])
    assert cat_count == len([[x['name__s'], x['name__b'], x['name__i']]
                             for x in get_records(target, 'cats')
                             if x['name__b'] is not None])
    assert cat_count == len([[x['name__s'], x['name__b'], x['name__i']]
                             for x in get_records(target, 'cats')
                             if x['name__i'] is not None])

    for record in get_records(target, 'cats'):
        if record['name__s'] is not None:
            assert (record['id'], record['name__s']) in target_keys

    more_than_one_split_column_has_values = []

    for record in get_records(target, 'cats'):
        split_values = set([record['name__s'], record['name__b'], record['name__i']])

        # Either the values which are in the split columns are exclusive, and hence something like:
        ## {v, None}
        ## or, None is not present, or there are more than 2 values present
        if len(split_values) > 2 or None not in split_values:
            more_than_one_split_column_has_values.append(split_values)

    assert 0 == len(more_than_one_split_column_has_values)


def test_loading__column_type_change__nullable():
    cat_count = 20
    target = FakeLastWriteWinsTarget()
    stream_to_target(CONFIG, target, input_stream=CatStream(cat_count))

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    class NameNullCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record['name'] = None
            return record

    stream = NameNullCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = json_schema.make_nullable(
        stream.schema['schema']['properties']['name'])

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string', 'null']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    class NameNonNullCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + 2 * cat_count
            return record

    stream_to_target(CONFIG, target, input_stream=NameNonNullCatStream(cat_count))

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string', 'null']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the column is has migrated data
    assert 3 * cat_count == get_count(target, 'cats')
    assert 2 * cat_count == len([x for x in get_records(target, 'cats') if x['name'] is not None])
    assert cat_count == len([x for x in get_records(target, 'cats') if x['name'] is None])


def test_loading__multi_types_columns():
    stream_count = 50
    target = FakeLastWriteWinsTarget()
    stream_to_target(CONFIG, target, input_stream=MultiTypeStream(stream_count))

    assert_columns_equal(None,
                         target,
                         'root',
                         {
                             '_sdc_primary_key': {'type': ['string']},
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'every_type__i': {'type': ['integer', 'null']},
                             'every_type__f': {'type': ['number', 'null']},
                             'every_type__b': {'type': ['boolean', 'null']},
                             'every_type__s': {'type': ['string', 'null'],
                                               'format': 'date-time'},
                             'every_type__i__1': {'type': ['integer', 'null']},
                             'every_type__f__1': {'type': ['number', 'null']},
                             'every_type__b__1': {'type': ['boolean', 'null']},
                             'number_which_only_comes_as_integer': {'type': ['number']}
                         })

    assert_columns_equal(None,
                         target,
                         'root__every_type',
                         {
                             '_sdc_source_key__sdc_primary_key': {'type': ['string']},
                             '_sdc_level_0_id': {'type': ['integer']},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_value': {'type': ['integer']},
                         })

    ## Assert that the column is has migrated data
    assert stream_count == len([x
                                for x in get_records(target, 'root')
                                if isinstance(x['number_which_only_comes_as_integer'], int)])


def test_loading__invalid__table_name__nested():
    cat_count = 20
    sub_table_name = 'immunizations'
    invalid_name = 'INValID!NON{conflicting'

    target = FakeLastWriteWinsTarget()

    class InvalidNameSubTableCatStream(CatStream):
        immunizations_count = 0

        def generate_record(self):
            record = CatStream.generate_record(self)
            if record.get('adoption', False):
                self.immunizations_count += len(record['adoption'][sub_table_name])
                record['adoption'][invalid_name] = record['adoption'][sub_table_name]
            return record

    stream = InvalidNameSubTableCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['adoption']['properties'][invalid_name] = \
        stream.schema['schema']['properties']['adoption']['properties'][sub_table_name]

    stream_to_target(CONFIG, target, input_stream=stream)

    immunizations_count = stream.immunizations_count
    invalid_name_count = stream.immunizations_count

    conflicting_name = sub_table_name.upper()

    class ConflictingNameSubTableCatStream(CatStream):
        immunizations_count = 0

        def generate_record(self):
            record = CatStream.generate_record(self)
            if record.get('adoption', False):
                self.immunizations_count += len(record['adoption'][sub_table_name])
                record['adoption'][conflicting_name] = record['adoption'][sub_table_name]
            record['id'] = record['id'] + cat_count
            return record

    stream = ConflictingNameSubTableCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['adoption']['properties'][conflicting_name] = \
        stream.schema['schema']['properties']['adoption']['properties'][sub_table_name]

    stream_to_target(CONFIG, target, input_stream=stream)

    immunizations_count += stream.immunizations_count
    conflicting_name_count = stream.immunizations_count

    assert_tables_equal(target, ['cats',
                                 ('cats__adoption__' + invalid_name.lower()),
                                 ('cats__adoption__' + sub_table_name),
                                 ('cats__adoption__' + sub_table_name + '__1')])

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    subtable_columns = {
        '_sdc_sequence': {'type': ['integer', 'null']},
        '_sdc_level_0_id': {'type': ['integer']},
        '_sdc_source_key_id': {'type': ['integer']},
        'date_administered': {'format': 'date-time', 'type': ['string', 'null']},
        'type': {'type': ['string', 'null']}
    }
    assert_columns_equal(None,
                         target,
                         ('cats__adoption__' + sub_table_name),
                         subtable_columns)
    assert_columns_equal(None,
                         target,
                         ('cats__adoption__' + invalid_name.lower()),
                         subtable_columns)
    assert_columns_equal(None,
                         target,
                         ('cats__adoption__' + sub_table_name + '__1'),
                         subtable_columns)

    assert 2 * cat_count == get_count(target, 'cats')

    assert immunizations_count == get_count(target, ('cats__adoption__' + sub_table_name))

    assert invalid_name_count == get_count(target, ('cats__adoption__' + invalid_name.lower()))

    assert conflicting_name_count == get_count(target, ('cats__adoption__' + sub_table_name + '__1'))


def test_loading__invalid_column_name():
    target = FakeLastWriteWinsTarget()

    non_lowercase_stream = CatStream(100)
    non_lowercase_stream.schema = deepcopy(non_lowercase_stream.schema)
    non_lowercase_stream.schema['schema']['properties']['INVALID_name'] = \
        non_lowercase_stream.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=non_lowercase_stream)

    duplicate_non_lowercase_stream_1 = CatStream(100)
    duplicate_non_lowercase_stream_1.schema = deepcopy(duplicate_non_lowercase_stream_1.schema)
    duplicate_non_lowercase_stream_1.schema['schema']['properties']['invalid_NAME'] = \
        duplicate_non_lowercase_stream_1.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=duplicate_non_lowercase_stream_1)

    duplicate_non_lowercase_stream_2 = CatStream(100)
    duplicate_non_lowercase_stream_2.schema = deepcopy(duplicate_non_lowercase_stream_2.schema)
    duplicate_non_lowercase_stream_2.schema['schema']['properties']['Invalid_NAME'] = \
        duplicate_non_lowercase_stream_2.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=duplicate_non_lowercase_stream_2)

    duplicate_non_lowercase_stream_3 = CatStream(100)
    duplicate_non_lowercase_stream_3.schema = deepcopy(duplicate_non_lowercase_stream_3.schema)
    duplicate_non_lowercase_stream_3.schema['schema']['properties']['invalid_NAmE'] = \
        duplicate_non_lowercase_stream_3.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=duplicate_non_lowercase_stream_3)

    name_too_long_stream = CatStream(100)
    name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
    name_too_long_stream.schema['schema']['properties']['x' * 1000] = \
        name_too_long_stream.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=name_too_long_stream)

    duplicate_name_too_long_stream = CatStream(100)
    duplicate_name_too_long_stream.schema = deepcopy(duplicate_name_too_long_stream.schema)
    duplicate_name_too_long_stream.schema['schema']['properties']['x' * 100] = \
        duplicate_name_too_long_stream.schema['schema']['properties']['age']

    stream_to_target(CONFIG, target, input_stream=duplicate_name_too_long_stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'invalid_name': {'type': ['integer', 'null']},
                             'invalid_name__1': {'type': ['integer', 'null']},
                             'invalid_name__2': {'type': ['integer', 'null']},
                             'invalid_name__3': {'type': ['integer', 'null']},
                             ('x' * 50): {'type': ['integer', 'null']},
                             ('x' * 47 + '__1'): {'type': ['integer', 'null']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })


def test_loading__invalid_column_name__duplicate_name_handling():
    target = FakeLastWriteWinsTarget()

    for i in range(101):
        name_too_long_stream = CatStream(100)
        name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
        name_too_long_stream.schema['schema']['properties']['x' * (100 + i)] = \
            name_too_long_stream.schema['schema']['properties']['age']

        stream_to_target(CONFIG, target, input_stream=name_too_long_stream)

    expected_columns = {
        '_sdc_batched_at': {'type': ['string', 'null'],
                            'format': 'date-time'},
        '_sdc_received_at': {'type': ['string', 'null'],
                             'format': 'date-time'},
        '_sdc_sequence': {'type': ['integer', 'null']},
        '_sdc_table_version': {'type': ['integer', 'null']},
        'adoption__adopted_on': {'type': ['string', 'null'],
                                 'format': 'date-time'},
        'adoption__was_foster': {'type': ['boolean', 'null']},
        'age': {'type': ['integer', 'null']},
        'id': {'type': ['integer']},
        'name': {'type': ['string']},
        'paw_size': {'type': ['integer']},
        'paw_colour': {'type': ['string']},
        ('x' * 50): {'type': ['integer', 'null']},
        ('x' * 45 + '__100'): {'type': ['integer', 'null']},
        'flea_check_complete': {'type': ['boolean']},
        'pattern': {'type': ['string', 'null']}
    }

    for i in range(1, 10):
        expected_columns['x' * 47 + '__' + str(i)] = {'type': ['integer', 'null']}
    for i in range(10, 100):
        expected_columns['x' * 46 + '__' + str(i)] = {'type': ['integer', 'null']}

    assert_columns_equal(None,
                         target, 'cats', expected_columns)


def test_loading__invalid_column_name__column_type_change():
    target = FakeLastWriteWinsTarget()

    invalid_column_name = 'INVALID!name'
    cat_count = 20
    stream = CatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = \
        stream.schema['schema']['properties']['paw_colour']

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'invalid!name': {'type': ['string']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the original data is present
    assert cat_count == len(get_records(target, 'cats'))
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name'] is not None])

    class BooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record[invalid_column_name] = False
            return record

    stream = BooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = {'type': 'boolean'}

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'invalid!name__s': {'type': ['string', 'null']},
                             'invalid!name__b': {'type': ['boolean', 'null']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the split columns migrated data/persisted new data
    assert 2 * cat_count == len(get_records(target, 'cats'))
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name__s'] is not None])
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name__b'] is not None])
    assert 0 == len([x
                     for x in get_records(target, 'cats')
                     if x['invalid!name__s'] is not None
                     and x['invalid!name__b'] is not None])

    class IntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record[invalid_column_name] = 314
            return record

    stream = IntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = {'type': 'integer'}

    stream_to_target(CONFIG, target, input_stream=stream)

    assert_columns_equal(None,
                         target,
                         'cats',
                         {
                             '_sdc_batched_at': {'type': ['string', 'null'],
                                                 'format': 'date-time'},
                             '_sdc_received_at': {'type': ['string', 'null'],
                                                  'format': 'date-time'},
                             '_sdc_sequence': {'type': ['integer', 'null']},
                             '_sdc_table_version': {'type': ['integer', 'null']},
                             'adoption__adopted_on': {'type': ['string', 'null'],
                                                      'format': 'date-time'},
                             'adoption__was_foster': {'type': ['boolean', 'null']},
                             'age': {'type': ['integer', 'null']},
                             'id': {'type': ['integer']},
                             'name': {'type': ['string']},
                             'paw_size': {'type': ['integer']},
                             'paw_colour': {'type': ['string']},
                             'invalid!name__s': {'type': ['string', 'null']},
                             'invalid!name__b': {'type': ['boolean', 'null']},
                             'invalid!name__i': {'type': ['integer', 'null']},
                             'flea_check_complete': {'type': ['boolean']},
                             'pattern': {'type': ['string', 'null']}
                         })

    ## Assert that the split columns migrated data/persisted new data
    assert 3 * cat_count == len(get_records(target, 'cats'))
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name__s'] is not None])
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name__b'] is not None])
    assert cat_count == len([x for x in get_records(target, 'cats') if x['invalid!name__i'] is not None])
    assert 0 == len([x
                     for x in get_records(target, 'cats')
                     if x['invalid!name__s'] is not None
                     and x['invalid!name__b'] is not None
                     and x['invalid!name__i'] is not None])
