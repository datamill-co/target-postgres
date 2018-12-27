from copy import deepcopy
from datetime import datetime

import psycopg2
from psycopg2 import sql
import psycopg2.extras
import pytest
import singer

from tests.fixtures import CatStream, CONFIG, db_cleanup, MultiTypeStream, NestedStream, TEST_DB, assert_columns_equal
from target_postgres import json_schema
from target_postgres import postgres
from target_postgres import singer_stream
from target_postgres import main
from target_postgres.sql_main import TargetError


## TODO: create and test more fake streams
## TODO: test invalid data against JSON Schema
## TODO: test compound pk

def get_count_sql(table_name):
    return 'SELECT count(*) FROM "public"."{}"'.format(table_name)


def get_pk_key(pks, obj, subrecord=False):
    pk_parts = []
    for pk in pks:
        pk_parts.append(str(obj[pk]))
    if subrecord:
        for key, value in obj.items():
            if key[:11] == '_sdc_level_':
                pk_parts.append(str(value))
    return ':'.join(pk_parts)


def flatten_record(old_obj, subtables, subpks, new_obj=None, current_path=None, level=0):
    if not new_obj:
        new_obj = {}

    for prop, value in old_obj.items():
        if current_path:
            next_path = current_path + '__' + prop
        else:
            next_path = prop

        if isinstance(value, dict):
            flatten_record(value, subtables, subpks, new_obj=new_obj, current_path=next_path, level=level)
        elif isinstance(value, list):
            if next_path not in subtables:
                subtables[next_path] = []
            row_index = 0
            for item in value:
                new_subobj = {}
                for key, value in subpks.items():
                    new_subobj[key] = value
                new_subpks = subpks.copy()
                new_subobj[singer_stream.SINGER_LEVEL.format(level)] = row_index
                new_subpks[singer_stream.SINGER_LEVEL.format(level)] = row_index
                subtables[next_path].append(flatten_record(item,
                                                           subtables,
                                                           new_subpks,
                                                           new_obj=new_subobj,
                                                           level=level + 1))
                row_index += 1
        else:
            new_obj[next_path] = value
    return new_obj


def assert_record(a, b, subtables, subpks):
    a_flat = flatten_record(a, subtables, subpks)
    for prop, value in a_flat.items():
        if value is None:
            if prop in b:
                assert b[prop] == None
        elif isinstance(b[prop], datetime):
            assert value == b[prop].isoformat()[:19]
        else:
            assert value == b[prop]


def assert_records(conn, records, table_name, pks, match_pks=False):
    if not isinstance(pks, list):
        pks = [pks]

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("set timezone='UTC';")

        cur.execute('SELECT * FROM {}'.format(table_name))
        persisted_records_raw = cur.fetchall()

        persisted_records = {}
        for persisted_record in persisted_records_raw:
            pk = get_pk_key(pks, persisted_record)
            persisted_records[pk] = persisted_record

        subtables = {}
        records_pks = []
        for record in records:
            pk = get_pk_key(pks, record)
            records_pks.append(pk)
            persisted_record = persisted_records[pk]
            subpks = {}
            for pk in pks:
                subpks[singer_stream.SINGER_SOURCE_PK_PREFIX + pk] = persisted_record[pk]
            assert_record(record, persisted_record, subtables, subpks)

        if match_pks:
            assert sorted(list(persisted_records.keys())) == sorted(records_pks)

        sub_pks = list(map(lambda pk: singer_stream.SINGER_SOURCE_PK_PREFIX + pk, pks))
        for subtable_name, items in subtables.items():
            cur.execute('SELECT * FROM {}'.format(
                table_name + '__' + subtable_name))
            persisted_records_raw = cur.fetchall()

            persisted_records = {}
            for persisted_record in persisted_records_raw:
                pk = get_pk_key(sub_pks, persisted_record, subrecord=True)
                persisted_records[pk] = persisted_record

            subtables = {}
            records_pks = []
            for record in items:
                pk = get_pk_key(sub_pks, record, subrecord=True)
                records_pks.append(pk)
                persisted_record = persisted_records[pk]
                assert_record(record, persisted_record, subtables, subpks)
            assert len(subtables.values()) == 0

            if match_pks:
                assert sorted(list(persisted_records.keys())) == sorted(records_pks)


def test_loading__invalid__configuration__schema(db_cleanup):
    stream = CatStream(1)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['type'] = 'invalid type for a JSON Schema'

    with pytest.raises(TargetError, match=r'.*invalid JSON Schema instance.*'):
        main(CONFIG, input_stream=stream)


def test_loading__invalid__default_null_value__non_nullable_column(db_cleanup):
    class NullDefaultCatStream(CatStream):

        def generate_record(self):
            record = CatStream.generate_record(self)
            record['name'] = postgres.RESERVED_NULL_DEFAULT
            return record

    with pytest.raises(postgres.PostgresError, match=r'.*IntegrityError.*'):
        main(CONFIG, input_stream=NullDefaultCatStream(20))


def test_loading__simple(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
                                 'cats__adoption__immunizations',
                                 {
                                     '_sdc_level_0_id': {'type': ['integer']},
                                     '_sdc_sequence': {'type': ['integer', 'null']},
                                     '_sdc_source_key_id': {'type': ['integer']},
                                     'date_administered': {'type': ['string', 'null'],
                                                           'format': 'date-time'},
                                     'type': {'type': ['string', 'null']}
                                 })

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, 'cats', 'id')


def test_loading__new_non_null_column(db_cleanup):
    cat_count = 50
    main(CONFIG, input_stream=CatStream(cat_count))

    class NonNullStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            return record

    non_null_stream = NonNullStream(cat_count)
    non_null_stream.schema = deepcopy(non_null_stream.schema)
    non_null_stream.schema['schema']['properties']['paw_toe_count'] = {'type': 'integer',
                                                                       'default': 5}

    main(CONFIG, input_stream=non_null_stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {}, {} FROM {}').format(
                sql.Identifier('id'),
                sql.Identifier('paw_toe_count'),
                sql.Identifier('cats')
            ))

            persisted_records = cur.fetchall()

            ## Assert that the split columns before/after new non-null data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[1] is None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])


def test_loading__column_type_change(db_cleanup):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

    class NameBooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record['name'] = False
            return record

    stream = NameBooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'boolean'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {}, {} FROM {}').format(
                sql.Identifier('name__s'),
                sql.Identifier('name__b'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is not None and x[1] is not None])

    class NameIntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record['name'] = 314
            return record

    stream = NameIntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'integer'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {}, {}, {} FROM {}').format(
                sql.Identifier('name__s'),
                sql.Identifier('name__b'),
                sql.Identifier('name__i'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 3 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert cat_count == len([x for x in persisted_records if x[2] is not None])
            assert 0 == len(
                [x for x in persisted_records if x[0] is not None and x[1] is not None and x[2] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is None and x[1] is None and x[2] is None])


def test_loading__column_type_change__nullable(db_cleanup):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

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

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the column is has migrated data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[0] is None])

    class NameNonNullCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + 2 * cat_count
            return record

    main(CONFIG, input_stream=NameNonNullCatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the column is has migrated data
            assert 3 * cat_count == len(persisted_records)
            assert 2 * cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[0] is None])


def test_loading__multi_types_columns(db_cleanup):
    stream_count = 50
    main(CONFIG, input_stream=MultiTypeStream(stream_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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

            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
                                 'root__every_type',
                                 {
                                     '_sdc_source_key__sdc_primary_key': {'type': ['string']},
                                     '_sdc_level_0_id': {'type': ['integer']},
                                     '_sdc_sequence': {'type': ['integer', 'null']},
                                     '_sdc_value': {'type': ['integer']},
                                 })

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('number_which_only_comes_as_integer'),
                sql.Identifier('root')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the column is has migrated data
            assert stream_count == len(persisted_records)
            assert stream_count == len([x for x in persisted_records if isinstance(x[0], float)])


def test_loading__invalid__table_name__stream(db_cleanup):
    def invalid_stream_named(stream_name, postgres_error_regex):
        stream = CatStream(100)
        stream.stream = stream_name
        stream.schema = deepcopy(stream.schema)
        stream.schema['stream'] = stream_name

        with pytest.raises(postgres.PostgresError, match=postgres_error_regex):
            main(CONFIG, input_stream=stream)

    invalid_stream_named('', r'.*non empty.*')
    invalid_stream_named('x' * 1000, r'Length.*')
    invalid_stream_named('INVALID_name', r'.*must start.*')
    invalid_stream_named('a!!!invalid_name', r'.*only contain.*')

    borderline_length_stream_name = 'x' * 61
    stream = CatStream(100, version=1)
    stream.stream = borderline_length_stream_name
    stream.schema = deepcopy(stream.schema)
    stream.schema['stream'] = borderline_length_stream_name
    main(CONFIG, input_stream=stream)

    stream = CatStream(100, version=10)
    stream.stream = borderline_length_stream_name
    stream.schema = deepcopy(stream.schema)
    stream.schema['stream'] = borderline_length_stream_name

    with pytest.raises(postgres.PostgresError, match=r'Length.*'):
        main(CONFIG, input_stream=stream)


def test_loading__invalid__table_name__nested(db_cleanup):
    cat_count = 20
    sub_table_name = 'immunizations'
    invalid_name = 'INValID!NON{conflicting'

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

    main(CONFIG, input_stream=stream)

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

    main(CONFIG, input_stream=stream)

    immunizations_count += stream.immunizations_count
    conflicting_name_count = stream.immunizations_count

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            target = postgres.PostgresTarget(conn)
            assert_columns_equal(cur,
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
            assert_columns_equal(cur,
                                 target,
                                 'cats__adoption__immunizations',
                                 subtable_columns)
            assert_columns_equal(cur,
                                 target,
                                 'cats__adoption__invalid_non_conflicting',
                                 subtable_columns)
            assert_columns_equal(cur,
                                 target,
                                 'cats__adoption__immunizations__1',
                                 subtable_columns)

            cur.execute(get_count_sql('cats'))
            assert 2 * cat_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert immunizations_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__invalid_non_conflicting'))
            assert invalid_name_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__immunizations__1'))
            assert conflicting_name_count == cur.fetchone()[0]


def test_loading__invalid_column_name__column_type_change(db_cleanup):
    invalid_column_name = 'INVALID!name'
    cat_count = 20
    stream = CatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = \
        stream.schema['schema']['properties']['paw_colour']

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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
                                     'invalid_name': {'type': ['string']},
                                     'flea_check_complete': {'type': ['boolean']},
                                     'pattern': {'type': ['string', 'null']}
                                 })

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('invalid_name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

    class BooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record[invalid_column_name] = False
            return record

    stream = BooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = {'type': 'boolean'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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
                                     'invalid_name__s': {'type': ['string', 'null']},
                                     'invalid_name__b': {'type': ['boolean', 'null']},
                                     'flea_check_complete': {'type': ['boolean']},
                                     'pattern': {'type': ['string', 'null']}
                                 })

            cur.execute(sql.SQL('SELECT {}, {} FROM {}').format(
                sql.Identifier('invalid_name__s'),
                sql.Identifier('invalid_name__b'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is not None and x[1] is not None])

    class IntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record[invalid_column_name] = 314
            return record

    stream = IntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties'][invalid_column_name] = {'type': 'integer'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 postgres.PostgresTarget(conn),
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
                                     'invalid_name__s': {'type': ['string', 'null']},
                                     'invalid_name__b': {'type': ['boolean', 'null']},
                                     'invalid_name__i': {'type': ['integer', 'null']},
                                     'flea_check_complete': {'type': ['boolean']},
                                     'pattern': {'type': ['string', 'null']}
                                 })

            cur.execute(sql.SQL('SELECT {}, {}, {} FROM {}').format(
                sql.Identifier('invalid_name__s'),
                sql.Identifier('invalid_name__b'),
                sql.Identifier('invalid_name__i'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 3 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert cat_count == len([x for x in persisted_records if x[2] is not None])
            assert 0 == len(
                [x for x in persisted_records if x[0] is not None and x[1] is not None and x[2] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is None and x[1] is None and x[2] is None])


def test_loading__column_type_change__pks__same_resulting_type(db_cleanup):
    stream = CatStream(20)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['id'] = {'type': ['integer', 'null']}

    main(CONFIG, input_stream=stream)

    stream = CatStream(20)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['id'] = {'type': ['null', 'integer']}

    main(CONFIG, input_stream=stream)


def test_loading__invalid__column_type_change__pks(db_cleanup):
    main(CONFIG, input_stream=CatStream(20))

    class StringIdCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = str(record['id'])
            return record

    stream = StringIdCatStream(20)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['id'] = {'type': 'string'}

    with pytest.raises(postgres.PostgresError, match=r'.*key_properties. type change detected'):
        main(CONFIG, input_stream=stream)


def test_loading__invalid__column_type_change__pks__nullable(db_cleanup):
    main(CONFIG, input_stream=CatStream(20))

    stream = CatStream(20)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['id'] = json_schema.make_nullable(stream.schema['schema']['properties']['id'])

    with pytest.raises(postgres.PostgresError, match=r'.*key_properties. type change detected'):
        main(CONFIG, input_stream=stream)


def test_upsert(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(200)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 200
        assert_records(conn, stream.records, 'cats', 'id')


def test_upsert__invalid__primary_key_change(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    stream = CatStream(100)
    schema = deepcopy(stream.schema)
    schema['key_properties'].append('name')
    stream.schema = schema

    with pytest.raises(postgres.PostgresError, match=r'.*key_properties.*'):
        main(CONFIG, input_stream=stream)


def test_nested_delete_on_parent(db_cleanup):
    stream = CatStream(100, nested_count=3)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            high_nested = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100, nested_count=2)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            low_nested = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id')

    assert low_nested < high_nested


def test_full_table_replication(db_cleanup):
    stream = CatStream(110, version=0, nested_count=3)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            version_0_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            version_0_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_0_count == 110
    assert version_0_sub_count == 330

    stream = CatStream(100, version=1, nested_count=3)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            version_1_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            version_1_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_1_count == 100
    assert version_1_sub_count == 300

    stream = CatStream(120, version=2, nested_count=2)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            version_2_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            version_2_sub_count = cur.fetchone()[0]
        assert_records(conn, stream.records, 'cats', 'id', match_pks=True)

    assert version_2_count == 120
    assert version_2_sub_count == 240

    ## Test that an outdated version cannot overwrite
    stream = CatStream(314, version=1, nested_count=2)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            older_version_count = cur.fetchone()[0]

    assert older_version_count == version_2_count


def test_deduplication_newer_rows(db_cleanup):
    stream = CatStream(100, nested_count=3, duplicates=2)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]

            cur.execute('SELECT _sdc_sequence FROM cats WHERE id in ({})'.format(
                ','.join(map(str, stream.duplicate_pks_used))))
            dup_cat_records = cur.fetchall()

    assert stream.record_message_count == 102
    assert table_count == 100
    assert nested_table_count == 300

    for record in dup_cat_records:
        assert record[0] == stream.sequence + 200


def test_deduplication_older_rows(db_cleanup):
    stream = CatStream(100, nested_count=2, duplicates=2, duplicate_sequence_delta=-100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]

            cur.execute('SELECT _sdc_sequence FROM cats WHERE id in ({})'.format(
                ','.join(map(str, stream.duplicate_pks_used))))
            dup_cat_records = cur.fetchall()

    assert stream.record_message_count == 102
    assert table_count == 100
    assert nested_table_count == 200

    for record in dup_cat_records:
        assert record[0] == stream.sequence


def test_deduplication_existing_new_rows(db_cleanup):
    stream = CatStream(100, nested_count=2)
    main(CONFIG, input_stream=stream)

    original_sequence = stream.sequence

    stream = CatStream(100,
                       nested_count=2,
                       sequence=original_sequence - 20)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            table_count = cur.fetchone()[0]
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            nested_table_count = cur.fetchone()[0]

            cur.execute('SELECT DISTINCT _sdc_sequence FROM cats')
            sequences = cur.fetchall()

    assert table_count == 100
    assert nested_table_count == 200

    assert len(sequences) == 1
    assert sequences[0][0] == original_sequence


def test_multiple_batches_upsert(db_cleanup):
    config = CONFIG.copy()
    config['max_batch_rows'] = 20
    config['batch_detection_threshold'] = 5

    stream = CatStream(100, nested_count=2)
    main(config, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert cur.fetchone()[0] == 200
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100, nested_count=3)
    main(config, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert cur.fetchone()[0] == 300
        assert_records(conn, stream.records, 'cats', 'id')


def test_multiple_batches_by_memory_upsert(db_cleanup):
    config = CONFIG.copy()
    config['max_batch_size'] = 1024
    config['batch_detection_threshold'] = 5

    stream = CatStream(100, nested_count=2)
    main(config, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert cur.fetchone()[0] == 200
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100, nested_count=3)
    main(config, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert cur.fetchone()[0] == 300
        assert_records(conn, stream.records, 'cats', 'id')
