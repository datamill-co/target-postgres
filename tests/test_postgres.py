from copy import deepcopy
from datetime import datetime
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
import psycopg2.extras
import pytest

from fixtures import CatStream, CONFIG, db_cleanup, InvalidCatStream, TEST_DB
from target_postgres import json_schema
from target_postgres import postgres
from target_postgres import singer_stream
from target_postgres import TargetError, main

## TODO: create and test more fake streams
## TODO: test invalid data against JSON Schema
## TODO: test compound pk

def get_columns_sql(table_name):
    return "SELECT column_name, data_type, is_nullable FROM information_schema.columns " + \
           "WHERE table_schema = 'public' and table_name = '{}';".format(
        table_name)

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


def test_loading__invalid__configuration__schema():
    stream = CatStream(1)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['type'] = 'invalid type for a JSON Schema'

    with pytest.raises(TargetError, match=r'.*invalid JSON Schema instance.*'):
        main(CONFIG, input_stream=stream)


def test_loading__invalid__records():
    with pytest.raises(singer_stream.SingerStreamError, match=r'.*'):
        main(CONFIG,
             input_stream=InvalidCatStream(1))


def test_loading__invalid__records__disable():
    config = deepcopy(CONFIG)
    config['invalid_records_detect'] = False

    main(config, input_stream=InvalidCatStream(100))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_columns_sql('cats'))
            # No columns for a non existent table
            ## Since all `cat`s records were invalid, we could not persist them, hence, no table created
            assert not cur.fetchall()


def test_loading__invalid__records__threshold():
    config = deepcopy(CONFIG)
    config['invalid_records_threshold'] = 10

    with pytest.raises(singer_stream.SingerStreamError, match=r'.*.10*'):
        main(config, input_stream=InvalidCatStream(20))


def test_loading__invalid__default_null_value__non_nullable_column():

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
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'NO'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

            cur.execute(get_columns_sql('cats__adoption__immunizations'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_level_0_id', 'bigint', 'NO'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_source_key_id', 'bigint', 'NO'),
                ('date_administered', 'timestamp with time zone', 'YES'),
                ('type', 'text', 'YES')
            }

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
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'NO'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('paw_toe_count', 'bigint', 'YES'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

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
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'NO'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

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
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name__s', 'text', 'YES'),
                ('name__b', 'boolean', 'YES'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

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


def test_loading__column_type_change__nullable(db_cleanup):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'NO'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

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
    stream.schema['schema']['properties']['name'] = json_schema.make_nullable(stream.schema['schema']['properties']['name'])

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'YES'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

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
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert set(columns) == {
                ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                ('_sdc_sequence', 'bigint', 'YES'),
                ('_sdc_table_version', 'bigint', 'YES'),
                ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                ('adoption__was_foster', 'boolean', 'YES'),
                ('age', 'bigint', 'YES'),
                ('id', 'bigint', 'NO'),
                ('name', 'text', 'YES'),
                ('paw_size', 'bigint', 'NO'),
                ('paw_colour', 'text', 'NO'),
                ('flea_check_complete', 'boolean', 'NO'),
                ('pattern', 'text', 'YES')
            }

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the column is has migrated data
            assert 3 * cat_count == len(persisted_records)
            assert 2 * cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[0] is None])


def test_loading__invalid__table_name(db_cleanup):
    non_alphanumeric_stream = CatStream(100)
    non_alphanumeric_stream.stream = '!!!invalid_name'
    non_alphanumeric_stream.schema = deepcopy(non_alphanumeric_stream.schema)
    non_alphanumeric_stream.schema['stream'] = '!!!invalid_name'

    with pytest.raises(postgres.PostgresError):
        main(CONFIG, input_stream=non_alphanumeric_stream)

    non_lowercase_stream = CatStream(100)
    non_lowercase_stream.stream = 'INVALID_name'
    non_lowercase_stream.schema = deepcopy(non_lowercase_stream.schema)
    non_lowercase_stream.schema['stream'] = 'INVALID_name'

    with pytest.raises(postgres.PostgresError):
        main(CONFIG, input_stream=non_lowercase_stream)

    name_too_long_stream = CatStream(100)
    name_too_long_stream.stream = 'x' * 1000
    name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
    name_too_long_stream.schema['stream'] = 'x' * 1000

    with pytest.raises(postgres.PostgresError):
        main(CONFIG, input_stream=name_too_long_stream)

def test_loading__invalid__column_name(db_cleanup):
    non_alphanumeric_stream = CatStream(100)
    non_alphanumeric_stream.schema = deepcopy(non_alphanumeric_stream.schema)
    non_alphanumeric_stream.schema['schema']['properties']['!!!invalid_name'] = non_alphanumeric_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=non_alphanumeric_stream)

    non_lowercase_stream = CatStream(100)
    non_lowercase_stream.schema = deepcopy(non_lowercase_stream.schema)
    non_lowercase_stream.schema['schema']['properties']['INVALID_name'] = non_lowercase_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=non_lowercase_stream)

def test_loading__invalid__column_name__non_canonicalizable(db_cleanup):
    name_too_long_stream = CatStream(100)
    name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
    name_too_long_stream.schema['schema']['properties']['x' * 1000] = name_too_long_stream.schema['schema']['properties']['age']

    with pytest.raises(postgres.PostgresError):
        main(CONFIG, input_stream=name_too_long_stream)

    non_lowercase_stream = CatStream(100)
    non_lowercase_stream.schema = deepcopy(non_lowercase_stream.schema)
    non_lowercase_stream.schema['schema']['properties']['INVALID_name'] = non_lowercase_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=non_lowercase_stream)

    duplicate_canonicalization_stream = CatStream(100)
    duplicate_canonicalization_stream.schema = deepcopy(duplicate_canonicalization_stream.schema)
    duplicate_canonicalization_stream.schema['schema']['properties']['invalid!NAME'] = duplicate_canonicalization_stream.schema['schema']['properties']['age']

    with pytest.raises(postgres.PostgresError):
        main(CONFIG, input_stream=duplicate_canonicalization_stream)


def test_loading__invalid__column_type_change__pks():
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


def test_loading__invalid__column_type_change__pks__nullable():
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

def mocked_mock_write_batch(stream_buffer):
    records = stream_buffer.flush_buffer()

def test_multiple_batches_by_rows(db_cleanup):
    with patch.object(postgres.PostgresTarget,
                      'write_batch',
                      side_effect=mocked_mock_write_batch) as mock_write_batch:
        config = CONFIG.copy()
        config['max_batch_rows'] = 20
        config['batch_detection_threshold'] = 5

        stream = CatStream(100)
        main(config, input_stream=stream)

        assert mock_write_batch.call_count == 6

def test_multiple_batches_by_memory(db_cleanup):
    with patch.object(postgres.PostgresTarget,
                      'write_batch',
                      side_effect=mocked_mock_write_batch) as mock_write_batch:
        config = CONFIG.copy()
        config['max_batch_size'] = 1024
        config['batch_detection_threshold'] = 5

        stream = CatStream(100)
        main(config, input_stream=stream)

        assert mock_write_batch.call_count == 21

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
