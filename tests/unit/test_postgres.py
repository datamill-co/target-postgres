from copy import deepcopy
from datetime import datetime
import json

import psycopg2
from psycopg2 import sql
import psycopg2.extras
import pytest

from utils.fixtures import CatStream, CONFIG, db_cleanup, MultiTypeStream, NestedStream, TEST_DB, TypeChangeStream, DogStream
from target_postgres import json_schema, main, postgres, singer, singer_stream
from target_postgres.target_tools import TargetError


## TODO: create and test more fake streams
## TODO: test invalid data against JSON Schema
## TODO: test compound pk

def assert_columns_equal(cursor, table_name, expected_column_tuples):
    cursor.execute('''
        SELECT column_name, data_type, is_nullable FROM information_schema.columns
        WHERE table_schema = 'public' and table_name = '{}';
    '''.format(
        table_name))
    columns = cursor.fetchall()

    assert (not columns and not expected_column_tuples) \
           or set(columns) == expected_column_tuples


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
                new_subobj[singer_stream.singer.LEVEL_FMT.format(level)] = row_index
                new_subpks[singer_stream.singer.LEVEL_FMT.format(level)] = row_index
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
                subpks[singer_stream.singer.SOURCE_PK_PREFIX + pk] = persisted_record[pk]
            assert_record(record, persisted_record, subtables, subpks)

        if match_pks:
            assert sorted(list(persisted_records.keys())) == sorted(records_pks)

        sub_pks = list(map(lambda pk: singer_stream.singer.SOURCE_PK_PREFIX + pk, pks))
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


def assert_column_indexed(conn, table_name, column_name):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT t.relname AS table_name, i.relname AS index_name, a.attname AS column_name
            FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
            WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r'
            AND t.relname = '{table_name}' AND a.attname = '{column_name}'
        '''.format(
            table_name=table_name,
            column_name=column_name))

        assert len(cur.fetchall()) > 0

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

    with pytest.raises(postgres.PostgresError, match=r'.*NotNullViolation.*'):
        main(CONFIG, input_stream=NullDefaultCatStream(20))


def test_loading__schema_version_0_gets_migrated_to_2(db_cleanup):
    main(CONFIG, input_stream=CatStream(100))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            target = postgres.PostgresTarget(conn)
            metadata = target._get_table_metadata(cur, 'cats')
            metadata.pop('schema_version')
            metadata.pop('path')
            metadata['table_mappings'] = [{'from': ['cats', 'adoption', 'immunizations'],
                                           'to': 'cats__adoption__immunizations'}]
            target._set_table_metadata(cur, 'cats', metadata)

            metadata = target._get_table_metadata(cur, 'cats__adoption__immunizations')
            metadata.pop('schema_version')
            metadata.pop('path')
            target._set_table_metadata(cur, 'cats__adoption__immunizations', metadata)

    main(CONFIG, input_stream=CatStream(100))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            target = postgres.PostgresTarget(conn)
            metadata = target._get_table_metadata(cur, 'cats')

            assert metadata['schema_version'] == 2
            assert metadata['path']
            assert not metadata.get('table_mappings')

            metadata = target._get_table_metadata(cur, 'cats__adoption__immunizations')

            assert metadata['schema_version'] == 2
            assert metadata['path']
            assert not metadata.get('table_mappings')


def test_loading__simple(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, 'cats', 'id')
        assert_column_indexed(conn, 'cats', '_sdc_sequence')
        assert_column_indexed(conn, 'cats', 'id')
        assert_column_indexed(conn, 'cats__adoption__immunizations', '_sdc_sequence')
        assert_column_indexed(conn, 'cats__adoption__immunizations', '_sdc_level_0_id')


def test_loading__simple__allOf(db_cleanup):
    stream = CatStream(100)
    stream.schema = deepcopy(stream.schema)

    name = stream.schema['schema']['properties']['name']
    stream.schema['schema']['properties']['name'] = {
        'allOf': [
            name,
            {'maxLength': 100000000}
            ]
    }
    adoption = stream.schema['schema']['properties']['adoption']['properties']
    stream.schema['schema']['properties']['adoption'] = {
        'allOf': [
            {
                'type': ['object', 'null'],
                'properties': {
                    'adopted_on': adoption['adopted_on'],
                    'immunizations': adoption['immunizations'],
                }
            },
            {
                'properties': {
                    'was_foster': adoption['was_foster']
                }
            }
        ]
    }
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, 'cats', 'id')


def test_loading__simple__anyOf(db_cleanup):
    stream = CatStream(100)
    stream.schema = deepcopy(stream.schema)

    adoption_props = stream.schema['schema']['properties']['adoption']['properties']
    adoption_props['adopted_on'] = {
        "anyOf": [
            {
                "type": "string",
                "format": "date-time"
            },
            {"type": ["string", "null"]}]}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on__t', 'timestamp with time zone', 'YES'),
                                     ('adoption__adopted_on__s', 'text', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {}, {}, {} FROM {}').format(
                sql.Identifier('adoption__adopted_on__t'),
                sql.Identifier('adoption__adopted_on__s'),
                sql.Identifier('adoption__was_foster'),
                sql.Identifier('cats')
            ))

            persisted_records = cur.fetchall()

            ## Assert that the split columns correctly persisted all datetime data
            assert 100 == len(persisted_records)
            assert 100 == len([x for x in persisted_records if x[1] is None])
            assert len([x for x in persisted_records if x[2] is not None]) \
                == len([x for x in persisted_records if x[0] is not None])


def test_loading__empty(db_cleanup):
    stream = CatStream(0)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('''
            SELECT EXISTS(
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = {}
                AND table_name = {}
            );
            ''').format(
                    sql.Literal('public'),
                    sql.Literal('cats')))

            assert not cur.fetchone()[0]


def test_loading__empty__enabled_config(db_cleanup):
    config = CONFIG.copy()
    config['persist_empty_tables'] = True

    stream = CatStream(0)
    main(config, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 0


def test_loading__empty__enabled_config_with_messages_for_only_one_stream(db_cleanup):
    config = CONFIG.copy()
    config['max_batch_rows'] = 20
    config['batch_detection_threshold'] = 1
    config['persist_empty_tables'] = True
    cat_rows = list(CatStream(100))
    dog_rows = list(DogStream(0))

    # Simulate one stream that yields a lot of records with another that yields no records, and ensure that only the first
    # needs to be flushed before any state messages are emitted
    def test_stream():
        yield cat_rows[0]
        yield dog_rows[0]
        for row in cat_rows[slice(1, 5)]:
            yield row
        yield json.dumps({'type': 'STATE', 'value': {'test': 'state-1'}})

        for row in cat_rows[slice(6, 25)]:
            yield row
        yield json.dumps({'type': 'STATE', 'value': {'test': 'state-2'}})

    main(config, input_stream=test_stream())

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'dogs',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 23

            cur.execute(get_count_sql('dogs'))
            assert cur.fetchone()[0] == 0


## TODO: Complex types defaulted
# def test_loading__default__complex_type(db_cleanup):
#     main(CONFIG, input_stream=NestedStream(10))
#
#     with psycopg2.connect(**TEST_DB) as conn:
#         with conn.cursor() as cur:
#             cur.execute(get_count_sql('root'))
#             assert 10 == cur.fetchone()[0]
#
#             cur.execute(get_count_sql('root__array_scalar_defaulted'))
#             assert 100 == cur.fetchone()[0]


def test_loading__nested_tables(db_cleanup):
    main(CONFIG, input_stream=NestedStream(10))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('root'))
            assert 10 == cur.fetchone()[0]

            cur.execute(get_count_sql('root__array_scalar'))
            assert 50 == cur.fetchone()[0]

            cur.execute(
                get_count_sql('root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar'[:63]))
            assert 50 == cur.fetchone()[0]

            cur.execute(get_count_sql('root__array_of_array'))
            assert 20 == cur.fetchone()[0]

            cur.execute(get_count_sql('root__array_of_array___sdc_value'))
            assert 80 == cur.fetchone()[0]

            cur.execute(get_count_sql('root__array_of_array___sdc_value___sdc_value'))
            assert 200 == cur.fetchone()[0]

            assert_columns_equal(cur,
                                 'root',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('null', 'bigint', 'YES'),
                                     ('nested_null__null', 'bigint', 'YES'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__a', 'bigint', 'NO'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__b', 'bigint', 'NO'),
                                     ('object_of_object_0__object_of_object_1__object_of_object_2__c', 'bigint', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__object_of_object_0__object_of_object_1__object_of_object_2__array_scalar'[:63],
                                 {
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_value', 'boolean', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array',
                                 {
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('_sdc_level_0_id', 'bigint', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array___sdc_value',
                                 {
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_level_1_id', 'bigint', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__array_of_array___sdc_value___sdc_value',
                                 {
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_level_1_id', 'bigint', 'NO'),
                                     ('_sdc_level_2_id', 'bigint', 'NO'),
                                     ('_sdc_value', 'bigint', 'NO')
                                 })


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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('paw_toe_count', 'bigint', 'YES'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

    class NameDateTimeCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            record['name'] = '2001-01-01 01:01:01.0001+01:01'
            return record

    stream = NameDateTimeCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'string',
                                                     'format': 'date-time'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name__s', 'text', 'YES'),
                                     ('name__t', 'timestamp with time zone', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {}, {} FROM {}').format(
                sql.Identifier('name__s'),
                sql.Identifier('name__t'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 2 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert 0 == len([x for x in persisted_records if x[0] is not None and x[1] is not None])

    class NameBooleanCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (2 * cat_count)
            record['name'] = False
            return record

    stream = NameBooleanCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'boolean'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name__s', 'text', 'YES'),
                                     ('name__t', 'timestamp with time zone', 'YES'),
                                     ('name__b', 'boolean', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {}, {}, {} FROM {}').format(
                sql.Identifier('name__s'),
                sql.Identifier('name__t'),
                sql.Identifier('name__b'),
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

    class NameIntegerCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + (3 * cat_count)
            record['name'] = 314
            return record

    stream = NameIntegerCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    stream.schema['schema']['properties']['name'] = {'type': 'integer'}

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name__s', 'text', 'YES'),
                                     ('name__t', 'timestamp with time zone', 'YES'),
                                     ('name__b', 'boolean', 'YES'),
                                     ('name__i', 'bigint', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {}, {}, {}, {} FROM {}').format(
                sql.Identifier('name__s'),
                sql.Identifier('name__t'),
                sql.Identifier('name__b'),
                sql.Identifier('name__i'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert 4 * cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])
            assert cat_count == len([x for x in persisted_records if x[1] is not None])
            assert cat_count == len([x for x in persisted_records if x[2] is not None])
            assert cat_count == len([x for x in persisted_records if x[3] is not None])
            assert 0 == len(
                [x for x in persisted_records if
                 x[0] is not None and x[1] is not None and x[2] is not None and x[3] is not None])
            assert 0 == len(
                [x for x in persisted_records if x[0] is None and x[1] is None and x[2] is None and x[3] is None])


def test_loading__column_type_change__generative(db_cleanup):
    insert_count = 20
    repeats_to_perform = 5

    literal_types_remaining = set(['integer', 'number', 'boolean', 'string', 'date-time'])

    repeats_performed = 0
    while repeats_performed < repeats_to_perform or literal_types_remaining:
        stream = TypeChangeStream(insert_count, repeats_performed * insert_count)

        repeats_performed += 1
        if stream.changing_literal_type in literal_types_remaining:
            literal_types_remaining.remove(stream.changing_literal_type)

        main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'root',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('changing_literal_type__s', 'text', 'YES'),
                                     ('changing_literal_type__t', 'timestamp with time zone', 'YES'),
                                     ('changing_literal_type__b', 'boolean', 'YES'),
                                     ('changing_literal_type__i', 'bigint', 'YES'),
                                     ('changing_literal_type__f', 'double precision', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {}, {}, {}, {}, {} FROM {}').format(
                sql.Identifier('changing_literal_type__s'),
                sql.Identifier('changing_literal_type__t'),
                sql.Identifier('changing_literal_type__b'),
                sql.Identifier('changing_literal_type__i'),
                sql.Identifier('changing_literal_type__f'),
                sql.Identifier('root')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the split columns migrated data/persisted new data
            assert repeats_performed * insert_count == len(persisted_records)
            assert insert_count <= len([x for x in persisted_records if x[0] is not None])
            assert insert_count <= len([x for x in persisted_records if x[1] is not None])
            assert insert_count <= len([x for x in persisted_records if x[2] is not None])
            ## Integers are valid Numbers, so sometimes a Number can be placed into an existing Integer column
            assert (2 * insert_count) \
                   <= len([x for x in persisted_records if x[3] is not None]) \
                   + len([x for x in persisted_records if x[4] is not None])
            assert 0 == len(
                [x for x in persisted_records
                 if x[0] is not None
                 and x[1] is not None
                 and x[2] is not None
                 and x[3] is not None
                 and x[4] is not None])
            assert 0 == len(
                [x for x in persisted_records
                 if x[0] is None
                 and x[1] is None
                 and x[2] is None
                 and x[3] is None
                 and x[4] is None])


def test_loading__column_type_change__nullable(db_cleanup):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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


def test_loading__column_type_change__nullable__missing_from_schema(db_cleanup):
    cat_count = 20
    main(CONFIG, input_stream=CatStream(cat_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(sql.SQL('SELECT {} FROM {}').format(
                sql.Identifier('name'),
                sql.Identifier('cats')
            ))
            persisted_records = cur.fetchall()

            ## Assert that the original data is present
            assert cat_count == len(persisted_records)
            assert cat_count == len([x for x in persisted_records if x[0] is not None])

    class NameMissingCatStream(CatStream):
        def generate_record(self):
            record = CatStream.generate_record(self)
            record['id'] = record['id'] + cat_count
            del record['name']
            return record

    stream = NameMissingCatStream(cat_count)
    stream.schema = deepcopy(stream.schema)
    del stream.schema['schema']['properties']['name']

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'YES'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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


def test_loading__multi_types_columns(db_cleanup):
    stream_count = 50
    main(CONFIG, input_stream=MultiTypeStream(stream_count))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'root',
                                 {
                                     ('_sdc_primary_key', 'text', 'NO'),
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('every_type__i', 'bigint', 'YES'),
                                     ('every_type__f', 'double precision', 'YES'),
                                     ('every_type__b', 'boolean', 'YES'),
                                     ('every_type__t', 'timestamp with time zone', 'YES'),
                                     ('every_type__i__1', 'bigint', 'YES'),
                                     ('every_type__f__1', 'double precision', 'YES'),
                                     ('every_type__b__1', 'boolean', 'YES'),
                                     ('number_which_only_comes_as_integer', 'double precision', 'NO')
                                 })

            assert_columns_equal(cur,
                                 'root__every_type',
                                 {
                                     ('_sdc_source_key__sdc_primary_key', 'text', 'NO'),
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_value', 'bigint', 'NO'),
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
    def invalid_stream_named(stream_name):
        stream = CatStream(100)
        stream.stream = stream_name
        stream.schema = deepcopy(stream.schema)
        stream.schema['stream'] = stream_name

        main(CONFIG, input_stream=stream)

    invalid_stream_named('')
    invalid_stream_named('x' * 1000)
    invalid_stream_named('INVALID_name')
    invalid_stream_named('a!!!invalid_name')

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
    main(CONFIG, input_stream=stream)


def test_loading__table_name__stream__simple(db_cleanup):
    def assert_tables_equal(cursor, expected_table_names):
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = []
        for table in cursor.fetchall():
            tables.append(table[0])

        assert (not tables and not expected_table_names) \
               or set(tables) == expected_table_names

    stream_name = "C@ts"
    stream = CatStream(100)
    stream.stream = stream_name
    stream.schema = deepcopy(stream.schema)
    stream.schema['stream'] = stream_name
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = []
            for table in cur.fetchall():
                tables.append(table[0])

            assert {'c_ts', 'c_ts__adoption__immunizations'} == set(tables)

            assert_columns_equal(cur,
                                 'c_ts',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'c_ts__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('c_ts'))
            assert cur.fetchone()[0] == 100

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, 'c_ts', 'id')


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
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            cur.execute(get_count_sql('cats'))
            assert 2 * cat_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__immunizations'))
            assert immunizations_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__invalid_non_conflicting'))
            assert invalid_name_count == cur.fetchone()[0]

            cur.execute(get_count_sql('cats__adoption__immunizations__1'))
            assert conflicting_name_count == cur.fetchone()[0]


def test_loading__invalid_column_name(db_cleanup):
    non_alphanumeric_stream = CatStream(100)
    non_alphanumeric_stream.schema = deepcopy(non_alphanumeric_stream.schema)
    non_alphanumeric_stream.schema['schema']['properties']['!!!invalid_name'] = \
        non_alphanumeric_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=non_alphanumeric_stream)

    non_lowercase_stream = CatStream(100)
    non_lowercase_stream.schema = deepcopy(non_lowercase_stream.schema)
    non_lowercase_stream.schema['schema']['properties']['INVALID_name'] = \
        non_lowercase_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=non_lowercase_stream)

    duplicate_non_lowercase_stream_1 = CatStream(100)
    duplicate_non_lowercase_stream_1.schema = deepcopy(duplicate_non_lowercase_stream_1.schema)
    duplicate_non_lowercase_stream_1.schema['schema']['properties']['invalid!NAME'] = \
        duplicate_non_lowercase_stream_1.schema['schema']['properties']['age']

    main(CONFIG, input_stream=duplicate_non_lowercase_stream_1)

    duplicate_non_lowercase_stream_2 = CatStream(100)
    duplicate_non_lowercase_stream_2.schema = deepcopy(duplicate_non_lowercase_stream_2.schema)
    duplicate_non_lowercase_stream_2.schema['schema']['properties']['invalid#NAME'] = \
        duplicate_non_lowercase_stream_2.schema['schema']['properties']['age']

    main(CONFIG, input_stream=duplicate_non_lowercase_stream_2)

    duplicate_non_lowercase_stream_3 = CatStream(100)
    duplicate_non_lowercase_stream_3.schema = deepcopy(duplicate_non_lowercase_stream_3.schema)
    duplicate_non_lowercase_stream_3.schema['schema']['properties']['invalid%NAmE'] = \
        duplicate_non_lowercase_stream_3.schema['schema']['properties']['age']

    main(CONFIG, input_stream=duplicate_non_lowercase_stream_3)

    name_too_long_stream = CatStream(100)
    name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
    name_too_long_stream.schema['schema']['properties']['x' * 1000] = \
        name_too_long_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=name_too_long_stream)

    duplicate_name_too_long_stream = CatStream(100)
    duplicate_name_too_long_stream.schema = deepcopy(duplicate_name_too_long_stream.schema)
    duplicate_name_too_long_stream.schema['schema']['properties']['x' * 100] = \
        duplicate_name_too_long_stream.schema['schema']['properties']['age']

    main(CONFIG, input_stream=duplicate_name_too_long_stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('___invalid_name', 'bigint', 'YES'),
                                     ('invalid_name', 'bigint', 'YES'),
                                     ('invalid_name__1', 'bigint', 'YES'),
                                     ('invalid_name__2', 'bigint', 'YES'),
                                     ('invalid_name__3', 'bigint', 'YES'),
                                     ('x' * 63, 'bigint', 'YES'),
                                     (('x' * 60 + '__1'), 'bigint', 'YES'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })


def test_loading__invalid_column_name__pk(db_cleanup):
    def setup(count):
        class Stream(CatStream):
            def generate_record(self):
                record = CatStream.generate_record(self)
                record['ID'] = record['id']
                record.pop('id')
                return record

        stream = Stream(count)
        stream.schema = deepcopy(stream.schema)
        stream.schema['schema']['properties']['ID'] = \
            stream.schema['schema']['properties']['id']

        stream.schema['key_properties'] = ['ID']
        stream.schema['schema']['properties'].pop('id')

        return stream

    main(CONFIG, input_stream=setup(100))
    main(CONFIG, input_stream=setup(200))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })


def test_loading__invalid_column_name__duplicate_name_handling(db_cleanup):
    for i in range(101):
        name_too_long_stream = CatStream(100)
        name_too_long_stream.schema = deepcopy(name_too_long_stream.schema)
        name_too_long_stream.schema['schema']['properties']['x' * (100 + i)] = \
            name_too_long_stream.schema['schema']['properties']['age']

        main(CONFIG, input_stream=name_too_long_stream)

    expected_columns = {
        ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
        ('_sdc_received_at', 'timestamp with time zone', 'YES'),
        ('_sdc_sequence', 'bigint', 'YES'),
        ('_sdc_table_version', 'bigint', 'YES'),
        ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
        ('adoption__was_foster', 'boolean', 'YES'),
        ('age', 'bigint', 'YES'),
        ('id', 'bigint', 'NO'),
        ('name', 'text', 'NO'),
        ('bio', 'text', 'NO'),
        ('paw_size', 'bigint', 'NO'),
        ('paw_colour', 'text', 'NO'),
        ('x' * 63, 'bigint', 'YES'),
        (('x' * 58 + '__100'), 'bigint', 'YES'),
        ('flea_check_complete', 'boolean', 'NO'),
        ('pattern', 'text', 'YES')
    }

    for i in range(1, 10):
        expected_columns.add((('x' * 60 + '__' + str(i)), 'bigint', 'YES'))
    for i in range(10, 100):
        expected_columns.add((('x' * 59 + '__' + str(i)), 'bigint', 'YES'))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur, 'cats', expected_columns)


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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('invalid_name', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('invalid_name__s', 'text', 'YES'),
                                     ('invalid_name__b', 'boolean', 'YES'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('invalid_name__s', 'text', 'YES'),
                                     ('invalid_name__b', 'boolean', 'YES'),
                                     ('invalid_name__i', 'bigint', 'YES'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
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
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })
        assert_records(conn, stream.records, 'cats', 'id')

    stream = CatStream(200)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 200
            assert_columns_equal(cur,
                                 'cats',
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 'cats__adoption__immunizations',
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })
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


def test_loading__very_long_stream_name(db_cleanup):
    stream_name = 'extremely_______________long_cats'
    class LongCatStream(CatStream):
        stream = stream_name
        schema = CatStream.schema.copy()
    LongCatStream.schema['stream'] = stream_name
    stream = LongCatStream(100)

    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            assert_columns_equal(cur,
                                 stream_name,
                                 {
                                     ('_sdc_batched_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_received_at', 'timestamp with time zone', 'YES'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_table_version', 'bigint', 'YES'),
                                     ('adoption__adopted_on', 'timestamp with time zone', 'YES'),
                                     ('adoption__was_foster', 'boolean', 'YES'),
                                     ('age', 'bigint', 'YES'),
                                     ('id', 'bigint', 'NO'),
                                     ('name', 'text', 'NO'),
                                     ('bio', 'text', 'NO'),
                                     ('paw_size', 'bigint', 'NO'),
                                     ('paw_colour', 'text', 'NO'),
                                     ('flea_check_complete', 'boolean', 'NO'),
                                     ('pattern', 'text', 'YES')
                                 })

            assert_columns_equal(cur,
                                 '{}__adoption__immunizations'.format(stream_name),
                                 {
                                     ('_sdc_level_0_id', 'bigint', 'NO'),
                                     ('_sdc_sequence', 'bigint', 'YES'),
                                     ('_sdc_source_key_id', 'bigint', 'NO'),
                                     ('date_administered', 'timestamp with time zone', 'YES'),
                                     ('type', 'text', 'YES')
                                 })

            cur.execute(get_count_sql(stream_name))
            assert cur.fetchone()[0] == 100

        for record in stream.records:
            record['paw_size'] = 314159
            record['paw_colour'] = ''
            record['flea_check_complete'] = False

        assert_records(conn, stream.records, stream_name, 'id')
        assert_column_indexed(conn, stream_name, '_sdc_sequence')
        assert_column_indexed(conn, stream_name, 'id')
        assert_column_indexed(conn, '{}__adoption__immunizations'.format(stream_name), '_sdc_sequence')
        assert_column_indexed(conn, '{}__adoption__immunizations'.format(stream_name), '_sdc_level_0_id')


def test_before_run_sql_is_executed_upon_construction(db_cleanup):
    config = CONFIG.copy()
    config['before_run_sql'] = 'CREATE TABLE before_sql_test ( code char(5) CONSTRAINT firstkey PRIMARY KEY );'
    config['after_run_sql'] = 'CREATE TABLE after_sql_test ( code char(5) CONSTRAINT secondkey PRIMARY KEY );'

    main(config, input_stream=CatStream(100))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='before_sql_test';")
            assert cur.fetchone()[0] == 'before_sql_test'

            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name='after_sql_test';")
            assert cur.fetchone()[0] == 'after_sql_test'
