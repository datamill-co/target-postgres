from copy import deepcopy
from datetime import datetime
from unittest.mock import patch

import psycopg2
import psycopg2.extras
import pytest

from target_postgres import main
from target_postgres import singer_stream
from target_postgres import postgres
from fixtures import CatStream, CONFIG, db_cleanup, InvalidCatStream, TEST_DB

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

    with pytest.raises(Exception, match=r'.*invalid JSON Schema instance.*'):
        main(CONFIG, input_stream=stream)


def test_loading__invalid__records():
    with pytest.raises(Exception, match=r'.*'):
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

    with pytest.raises(Exception, match=r'.*.10*'):
        main(config, input_stream=InvalidCatStream(20))


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

        assert_records(conn, stream.records, 'cats', 'id')

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
