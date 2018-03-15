import io

import psycopg2

from target_postgres import main
from fixtures import CatStream, CONFIG, TEST_DB, db_cleanup

## TODO: create and test more fake streams
## TODO: test invalid data against JSON Schema
## TODO: test stream with duplicate records
## TODO: test stream with duplicate records, that have nested arrays

def get_columns_sql(table_name):
    return "SELECT column_name, data_type, is_nullable FROM information_schema.columns " + \
           "WHERE table_schema = 'public' and table_name = '{}' order by column_name;".format(
        table_name)

def get_count_sql(table_name):
    return 'SELECT count(*) FROM "public"."{}"'.format(table_name)

def test_loading_simple(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_columns_sql('cats'))
            columns = cur.fetchall()

            assert columns == [
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
            ]

            cur.execute(get_columns_sql('cats__adoption__immunizations'))
            columns = cur.fetchall()

            assert columns == [
                ('_sdc_level_0_id', 'bigint', 'NO'),
                ('_sdc_source_key_id', 'bigint', 'NO'),
                ('date_administered', 'timestamp with time zone', 'YES'),
                ('type', 'text', 'YES')
            ]

            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

def test_upsert(db_cleanup):
    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

    stream = CatStream(100)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 100

    stream = CatStream(200)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats'))
            assert cur.fetchone()[0] == 200

def test_nested_delete_on_parent(db_cleanup):
    stream = CatStream(100, nested_count=3)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            high_nested = cur.fetchone()[0]

    stream = CatStream(100, nested_count=2)
    main(CONFIG, input_stream=stream)

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(get_count_sql('cats__adoption__immunizations'))
            low_nested = cur.fetchone()[0]

    assert low_nested < high_nested
