import pytest

from target_postgres.singer_stream import (
    BufferedSingerStream, SingerStreamError,
    SINGER_BATCHED_AT,
    SINGER_PK,
    SINGER_RECEIVED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION
)
from fixtures import CatStream, InvalidCatStream, CATS_SCHEMA


def missing_sdc_properties(stream_buffer):
    errors = []
    for p in [SINGER_BATCHED_AT, SINGER_RECEIVED_AT, SINGER_SEQUENCE, SINGER_TABLE_VERSION]:
        if not p in stream_buffer.schema['properties']:
            errors.append({'_sdc': p,
                           'message': '`_sdc` missing'})

    return errors


def test_init():
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])

    assert singer_stream
    assert [] == missing_sdc_properties(singer_stream)


def test_init__empty_key_properties():
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         [])

    stream = CatStream(100)
    for _ in range(20):
        singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream
    assert [] == missing_sdc_properties(singer_stream)
    assert [SINGER_PK] == singer_stream.key_properties

    rows_missing_pk = []
    rows_checked = 0
    for r in singer_stream.get_batch():
        if not r[SINGER_PK]:
            rows_missing_pk.append(r)

        rows_checked += 1

    assert rows_checked > 1
    assert [] == rows_missing_pk


def test_add_record_message():
    stream = CatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])
    assert singer_stream.add_record_message(stream.generate_record_message()) is None
    assert not singer_stream.peek_invalid_records()
    assert [] == missing_sdc_properties(singer_stream)


def test_add_record_message__invalid_record():
    stream = InvalidCatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])
    with pytest.raises(SingerStreamError):
        singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0
    assert [] == missing_sdc_properties(singer_stream)


def test_add_record_message__invalid_record__detection_off():
    stream = InvalidCatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_detect=False)

    singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0
    assert [] == missing_sdc_properties(singer_stream)


def test_add_record_message__invalid_record__cross_threshold():
    stream = InvalidCatStream(10)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_threshold=3)

    singer_stream.add_record_message(stream.generate_record_message())
    singer_stream.add_record_message(stream.generate_record_message())

    with pytest.raises(SingerStreamError):
        singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0
    assert [] == missing_sdc_properties(singer_stream)


def mocked_mock_write_batch(stream_buffer):
    stream_buffer.flush_buffer()


def test_multiple_batches__by_rows():
    stream = CatStream(100)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         max_rows=20)

    assert len(singer_stream.peek_buffer()) == 0

    while not singer_stream.buffer_full:
        singer_stream.add_record_message(stream.generate_record_message())

    assert len(singer_stream.peek_buffer()) == 20
    assert [] == missing_sdc_properties(singer_stream)

    singer_stream.flush_buffer()

    assert len(singer_stream.peek_buffer()) == 0


def test_multiple_batches__by_memory():
    stream = CatStream(100)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         max_buffer_size=1024)

    assert len(singer_stream.peek_buffer()) == 0

    while not singer_stream.buffer_full:
        singer_stream.add_record_message(stream.generate_record_message())

    assert len(singer_stream.peek_buffer()) == 1
    assert [] == missing_sdc_properties(singer_stream)

    singer_stream.flush_buffer()

    assert len(singer_stream.peek_buffer()) == 0


def test_multiple_batches__old_records__by_rows():
    stream_oldest = CatStream(100, version=0)
    stream_middle_aged = CatStream(100, version=5)
    stream_latest = CatStream(100, version=10)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         max_rows=20)

    assert len(singer_stream.peek_buffer()) == 0

    while not singer_stream.buffer_full:
        singer_stream.add_record_message(stream_oldest.generate_record_message())

    assert len(singer_stream.peek_buffer()) == 20

    singer_stream.flush_buffer()

    assert len(singer_stream.peek_buffer()) == 0

    singer_stream.add_record_message(stream_latest.generate_record_message())

    assert len(singer_stream.peek_buffer()) == 1

    reasonable_cutoff = 1000
    while not singer_stream.buffer_full and reasonable_cutoff != 0:
        singer_stream.add_record_message(stream_middle_aged.generate_record_message())
        reasonable_cutoff -= 1

    assert reasonable_cutoff == 0
    assert len(singer_stream.peek_buffer()) == 1
    assert [] == missing_sdc_properties(singer_stream)


def test_multiple_batches__old_records__by_memory():
    stream_oldest = CatStream(100, version=0)
    stream_middle_aged = CatStream(100, version=5)
    stream_latest = CatStream(100, version=10)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         max_buffer_size=32768)

    assert len(singer_stream.peek_buffer()) == 0

    while not singer_stream.buffer_full:
        singer_stream.add_record_message(stream_oldest.generate_record_message())

    assert len(singer_stream.peek_buffer()) > 0
    assert [] == missing_sdc_properties(singer_stream)

    singer_stream.flush_buffer()

    assert len(singer_stream.peek_buffer()) == 0

    singer_stream.add_record_message(stream_latest.generate_record_message())

    assert len(singer_stream.peek_buffer()) == 1

    reasonable_cutoff = 1000
    while not singer_stream.buffer_full and reasonable_cutoff != 0:
        singer_stream.add_record_message(stream_middle_aged.generate_record_message())
        reasonable_cutoff -= 1

    assert reasonable_cutoff == 0
    assert len(singer_stream.peek_buffer()) == 1
    assert [] == missing_sdc_properties(singer_stream)
