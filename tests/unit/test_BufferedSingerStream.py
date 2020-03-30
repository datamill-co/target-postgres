from decimal import Decimal
from copy import deepcopy

import pytest

from target_postgres import singer
from target_postgres.singer_stream import BufferedSingerStream, SingerStreamError, RAW_LINE_SIZE

from utils.fixtures import CatStream, InvalidCatStream, CATS_SCHEMA


def missing_sdc_properties(stream_buffer):
    errors = []
    for p in [singer.BATCHED_AT, singer.RECEIVED_AT, singer.SEQUENCE, singer.TABLE_VERSION]:
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
    assert [singer.PK] == singer_stream.key_properties

    rows_missing_pk = []
    rows_checked = 0
    for r in singer_stream.get_batch():
        if not r[singer.PK]:
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


SIMPLE_MULTIPLE_OF_VALID_SCHEMA = {
    'properties': {
        'multipleOfKey': {
            'type': 'number',
            'multipleOf': Decimal('1e-15')
        }
    }
}

SIMPLE_MULTIPLE_OF_INVALID_SCHEMA = {
    'properties': {
        'multipleOfKey': {
            'type': 'number',
            'multipleOf': 1e-15
        }
    }
}

def test_add_record_message__multipleOf():
    stream_name = 'test'
    singer_stream = BufferedSingerStream(stream_name,
                                         deepcopy(SIMPLE_MULTIPLE_OF_VALID_SCHEMA),
                                         [])

    multiple_of_values = ['1', '2', '3', '4', '5', '1.1', '2.3', '1.23456789', '20', '100.1']

    for value in multiple_of_values:
        singer_stream.add_record_message(
            {
                'type': 'RECORD',
                'stream': stream_name,
                'record': {'multipleOfKey': Decimal(value)},
                'sequence': 0,
                RAW_LINE_SIZE: 100
            }
        )

    assert not singer_stream.peek_invalid_records()
    assert singer_stream.count == len(multiple_of_values)


def test_add_record_message__multipleOf_invalid_record():
    stream_name = 'test'
    singer_stream = BufferedSingerStream(stream_name,
                                         deepcopy(SIMPLE_MULTIPLE_OF_INVALID_SCHEMA),
                                         [])

    multiple_of_values = [1, 2]

    for value in multiple_of_values:
        with pytest.raises(SingerStreamError):
            singer_stream.add_record_message(
                {
                    'type': 'RECORD',
                    'stream': stream_name,
                    'record': {'multipleOfKey': value},
                    'sequence': 0,
                    RAW_LINE_SIZE: 100
                }
            )

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0


SIMPLE_ALLOF_SCHEMA = {
  'type': 'object',
  'properties': {
      'allOfKey': {
          'allOf': [
              { 'type': ['string'] },
              { 'maxLength': 5 }
              ]}}}


def test_add_record_message__allOf():
    stream_name = 'test'
    singer_stream = BufferedSingerStream(stream_name,
                                         deepcopy(SIMPLE_ALLOF_SCHEMA),
                                         [])

    strs_shorter_than_6 = [
        'hello',
        'I',
        'am',
        'a set',
        'of',
        'short',
        'strs'
    ]

    for string in strs_shorter_than_6:
        singer_stream.add_record_message(
            {
                'type': 'RECORD',
                'stream': stream_name,
                'record': {'allOfKey': string},
                'sequence': 0
            }
        )

    assert not singer_stream.peek_invalid_records()
    assert singer_stream.count == len(strs_shorter_than_6)
    assert [] == missing_sdc_properties(singer_stream)


def test_add_record_message__allOf__invalid_record():
    stream_name = 'test'
    singer_stream = BufferedSingerStream(stream_name,
                                         deepcopy(SIMPLE_ALLOF_SCHEMA),
                                         [])

    with pytest.raises(SingerStreamError):
        singer_stream.add_record_message(
            {
                'type': 'RECORD',
                'stream': stream_name,
                'record': {'allOfKey': 'this is a string which is much too long to be allowed'},
                'sequence': 0
            }
        )

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0
    assert [] == missing_sdc_properties(singer_stream)


def test_add_record_message__allOf__impossible_schema():
    stream_name = 'test'

    schema = deepcopy(SIMPLE_ALLOF_SCHEMA)
    schema['properties']['allOfKey']['allOf'].append({'type': ['number']})

    singer_stream = BufferedSingerStream(stream_name,
                                         schema,
                                         [])


    with pytest.raises(SingerStreamError):
        singer_stream.add_record_message(
            {
                'type': 'RECORD',
                'stream': stream_name,
                'record': {'allOfKey': 'short'},
                'sequence': 0
            }
        )
    with pytest.raises(SingerStreamError):
        singer_stream.add_record_message(
            {
                'type': 'RECORD',
                'stream': stream_name,
                'record': {'allOfKey': 314159},
                'sequence': 0
            }
        )

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
                                         max_buffer_size=10)

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
