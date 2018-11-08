import pytest

from target_postgres.singer_stream import BufferedSingerStream, Error
from fixtures import CatStream, CATS_SCHEMA


def test_init():
    assert BufferedSingerStream(CATS_SCHEMA['stream'],
                                CATS_SCHEMA['schema'],
                                CATS_SCHEMA['key_properties'])


def test_add_record_message():
    stream = CatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])
    assert singer_stream.add_record_message(stream.generate_record_message()) is None


def test_add_record_message__invalid_record():
    stream = CatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])
    with pytest.raises(Error):
        record = stream.generate_record_message()
        record['record']['name'] = 22 / 7

        singer_stream.add_record_message(record)


def test_add_record_message__invalid_record__detection_off():
    stream = CatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_detect=False)

    record = stream.generate_record_message()
    record['record']['name'] = 22 / 7

    singer_stream.add_record_message(record)


def test_add_record_message__invalid_record__cross_threshold():
    stream = CatStream(10)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_threshold=3)

    record = stream.generate_record_message()
    record['record']['name'] = 22 / 7
    singer_stream.add_record_message(record)

    record = stream.generate_record_message()
    record['record']['age'] = 'not an age'
    singer_stream.add_record_message(record)

    with pytest.raises(Error):
        record = stream.generate_record_message()
        record['record']['adoption'] = [1, 2, 'not a kitty cat']

        singer_stream.add_record_message(record)
