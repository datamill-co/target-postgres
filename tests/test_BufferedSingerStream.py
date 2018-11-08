import pytest

from target_postgres.singer_stream import BufferedSingerStream, Error
from fixtures import CatStream, InvalidCatStream, CATS_SCHEMA


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
    assert not singer_stream.peek_invalid_records()


def test_add_record_message__invalid_record():
    stream = InvalidCatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'])
    with pytest.raises(Error):
        singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0


def test_add_record_message__invalid_record__detection_off():
    stream = InvalidCatStream(10)
    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_detect=False)

    singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0


def test_add_record_message__invalid_record__cross_threshold():
    stream = InvalidCatStream(10)

    singer_stream = BufferedSingerStream(CATS_SCHEMA['stream'],
                                         CATS_SCHEMA['schema'],
                                         CATS_SCHEMA['key_properties'],
                                         invalid_records_threshold=3)

    singer_stream.add_record_message(stream.generate_record_message())
    singer_stream.add_record_message(stream.generate_record_message())

    with pytest.raises(Error):
        singer_stream.add_record_message(stream.generate_record_message())

    assert singer_stream.peek_invalid_records()
    assert singer_stream.count == 0
