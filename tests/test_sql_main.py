from copy import deepcopy
from unittest.mock import patch

import pytest

from fixtures import CONFIG, InvalidCatStream
from target_postgres import singer_stream
from target_postgres.sql_base import SQLInterface
from target_postgres import sql_main


class Target(SQLInterface):
    IDENTIFIER_FIELD_LENGTH = 50

    def __init__(self):
        self.calls = {'write_batch': [], 'activate_version': []}

    def write_batch(self, stream_buffer):
        self.calls['write_batch'].append({'records_count': len(stream_buffer.peek_buffer())})
        return None

    def activate_version(self, stream_buffer, version):
        self.calls['activate_version'] += 1
        return None


def test_loading__invalid__records():
    with pytest.raises(singer_stream.SingerStreamError, match=r'.*'):
        sql_main.stream_to_target(CONFIG, None,
                                  input_stream=InvalidCatStream(1))


def test_loading__invalid__records__disable():
    config = deepcopy(CONFIG)
    config['invalid_records_detect'] = False

    target = Target()

    sql_main.stream_to_target(config, target, input_stream=InvalidCatStream(100))

    ## Since all `cat`s records were invalid, we could not persist them, hence, no calls made to `write_batch`
    assert len(target.calls['write_batch']) == 1
    assert target.calls['write_batch'][0]['records_count'] == 0


def test_loading__invalid__records__threshold():
    config = deepcopy(CONFIG)
    config['invalid_records_threshold'] = 10

    target = Target()

    with pytest.raises(singer_stream.SingerStreamError, match=r'.*.10*'):
        sql_main.stream_to_target(config, target, input_stream=InvalidCatStream(20))

    assert len(target.calls['write_batch']) == 0


def test_usage_stats():
    config = deepcopy(CONFIG)
    assert config['disable_collection']

    with patch.object(sql_main,
                      '_async_send_usage_stats') as mock:
        target = Target()
        sql_main.stream_to_target(config, target, input_stream=InvalidCatStream(0))

        assert mock.call_count == 0

        config['disable_collection'] = False
        target = Target()
        sql_main.stream_to_target(config, target, input_stream=InvalidCatStream(0))

        assert mock.call_count == 1
