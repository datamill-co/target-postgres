from copy import deepcopy
import json

from unittest.mock import patch
import pytest

from target_postgres import singer_stream
from target_postgres import target_tools
from target_postgres.sql_base import SQLInterface

from fixtures import CONFIG, InvalidCatStream


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


def test_usage_stats():
    config = deepcopy(CONFIG)
    assert config['disable_collection']

    with patch.object(target_tools,
                      '_async_send_usage_stats') as mock:
        target_tools.stream_to_target([], None, config=config)

        assert mock.call_count == 0

        config['disable_collection'] = False

        target_tools.stream_to_target([], None, config=config)

        assert mock.call_count == 1


def test_loading__invalid__records():
    with pytest.raises(singer_stream.SingerStreamError, match=r'.*'):
        target_tools.stream_to_target(InvalidCatStream(1), None, config=CONFIG)


def test_loading__invalid__records__disable():
    config = deepcopy(CONFIG)
    config['invalid_records_detect'] = False

    target = Target()

    target_tools.stream_to_target(InvalidCatStream(100), target, config=config)

    ## Since all `cat`s records were invalid, we could not persist them, hence, no calls made to `write_batch`
    assert len(target.calls['write_batch']) == 1
    assert target.calls['write_batch'][0]['records_count'] == 0


def test_loading__invalid__records__threshold():
    config = deepcopy(CONFIG)
    config['invalid_records_threshold'] = 10

    target = Target()

    with pytest.raises(singer_stream.SingerStreamError, match=r'.*.10*'):
        target_tools.stream_to_target(InvalidCatStream(20), target, config=config)

    assert len(target.calls['write_batch']) == 0


def test_state__capture(capsys):
    stream = [
        json.dumps({'type': 'STATE', 'value': { 'test': 'state-1' }}),
        json.dumps({'type': 'STATE', 'value': { 'test': 'state-2' }})]

    target_tools.stream_to_target(stream, Target())

    out, _ = capsys.readouterr()

    filtered_output = list(filter(None, out.split('\n')))

    assert len(filtered_output) == 2
    assert json.loads(filtered_output[0])['test'] == 'state-1'
    assert json.loads(filtered_output[1])['test'] == 'state-2'
