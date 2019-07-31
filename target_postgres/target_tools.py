import http.client
import io
import json
import pkg_resources
import sys
import threading

from singer import utils, metadata, metrics

from target_postgres.globals import LOGGER
from target_postgres.pipes.batch import batch
from target_postgres.pipes.sql_flatten import flatten
from target_postgres.pipes.load import load


def main(target):
    """
    Given a target, stream stdin input as a text stream.
    :param target: object which implements `write_batch` and `activate_version`
    :return: None
    """
    config = utils.parse_args([]).config
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    stream_to_target(input_stream, target, config=config)

    return None


class BatchStreamBufferAdapter:
    def __init__(self, batch):
        self._batch = batch

    @property
    def count(self):
        return len(self.get_batch())

    @property
    def key_properties(self):
        return self._batch['key_properties']

    @property
    def max_version(self):
        return self._batch['max_version']

    @property
    def schema(self):
        return self._batch['schema']

    @property
    def stream(self):
        return self._batch['stream']

    def get_batch(self):
        return self._batch['records']

    def peek_buffer(self):
        return self._batch['records']


def stream_to_target(stream, target, config={}):
    """
    Persist `stream` to `target` with optional `config`.
    :param stream: iterator which represents a Singer data stream
    :param target: object which implements `write_batch` and `activate_version`
    :param config: [optional] configuration for buffers etc.
    :return: None
    """

    try:
        if not config.get('disable_collection', False):
            _async_send_usage_stats()

        as_loaded = load(stream)
        as_batches = batch(config, as_loaded)
        as_table_batches = flatten(as_batches)

        for line_data in as_table_batches:
            if line_data['type'] == 'STATE':
                line = json.dumps(line_data)
                sys.stdout.write("{}\n".format(line))
                sys.stdout.flush()

            elif line_data['type'] == 'ACTIVATE_VERSION':
                target.activate_version(line_data)

            elif line_data['type'] == '__DataMill__TABLE_BATCH':
                target.write_batch(line_data)

    except Exception as e:
        LOGGER.critical(e)
        raise e


def _send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-postgres').version
        with http.client.HTTPConnection('collector.singer.io', timeout=10).connect() as conn:
            params = {
                'e': 'se',
                'aid': 'singer',
                'se_ca': 'target-postgres',
                'se_ac': 'open',
                'se_la': version,
            }
            conn.request('GET', '/i?' + urllib.parse.urlencode(params))
            conn.getresponse()
    except:
        LOGGER.debug('Collection request failed')


def _async_send_usage_stats():
    LOGGER.info('Sending version information to singer.io. ' +
                'To disable sending anonymous usage data, set ' +
                'the config parameter "disable_collection" to true')
    threading.Thread(target=_send_usage_stats()).start()
