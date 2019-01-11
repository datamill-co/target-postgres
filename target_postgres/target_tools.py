import http.client
import io
import json
import pkg_resources
import sys
import threading

import singer
from singer import utils, metadata, metrics

from target_postgres import json_schema
from target_postgres.singer_stream import BufferedSingerStream

LOGGER = singer.get_logger()


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


def stream_to_target(stream, target, config={}):
    """
    Persist `stream` to `target` with optional `config`.
    :param stream: iterator which represents a Singer data stream
    :param target: object which implements `write_batch` and `activate_version`
    :param config: [optional] configuration for buffers etc.
    :return: None
    """

    streams = {}
    try:
        if not config.get('disable_collection', False):
            _async_send_usage_stats()

        invalid_records_detect = config.get('invalid_records_detect')
        invalid_records_threshold = config.get('invalid_records_threshold')
        max_batch_rows = config.get('max_batch_rows')
        max_batch_size = config.get('max_batch_size')
        batch_detection_threshold = config.get('batch_detection_threshold', 5000)

        line_count = 0
        for line in stream:
            _line_handler(streams,
                          target,
                          invalid_records_detect,
                          invalid_records_threshold,
                          max_batch_rows,
                          max_batch_size,
                          line)
            if line_count > 0 and line_count % batch_detection_threshold == 0:
                _flush_streams(streams, target)
            line_count += 1

        _flush_streams(streams, target, force=True)

        return None

    except Exception as e:
        LOGGER.critical(e)
        raise e
    finally:
        _report_invalid_records(streams)


class TargetError(Exception):
    """
    Raise when there is an Exception streaming data to the target.
    """


def _flush_stream(target, stream_buffer):
    target.write_batch(stream_buffer)
    stream_buffer.flush_buffer()


def _flush_streams(streams, target, force=False):
    for stream_buffer in streams.values():
        if force or stream_buffer.buffer_full:
            _flush_stream(target, stream_buffer)


def _report_invalid_records(streams):
    for stream_buffer in streams.values():
        if stream_buffer.peek_invalid_records():
            LOGGER.warning("Invalid records detected for stream {}: {}".format(
                stream_buffer.stream,
                stream_buffer.peek_invalid_records()
            ))


def _line_handler(streams, target, invalid_records_detect, invalid_records_threshold, max_batch_rows, max_batch_size,
                  line):
    try:
        line_data = json.loads(line)
    except json.decoder.JSONDecodeError:
        LOGGER.error("Unable to parse JSON: {}".format(line))
        raise

    if 'type' not in line_data:
        raise TargetError('`type` is a required key: {}'.format(line))

    if line_data['type'] == 'SCHEMA':
        if 'stream' not in line_data:
            raise TargetError('`stream` is a required key: {}'.format(line))

        stream = line_data['stream']

        if 'schema' not in line_data:
            raise TargetError('`schema` is a required key: {}'.format(line))

        schema = line_data['schema']

        schema_validation_errors = json_schema.validation_errors(schema)
        if schema_validation_errors:
            raise TargetError('`schema` is an invalid JSON Schema instance: {}'.format(line), *schema_validation_errors)

        if 'key_properties' in line_data:
            key_properties = line_data['key_properties']
        else:
            key_properties = None

        if stream not in streams:
            buffered_stream = BufferedSingerStream(stream,
                                                   schema,
                                                   key_properties,
                                                   invalid_records_detect=invalid_records_detect,
                                                   invalid_records_threshold=invalid_records_threshold)
            if max_batch_rows:
                buffered_stream.max_rows = max_batch_rows
            if max_batch_size:
                buffered_stream.max_buffer_size = max_batch_size
            streams[stream] = buffered_stream
        else:
            streams[stream].update_schema(schema, key_properties)
    elif line_data['type'] == 'RECORD':
        if 'stream' not in line_data:
            raise TargetError('`stream` is a required key: {}'.format(line))
        if line_data['stream'] not in streams:
            raise TargetError('A record for stream {} was encountered before a corresponding schema'
                              .format(line_data['stream']))

        streams[line_data['stream']].add_record_message(line_data)

    elif line_data['type'] == 'ACTIVATE_VERSION':
        if 'stream' not in line_data:
            raise TargetError('`stream` is a required key: {}'.format(line))
        if 'version' not in line_data:
            raise TargetError('`version` is a required key: {}'.format(line))
        if line_data['stream'] not in streams:
            raise TargetError('A ACTIVATE_VERSION for stream {} was encountered before a corresponding schema'
                              .format(line_data['stream']))

        stream_buffer = streams[line_data['stream']]
        target.write_batch(stream_buffer)
        target.activate_version(stream_buffer, line_data['version'])
    elif line_data['type'] == 'STATE':
        LOGGER.warning('`STATE` Singer message type not supported')
    else:
        raise TargetError('Unknown message type {} in message {}'.format(
            line_data['type'],
            line))


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
