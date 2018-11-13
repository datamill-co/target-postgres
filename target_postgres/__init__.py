#!/usr/bin/env python3

import sys
import io
import json
import uuid
import threading
import http.client
import pkg_resources

import singer
from singer import utils, metadata, metrics
import psycopg2

from target_postgres.postgres import PostgresTarget
from target_postgres.singer_stream import BufferedSingerStream
from target_postgres import json_schema

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]

class TargetError(Exception):
    """
    Raise when there is an Exception streaming data to the target.
    """

def flush_stream(target, stream_buffer):
    target.write_batch(stream_buffer)

def flush_streams(streams, target, force=False):
    for stream_buffer in streams.values():
        if force or stream_buffer.buffer_full:
            flush_stream(target, stream_buffer)

def report_invalid_records(streams):
    for stream_buffer in streams.values():
        if stream_buffer.peek_invalid_records():
            LOGGER.warn("Invalid records detected for stream {}: {}".format(
                stream_buffer.stream,
                stream_buffer.peek_invalid_records()
            ))

def line_handler(streams, target, invalid_records_detect, invalid_records_threshold, max_batch_rows, max_batch_size, line):
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
        LOGGER.warn('`STATE` Singer message type not supported')
    else:
        raise TargetError('Unknown message type {} in message {}'.format(
            line_data['type'],
            line))

def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-postgres').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-postgres',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        LOGGER.debug('Collection request failed')

def main(config, input_stream=None):
    streams = {}
    try:
        if not config.get('disable_collection', False):
            LOGGER.info('Sending version information to singer.io. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            threading.Thread(target=send_usage_stats).start()

        connection = psycopg2.connect(
            host=config.get('postgres_host', 'localhost'),
            port=config.get('postgres_port', 5432),
            dbname=config.get('postgres_database'),
            user=config.get('postgres_username'),
            password=config.get('postgres_password'))

        postgres_target = PostgresTarget(
            connection,
            LOGGER,
            postgres_schema=config.get('postgres_schema', 'public'))

        invalid_records_detect = config.get('invalid_records_detect')
        invalid_records_threshold = config.get('invalid_records_threshold')
        max_batch_rows = config.get('max_batch_rows')
        max_batch_size = config.get('max_batch_size')
        batch_detection_threshold = config.get('batch_detection_threshold', 5000)

        if not input_stream:
            input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        line_count = 0
        for line in input_stream:
            line_handler(streams,
                         postgres_target,
                         invalid_records_detect,
                         invalid_records_threshold,
                         max_batch_rows,
                         max_batch_size,
                         line)
            if line_count > 0 and line_count % batch_detection_threshold == 0:
                flush_streams(streams, postgres_target)
            line_count += 1

        flush_streams(streams, postgres_target, force=True)

        connection.close()
    except Exception as e:
        LOGGER.critical(e)
        raise e
    finally:
        report_invalid_records(streams)

def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
