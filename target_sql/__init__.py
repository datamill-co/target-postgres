#!/usr/bin/env python3

import sys
import io
import json
import uuid

import singer
from singer import utils, metadata, metrics
import psycopg2

from target_sql.target_postgres import TargetPostgres
from target_sql.target_redshift import TargetRedshift
from target_sql.singer_stream import BufferedSingerStream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'target_connection'
]

def flush_stream(target, stream_buffer):
    target.write_batch(stream_buffer)

def flush_streams(streams, target, force=False):
    for stream_buffer in streams.values():
        if force or stream_buffer.buffer_full:
            flush_stream(target, stream_buffer)

def line_handler(streams, target, max_batch_rows, max_batch_size, line):
    try:
        line_data = json.loads(line)
    except json.decoder.JSONDecodeError:
        LOGGER.error("Unable to parse JSON: {}".format(line))
        raise

    if 'type' not in line_data:
        raise Exception('`type` is a required key: {}'.format(line))

    if line_data['type'] == 'SCHEMA':
        if 'stream' not in line_data:
            raise Exception('`stream` is a required key: {}'.format(line))
        if 'schema' not in line_data:
            raise Exception('`schema` is a required key: {}'.format(line))

        stream = line_data['stream']
        schema = line_data['schema']
        if 'key_properties' in line_data:
            key_properties = line_data['key_properties']
        else:
            key_properties = None

        if stream not in streams:
            buffered_stream = BufferedSingerStream(stream,
                                                   schema,
                                                   key_properties)
            if max_batch_rows:
                buffered_stream.max_rows = max_batch_rows
            if max_batch_size:
                buffered_stream.max_buffer_size = max_batch_size
            streams[stream] = buffered_stream
        else:
            streams[stream].update_schema(schema, key_properties)
    elif line_data['type'] == 'RECORD':
        if 'stream' not in line_data:
            raise Exception('`stream` is a required key: {}'.format(line))
        if line_data['stream'] not in streams:
            raise Exception('A record for stream {} was encountered before a corresponding schema'
                .format(line_data['stream']))

        streams[line_data['stream']].add_record_message(line_data)
    elif line_data['type'] == 'ACTIVATE_VERSION':
        if 'stream' not in line_data:
            raise Exception('`stream` is a required key: {}'.format(line))
        if 'version' not in line_data:
            raise Exception('`version` is a required key: {}'.format(line))
        if line_data['stream'] not in streams:
            raise Exception('A ACTIVATE_VERSION for stream {} was encountered before a corresponding schema'
                .format(line_data['stream']))

        stream_buffer = streams[line_data['stream']]
        target.write_batch(stream_buffer)
        target.activate_version(stream_buffer, line_data['version'])
    elif line_data['type'] == 'STATE':
        LOGGER.warn('`STATE` Singer message type not supported')
    else:
        raise Exception('Unknown message type {} in message {}'.format(
            line_data['type'],
            line))

def target_sql(target_class, config, input_stream=None):
    try:
        with target_class(config, LOGGER) as target:
            max_batch_rows = config.get('max_batch_rows')
            max_batch_size = config.get('max_batch_size')
            batch_detection_threshold = config.get('batch_detection_threshold', 5000)

            if not input_stream:
                input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

            line_count = 0
            streams = {}
            for line in input_stream:
                line_handler(streams, target, max_batch_rows, max_batch_size, line)
                if line_count > 0 and line_count % batch_detection_threshold == 0:
                    flush_streams(streams, target)
                line_count += 1

            flush_streams(streams, target, force=True)
    except Exception as e:
        LOGGER.critical(e)
        raise e

def main(target_class):
    try:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        target_sql(target_class, args.config)
    except Exception as e:
        LOGGER.critical(e)
        raise e

def target_postgres_main():
    main(TargetPostgres)

def target_redshift_main():
    main(TargetRedshift)
