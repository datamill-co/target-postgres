#!/usr/bin/env python3

import sys
import io
import json
import uuid

import singer
from singer import utils, metadata, metrics
import psycopg2

from target_postgres.postgres import PostgresTarget
from target_postgres.singer_stream import BufferedSingerStream

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]

STREAMS = {}

def flush_stream(target, stream_buffer):
    target.write_records(stream_buffer)

def flush_streams(target, force=False):
    for stream_buffer in STREAMS.values():
        if force or stream_buffer.buffer_full:
            flush_stream(target, stream_buffer)

def line_handler(line):
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

        if stream not in STREAMS:
            buffered_stream = BufferedSingerStream(stream, schema, key_properties)
            STREAMS[stream] = buffered_stream
        else:
            STREAMS[stream].update_schema(schema, key_properties)
    elif line_data['type'] == 'RECORD':
        if 'stream' not in line_data:
            raise Exception('`stream` is a required key: {}'.format(line))
        if line_data['stream'] not in STREAMS:
            raise Exception('A record for stream {} was encountered before a corresponding schema'
                .format(line_data['stream']))

        STREAMS[line_data['stream']].add_record(line_data['record'])
    else:
        raise Exception('Unknown message type {} in message {}'.format(
            line_data['type'],
            line))

def main(config, input_stream=None):
    try:
        connection = psycopg2.connect(
            host=config.get('postgres_host', 'localhost'),
            port=int(config.get('postgres_host', '5432')),
            dbname=config.get('postgres_database'),
            user=config.get('postgres_username'),
            password=config.get('postgres_password'))

        postgres_target = PostgresTarget(
            connection,
            LOGGER,
            postgres_schema=config.get('postgres_schema', 'public'))

        if not input_stream:
            input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        line_count = 0
        for line in input_stream:
            line_handler(line)
            if line_count > 0 and line_count % 5000 == 0: ## TODO: line count or timeout?
                flush_streams(postgres_target)
            line_count += 1

        flush_streams(postgres_target, force=True)

        connection.close()
    except Exception as e:
        LOGGER.critical(e)
        raise e

if __name__ == "__main__":
    try:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        main(args.config)
    except Exception as e:
        LOGGER.critical(e)
        raise e
