#!/usr/bin/env python3

import sys
import io
import json
import uuid
from datetime import datetime
from copy import deepcopy

import singer
from singer import utils, metadata, metrics
import psycopg2

from .pysize import get_size
from target_postgres.postgres import PostgresTarget

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]

CONFIG = {}

STREAMS = {}

class BufferedSingerStream(object):
    def __init__(self,
                 stream,
                 schema,
                 key_properties,
                 *args,
                 max_rows=200000,
                 max_buffer_size=104857600, # 100MB
                 buffer_timeout=600, # 10 minutes
                 **kwargs):
        self.update_schema(schema, key_properties)
        self.stream = stream
        self.max_rows = max_rows
        self.max_buffer_size = max_buffer_size
        self.buffer_timeout = buffer_timeout

        self.__buffer = []
        self.__size = 0
        self.__last_flush = datetime.utcnow()

    def update_schema(self, schema, key_properties):
        original_schema = deepcopy(schema)

        ## TODO: create validator?
        ## TODO: validate against stricter contrainsts for this target?

        ## TODO: mark schema dirty here for caching in PostgresTarget

        ## TODO: move to PostgresTarget?
        if len(key_properties) == 0:
            self.use_uuid_pk = True
            key_properties = [PostgresTarget.SINGER_PK]
            schema['properties'][PostgresTarget.SINGER_PK] = {
                'type': 'string'
            }
        else:
            self.use_uuid_pk = False

        ## TODO: move to PostgresTarget?
        if PostgresTarget.SINGER_SEQUENCE in schema['properties']:
            self.sequence_field = PostgresTarget.SINGER_SEQUENCE
        elif PostgresTarget.SINGER_RECEIVED_AT in schema['properties']:
            self.sequence_field = PostgresTarget.SINGER_RECEIVED_AT
        else:
            self.sequence_field = None

        self.schema = schema
        self.original_schema = original_schema
        self.key_properties = key_properties

    @property
    def buffer_full(self):
        ln = len(self.__buffer)
        if ln >= self.max_rows:
            return True

        if ln > 0:
            if self.__size >= self.max_buffer_size:
                return True

            elapsed_since_flush = (datetime.utcnow() - self.__last_flush).total_seconds()
            if elapsed_since_flush >= self.buffer_timeout:
                return True
        return False

    def add_record(self, record):
        self.__buffer.append(record)
        self.__size += get_size(record)

    def peek_buffer(self):
        return self.__buffer

    def flush_buffer(self):
        _buffer = self.__buffer
        self.__buffer = []
        self.__size = 0
        return _buffer

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
        ## TODO: validate record
        STREAMS[line_data['stream']].add_record(line_data['record'])
    else:
        raise Exception('Unknown message type {} in message {}'.format(
            line_data['type'],
            line))

def main():
    try:
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        CONFIG.update(args.config)

        connection = psycopg2.connect(
            host=CONFIG.get('postgres_host', 'localhost'),
            port=int(CONFIG.get('postgres_host', '5432')),
            dbname=CONFIG.get('postgres_database'),
            user=CONFIG.get('postgres_username'),
            password=CONFIG.get('postgres_password'))

        postgres_target = PostgresTarget(
            connection,
            LOGGER,
            postgres_schema=CONFIG.get('postgres_schema', 'public'))

        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        line_count = 0
        for line in input_stream:
            line_handler(line)
            if line_count > 0 and line_count % 5000 == 0: ## TODO: line count or timeout?
                flush_streams(postgres_target)
            line_count += 1

        flush_streams(postgres_target, force=True)

        connection.close()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
