#!/usr/bin/env python3

import singer
from singer import utils
import psycopg2

from target_postgres.postgres import PostgresTarget
import target_postgres.sql_main

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]

def main(config, input_stream=None):
    try:
        with psycopg2.connect(
                host=config.get('postgres_host', 'localhost'),
                port=config.get('postgres_port', 5432),
                dbname=config.get('postgres_database'),
                user=config.get('postgres_username'),
                password=config.get('postgres_password')
        ) as connection:
            postgres_target = PostgresTarget(
                connection,
                postgres_schema=config.get('postgres_schema', 'public'))
            sql_main.stream_to_target(config, postgres_target, input_stream=input_stream)
    except Exception as e:
        LOGGER.critical(e)
        raise e

def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
