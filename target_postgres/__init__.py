from singer import utils
import psycopg2

from target_postgres.postgres import MillisLoggingConnection, PostgresTarget
from target_postgres import target_tools

REQUIRED_CONFIG_KEYS = [
    'postgres_database'
]


def main(config, input_stream=None):
    with psycopg2.connect(
            connection_factory=MillisLoggingConnection,
            host=config.get('postgres_host', 'localhost'),
            port=config.get('postgres_port', 5432),
            dbname=config.get('postgres_database'),
            user=config.get('postgres_username'),
            password=config.get('postgres_password'),
            sslmode=config.get('postgres_sslmode'),
            sslcert=config.get('postgres_sslcert'),
            sslkey=config.get('postgres_sslkey'),
            sslrootcert=config.get('postgres_sslrootcert'),
            sslcrl=config.get('postgres_sslcrl')
    ) as connection:
        postgres_target = PostgresTarget(
            connection,
            postgres_schema=config.get('postgres_schema', 'public'),
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables'),
            add_upsert_indexes=config.get('add_upsert_indexes', True),
            before_run_sql=config.get('before_run_sql'),
            after_run_sql=config.get('after_run_sql'),
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, postgres_target, config=config)
        else:
            target_tools.main(postgres_target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
