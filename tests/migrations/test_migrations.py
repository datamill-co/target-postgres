'''
If we assert that any upgrade to _latest_ from some older version should
be met with chaining versions together...

versions = [v0 v1 v2]

v0
v0 -> v1
v1
v1 -> v2
v2
...
vn-1 -> vn
vn
'''

from copy import deepcopy
import json
import os
import pytest
import subprocess

import psycopg2
from psycopg2 import sql

from utils.fixtures import CONFIG, TEST_DB

SCHEMA_PREFIX = "migration_testing__"
FILE_PATH = "/code/tests/migrations/"


def abs_path(relative_path):
    return FILE_PATH + relative_path


def list_schemas():
    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '{}%'".format(SCHEMA_PREFIX))
            return cur.fetchall()


def clear_schema(schema):
    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL(
                'DROP SCHEMA IF EXISTS {} CASCADE;').format(
                sql.Identifier(schema)))


def clear_db():
    for schema in list_schemas():
        clear_schema(schema)


@pytest.fixture
def db_cleanup():
    clear_db()

    yield


def create_schema(schema):
    name = SCHEMA_PREFIX + schema
    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL(
                'CREATE SCHEMA IF NOT EXISTS {};').format(
                sql.Identifier(name)))

    return name


def setup_config(version, psql_schema):
    os.makedirs(abs_path("artifacts"), exist_ok=True)

    config_path = abs_path("artifacts/config--{}.json".format(psql_schema))

    if not os.path.exists(config_path):
        target_config = deepcopy(CONFIG)
        target_config['schema'] = psql_schema

        with open(config_path, 'w') as outfile:
            json.dump(target_config, outfile)

    return config_path


def script_cmd(script, *args):
    cmd = [abs_path("scripts/{}.sh".format(script))] + list(args)

    p = subprocess.Popen(cmd)
    communication = p.communicate()
    if p.returncode:
        raise Exception(communication)

    return communication


def tap_to_target(version, psql_schema):
    config_path = setup_config(version, psql_schema)

    if version == 'LATEST':
        return script_cmd("to_latest", config_path)

    return script_cmd("to_target", version, config_path)


def _test_versions(versions):
    length = len(versions)
    for idx in range(length):
        version = versions[idx]
        if idx:
            prev_version = versions[idx - 1]
            schema = create_schema('{}_{}'.format(prev_version, version))
            tap_to_target(prev_version, schema)
            tap_to_target(version, schema)

        schema = create_schema(version)
        tap_to_target(version, schema)


def test(db_cleanup):
    _test_versions(['schema0', 'schema1', 'LATEST'])
