import sys
import json
import random

import pytest
import psycopg2
from faker import Faker
from chance import chance

CONFIG = {
    'postgres_database': 'target_postgres_test'
}

TEST_DB = {
    'host': 'localhost',
    'port': 5432,
    'dbname': CONFIG['postgres_database'],
    'user': None,
    'password': None
}

fake = Faker()

CATS_SCHEMA = {
    'type': 'SCHEMA',
    'stream': 'cats',
    'schema': {
        'properties': {
            'id': {
                'type': ['integer']
            },
            'name': {
                'type': ['string']
            },
            'pattern': {
                'type': ['null', 'string']
            },
            'age': {
                'type': ['null', 'integer']
            },
            'adoption': {
                'type': ['object', 'null'],
                'properties': {
                    'adopted_on': {
                        'type': ['null','string'],
                        'format': 'date-time'
                    },
                    'was_foster': {
                        'type': ['boolean']
                    },
                    'immunizations': {
                        'type': ['null','array'],
                        'items': {
                            'type': ['object'],
                            'properties': {
                                'type': {
                                    'type': ['null','string']
                                },
                                'date_administered': {
                                    'type': ['null','string'],
                                    'format': 'date-time'
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    'key_properties': ['id']
}

class FakeStream(object):
    def __init__(self, n, *args, version=None, nested_count=0, **kwargs):
        self.n = n
        self.wrote_schema = False
        self.id = 1
        self.nested_count = nested_count
        self.version = version
        self.wrote_activate_version = False
        self.records = []

    def generate_record_message(self):
        record = self.generate_record()
        self.records.append(record)
        message = {
            'type': 'RECORD',
            'stream': self.stream,
            'record': record
        }

        self.id += 1

        if self.version is not None:
            message['version'] = self.version

        return message

    def activate_version(self):
        self.wrote_activate_version = True
        return {
            'type': 'ACTIVATE_VERSION',
            'stream': self.stream,
            'version': self.version
        }

    def __iter__(self):
        return self

    def __next__(self):
        if not self.wrote_schema:
            self.wrote_schema = True
            return json.dumps(self.schema)
        if self.id <= self.n:
            return json.dumps(self.generate_record_message())
        if self.version is not None and self.wrote_activate_version == False:
            return json.dumps(self.activate_version())
        raise StopIteration

class CatStream(FakeStream):
    stream = 'cats'
    schema = CATS_SCHEMA

    def generate_record(self):
        adoption = None
        if self.nested_count or chance.boolean(likelihood=70):
            immunizations = []
            for i in range(0, self.nested_count or random.randint(0, 4)):
                immunizations.append({
                    'type': chance.pickone(['FIV', 'Panleukopenia', 'Rabies', 'Feline Leukemia']),
                    'date_administered': chance.date(minyear=2012).isoformat()
                })
            adoption = {
                'adopted_on': chance.date(minyear=2012).isoformat(),
                'was_foster': chance.boolean(),
                'immunizations': immunizations
            }

        return {
            'id': self.id,
            'name': fake.first_name(),
            'pattern': chance.pickone(['Tabby', 'Tuxedo', 'Calico', 'Tortoiseshell']),
            'age': random.randint(1, 15),
            'adoption': adoption
        }

def clear_db():
    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute('begin;' +
                        'drop table if exists cats;' +
                        'drop table if exists cats__adoption__immunizations;' +
                        'commit;')

@pytest.fixture
def db_cleanup():
    clear_db()

    yield

    #clear_db()
