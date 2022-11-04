import json
import os
import random

import pytest
import psycopg2
import arrow
from faker import Faker
from chance import chance

CONFIG = {
    'postgres_host': os.environ['POSTGRES_HOST'],
    'postgres_database': os.environ['POSTGRES_DATABASE'],
    'postgres_username': os.environ['POSTGRES_USERNAME'],
    'disable_collection': True,
    'logging_level': 'DEBUG'
}

TEST_DB = {
    'host': CONFIG['postgres_host'],
    'dbname': CONFIG['postgres_database'],
    'user': CONFIG['postgres_username']
}

fake = Faker()

CATS_SCHEMA = {
    'type': 'SCHEMA',
    'stream': 'cats',
    'schema': {
        'additionalProperties': False,
        'properties': {
            'id': {
                'type': 'integer'
            },
            'name': {
                'type': ['string']
            },
            'bio': {
                'type': ['string']
            },
            'paw_size': {
                'type': ['integer'],
                'default': 314159
            },
            'paw_colour': {
                'type': 'string',
                'default': ''
            },
            'flea_check_complete': {
                'type': ['boolean'],
                'default': False
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
                        'type': ['null', 'string'],
                        'format': 'date-time'
                    },
                    'was_foster': {
                        'type': 'boolean'
                    },
                    'immunizations': {
                        'type': ['null', 'array'],
                        'items': {
                            'type': ['object'],
                            'properties': {
                                'type': {
                                    'type': ['null', 'string']
                                },
                                'date_administered': {
                                    'type': ['null', 'string'],
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
    def __init__(self,
                 n,
                 *args,
                 version=None,
                 nested_count=0,
                 duplicates=0,
                 duplicate_sequence_delta=200,
                 sequence=None,
                 **kwargs):
        self.n = n
        self.wrote_schema = False
        self.id = 1
        self.nested_count = nested_count
        self.version = version
        self.wrote_activate_version = False
        self.records = []
        self.duplicates = duplicates
        self.duplicates_written = 0
        self.duplicate_pks_used = []
        self.record_message_count = 0
        if sequence:
            self.sequence = sequence
        else:
            self.sequence = arrow.get().int_timestamp
        self.duplicate_sequence_delta = duplicate_sequence_delta

    def duplicate(self, force=False):
        if self.duplicates > 0 and \
                len(self.records) > 0 and \
                self.duplicates_written < self.duplicates and \
                (force or chance.boolean(likelihood=30)):
            self.duplicates_written += 1
            random_index = random.randint(0, len(self.records) - 1)
            record = self.records[random_index]
            self.duplicate_pks_used.append(record['id'])
            record_message = self.generate_record_message(record=record)
            record_message['sequence'] = self.sequence + self.duplicate_sequence_delta
            return record_message
        else:
            return False

    def generate_record_message(self, record=None):
        if not record:
            record = self.generate_record()
            self.id += 1

        self.records.append(record)
        message = {
            'type': 'RECORD',
            'stream': self.stream,
            'record': record,
            'sequence': self.sequence
        }

        if self.version is not None:
            message['version'] = self.version

        self.record_message_count += 1

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
            dup = self.duplicate()
            if dup != False:
                return json.dumps(dup)
            return json.dumps(self.generate_record_message())
        if self.id == self.n:
            dup = self.duplicate(force=True)
            if dup != False:
                return json.dumps(dup)
        if self.version is not None and self.wrote_activate_version == False:
            return json.dumps(self.activate_version())
        raise StopIteration


def fake_conjunctive_text(n):
    t = fake.text()
    for i in range(0, n):
        t = '{}, {} {}'.format(
            t[:-1],
            chance.pickone(['and', 'or', 'for', 'nor', 'but', 'yet', 'so']),
            fake.text())
    return t


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
            'bio': fake_conjunctive_text(random.randint(0, 10)),
            'pattern': chance.pickone(['Tabby', 'Tuxedo', 'Calico', 'Tortoiseshell']),
            'age': random.randint(1, 15),
            'adoption': adoption
        }


class InvalidCatStream(CatStream):
    def generate_record(self):
        record = CatStream.generate_record(self)

        if chance.boolean(likelihood=50):
            record['adoption'] = ['invalid', 'adoption']
        elif chance.boolean(likelihood=50):
            record['age'] = 'very invalid age'
        elif record['adoption'] and chance.boolean(likelihood=50):
            record['adoption']['immunizations'] = {
                'type': chance.pickone(['a', 'b', 'c']),
                'date_administered': ['clearly', 'not', 'a', 'date']
            }
        else:
            record['name'] = 22 / 7

        return record


NESTED_STREAM = {
    'type': 'SCHEMA',
    'stream': 'root',
    'schema': {
        'additionalProperties': False,
        'properties': {
            'id': {
                'type': 'integer'
            },
            ## TODO: Complex types defaulted
            # 'array_scalar_defaulted': {
            #     'type': 'array',
            #     'items': {'type': 'integer'},
            #     'default': list(range(10))
            # },
            'array_scalar': {
                'type': 'array',
                'items': {'type': 'integer'}
            },
            'array_of_array': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'items': {
                        'type': 'array',
                        'items': {'type': 'integer'}
                    }
                }
            },
            ## TODO: Complex types defaulted
            # 'object_defaulted': {
            #     'type': 'object',
            #     'properties': {
            #         'a': {
            #             'type': 'integer'
            #         },
            #         'b': {
            #             'type': 'integer'
            #         },
            #         'c': {
            #             'type': 'integer'
            #         }
            #     },
            #     'default': {'a': 123, 'b': 456, 'c': 789}
            # },
            'object_of_object_0': {
                'type': 'object',
                'properties': {
                    'object_of_object_1': {
                        'type': 'object',
                        'properties': {
                            'object_of_object_2': {
                                'type': 'object',
                                'properties': {
                                    'array_scalar': {
                                        'type': 'array',
                                        'items': {
                                            'type': 'boolean'
                                        }
                                    },
                                    'a': {
                                        'type': 'integer'
                                    },
                                    'b': {
                                        'type': 'integer'
                                    },
                                    'c': {
                                        'type': 'integer'
                                    }
                                }
                            }
                        }
                    }
                }
            },
            'null': {
                'type': ['null', 'integer']
            },
            'nested_null': {
                'type': 'object',
                'properties': {
                    'null': {
                        'type': ['null', 'integer']
                    }
                }
            }
        }
    },
    'key_properties': ['id']
}


class NestedStream(FakeStream):
    stream = 'root'
    schema = NESTED_STREAM

    def generate_record(self):
        null = None
        ## We use this trick so that we _always_ know we'll have both null and non-null values
        ##  vs using something like chance here.
        if self.id % 2 == 0:
            null = 31415

        return {
            'id': self.id,
            'array_scalar': list(range(5)),
            'array_of_array': [[[1, 2, 3],
                                [4, 5, 6, 7, 8],
                                [9, 10],
                                []],
                               [[10],
                                [20, 30],
                                [40, 50, 60],
                                [70, 80, 90, 100]]],
            'object_of_object_0': {
                'object_of_object_1': {
                    'object_of_object_2': {
                        'array_scalar': [True, False, True, False, False],
                        'a': self.id,
                        'b': self.id,
                        'c': self.id
                    }
                }
            },
            'null': null,
            'nested_null': {
                'null': null
            }
        }


MULTI_TYPE = {
    'type': 'SCHEMA',
    'stream': 'root',
    'schema': {
        'additionalProperties': False,
        'properties': {
            'every_type': {
                'type': ['null', 'integer', 'number', 'boolean', 'string', 'array', 'object'],
                'items': {'type': 'integer'},
                'format': 'date-time',
                'properties': {
                    ## We use these field names to increase the difficulty for our column
                    ##  name collision functionality. ie, the denested values will not only
                    ##  conflict in terms of their denested _names_ but also, their types
                    'i': {'type': 'integer'},
                    'f': {'type': 'number'},
                    'b': {'type': 'boolean'}
                }
            },
            'number_which_only_comes_as_integer': {
                'type': 'number'
            }
        }
    },
    'key_properties': []
}


class MultiTypeStream(FakeStream):
    stream = 'root'
    schema = MULTI_TYPE

    def generate_record(self):
        value_null = None
        value_integer = random.randint(-314159265359, 314159265359)
        value_integer_as_number = float(random.randint(-314159265359, 314159265359))
        value_number = random.uniform(-314159265359, 314159265359)
        value_boolean = chance.boolean()
        value_date_time_string = chance.date(minyear=2012).isoformat()
        value_array = []
        for i in range(random.randint(0, 1000)):
            value_array.append(random.randint(-314, 314))

        value_object = {'i': random.randint(-314159265359, 314159265359),
                        'n': random.uniform(-314159265359, 314159265359),
                        'b': chance.boolean()}

        return {
            'every_type': chance.pickone(
                [value_null,
                 value_integer,
                 value_integer_as_number,
                 value_number,
                 value_boolean,
                 value_date_time_string,
                 value_array,
                 value_object]),
            'number_which_only_comes_as_integer': value_integer
        }


class TypeChangeStream(FakeStream):
    stream = 'root'

    def __init__(self, n, starting_id):
        FakeStream.__init__(self, n)
        self.starting_id = starting_id
        self.changing_literal_type = chance.pickone(['integer', 'number', 'boolean', 'string', 'date-time'])
        type_def = {'type': self.changing_literal_type}

        if self.changing_literal_type == 'date-time':
            type_def = {'type': 'string',
                        'format': 'date-time'}

        print('TypeChangeStream chose:', type_def, 'id starting at:', self.id)
        self.schema = {
            'type': 'SCHEMA',
            'stream': 'root',
            'schema': {
                'additionalProperties': False,
                'properties': {
                    'id': {'type': 'integer'},
                    'changing_literal_type': type_def
                }
            },
            'key_properties': ['id']
        }

    def generate_record(self):
        value = None
        if self.changing_literal_type == 'integer':
            value = random.randint(-314159265359, 314159265359)
        elif self.changing_literal_type == 'number':
            value = chance.pickone([random.uniform(-314159265359, 314159265359),
                                    float(random.randint(-314159265359, 314159265359)),
                                    random.randint(-314159265359, 314159265359)])
        elif self.changing_literal_type == 'boolean':
            value = chance.boolean()
        elif self.changing_literal_type == 'string':
            value = chance.date(minyear=2012).isoformat()
        elif self.changing_literal_type == 'date-time':
            value = chance.date(minyear=2012).isoformat()
        else:
            raise Exception('Unknown changing_literal_type: `{}`'.format(self.changing_literal_type))

        return {
            'id': self.id + self.starting_id,
            'changing_literal_type': value,
        }


def clear_db():
    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            drop_command = ''
            for table in cur.fetchall():
                drop_command += 'DROP TABLE IF EXISTS ' + table[0] + ';'
            cur.execute('begin;' +
                        drop_command +
                        'commit;')


@pytest.fixture
def db_cleanup():
    clear_db()

    yield

    clear_db()


class ListStream:
    idx = None
    stream = NotImplementedError()

    def __init__(self):
        self.idx = -1

    def __iter__(self):
        return self

    def __next__(self):
        self.idx += 1

        if self.idx < len(self.stream):
            return json.dumps(self.stream[self.idx])

        raise StopIteration


class DogStream(CatStream):
    stream = 'dogs'
    schema = CatStream.schema.copy()


DogStream.schema['stream'] = 'dogs'