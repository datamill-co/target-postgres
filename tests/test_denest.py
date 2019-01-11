import random

import pytest
from chance import chance

from target_postgres import denest
from target_postgres import json_schema
from target_postgres.singer_stream import (
    SINGER_BATCHED_AT,
    SINGER_RECEIVED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION,
)


def non_path_properties(table_batch):
    errors = []
    for p in table_batch['streamed_schema']['schema']['properties']:
        if not isinstance(p, tuple):
            errors.append({'x': p,
                           'message': '`x` is not a `tuple`'})

    return errors


def missing_key_properties(table_batch):
    errors = []
    for p in table_batch['streamed_schema']['key_properties']:
        if not (p,) in table_batch['streamed_schema']['schema']['properties']:
            errors.append({'path': tuple(p),
                           'message': 'key_property missing'})

    return errors


def errors(table_batch):
    return non_path_properties(table_batch) + missing_key_properties(table_batch)


def test_empty():
    denested = denest.to_table_batches({}, [], [])
    assert 1 == len(denested)
    assert [] == denested[0]['records']
    assert [] == denested[0]['streamed_schema']['key_properties']

    for table_batch in denested:
        assert [] == errors(table_batch)


def test__schema__objects_add_fields():
    denested = denest.to_table_batches({'properties':
                                            {'a': {'type': 'integer'},
                                             'b': {'type': 'object',
                                                   'properties': {
                                                       'c': {'type': 'string'},
                                                       'd': {'type': 'boolean'}}}}},
                                       ['a'],
                                       [])

    assert 1 == len(denested)
    assert ('b', 'c') in denested[0]['streamed_schema']['schema']['properties']
    assert ('b', 'd') in denested[0]['streamed_schema']['schema']['properties']

    for table_batch in denested:
        assert [] == errors(table_batch)


def random_object_schema():
    length_of_path = random.randint(1, 50)
    path = []
    schema = {'type': chance.pickone([json_schema.BOOLEAN,
                                      json_schema.INTEGER,
                                      json_schema.NUMBER,
                                      json_schema.STRING])}
    for _ in range(0, length_of_path):
        field = chance.string(pool='', length=0)
        schema = {'type': json_schema.OBJECT,
                  'properties': {field: schema}}
        path.append(field)

    return {'schema': schema,
            'path': path[::-1]}


def test__schema__nested_objects_add_fields():
    for _ in range(0, 100):
        r = random_object_schema()
        denested = denest.to_table_batches(r['schema'],
                                           [],
                                           [])

        print('r:', r)
        print()
        print('denested:', denested)

        assert 1 == len(denested)
        assert tuple(r['path']) in denested[0]['streamed_schema']['schema']['properties']

        for table_batch in denested:
            assert [] == errors(table_batch)

        print('PASSED')
        print()


def test__schema__arrays_add_tables():
    denested = denest.to_table_batches({'properties':
                                            {'a': {'type': 'integer'},
                                             'b': {'type': 'array',
                                                   'items': {'properties': {
                                                       'c': {'type': 'string'},
                                                       'd': {'type': 'boolean'}}}}}},
                                       ['a'],
                                       [])
    assert 2 == len(denested)
    for table_batch in denested:
        assert [] == errors(table_batch)


def random_array_schema():
    length_of_path = random.randint(1, 50)
    path = []
    schema = {'type': json_schema.ARRAY,
              'items': {'type': chance.pickone([json_schema.BOOLEAN,
                                                json_schema.INTEGER,
                                                json_schema.NUMBER,
                                                json_schema.STRING])}}
    for _ in range(0, length_of_path):
        field = chance.string(pool='', length=0)
        schema = {'type': json_schema.ARRAY,
                  'items': {'type': json_schema.OBJECT,
                            'properties': {field: schema}}}
        path.append(field)

    schema = {'type': json_schema.OBJECT,
              'properties': {
                  'root': schema}}
    path.append('root')

    return {'schema': schema,
            'path': path[::-1]}


def test__schema__nested_arrays_add_tables():
    for _ in range(0, 100):
        r = random_array_schema()
        denested = denest.to_table_batches(r['schema'],
                                           [],
                                           [])

        print('r:', r)
        print()
        print('denested:', denested)

        assert len(r['path']) + 1 == len(denested)

        for table_batch in denested:
            assert [] == errors(table_batch)

        table_path_accum = []
        tables_checked = 0
        while True:
            found_table = False

            print('looking for a table with path:', table_path_accum)

            for table_batch in denested:
                if tuple(table_path_accum) == table_batch['streamed_schema']['path']:
                    found_table = True
                    break

            assert found_table
            print('...table found')

            tables_checked += 1

            if len(table_path_accum) == len(r['path']):
                break

            table_path_accum.append(r['path'][len(table_path_accum)])

        ## Assert that we looked for every table path
        assert tables_checked == len(denested)

        print('PASSED')
        print()
