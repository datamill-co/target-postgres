import random

import pytest
from chance import chance

from target_postgres import denest, json_schema, singer


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

    for p, s in table_batch['streamed_schema']['schema']['properties'].items():
        if not json_schema.is_anyof(s):
            errors.append({
                'path': tuple(p),
                'message': 'Expected anyOf json schema for propery schema, got: {}'.format(s)
            })

    return errors


def errors(table_batch):
    return non_path_properties(table_batch) + missing_key_properties(table_batch)


def error_check_denest(schema, key_properties, records):
    denested = denest.to_table_batches(schema, key_properties, records)

    for table_batch in denested:
        assert [] == errors(table_batch)

    return denested


def test_empty():
    denested = error_check_denest({}, [], [])
    assert 1 == len(denested)
    assert [] == denested[0]['records']
    assert [] == denested[0]['streamed_schema']['key_properties']


def test__schema__objects_add_fields():
    denested = error_check_denest({'properties':
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
        denested = error_check_denest(r['schema'],
                                           [],
                                           [])

        print('r:', r)
        print()
        print('denested:', denested)

        assert 1 == len(denested)
        assert tuple(r['path']) in denested[0]['streamed_schema']['schema']['properties']


def test__schema__arrays_add_tables():
    denested = error_check_denest({'properties':
                                            {'a': {'type': 'integer'},
                                             'b': {'type': 'array',
                                                   'items': {'properties': {
                                                       'c': {'type': 'string'},
                                                       'd': {'type': 'boolean'}}}}}},
                                       ['a'],
                                       [])
    assert 2 == len(denested)


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
        denested = error_check_denest(r['schema'],
                                           [],
                                           [])

        print('r:', r)
        print()
        print('denested:', denested)

        assert len(r['path']) + 1 == len(denested)

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


NESTED_SCHEMA = {
    "properties": {
        "a": {"type": "object",
              "properties": {
                  "b": {
                      "type": "array",
                      "items": {
                          "type": "object",
                          "properties": {
                              "c": {
                                  "type": "object",
                                  "properties": {
                                      "d": {"type": "integer"},
                                      "e": {"type": "array",
                                            "items": {"type": "object",
                                                      "properties": {
                                                          "f": {"type": "string"},
                                                          "g": {"type": "boolean"}}}}}}}}}}}}}

NESTED_RECORDS = [{"a": {"b": []}},
                  {"a": {"b": [{"c": {"d": 1}}]}},
                  {"a": {"b": [{"c": {"d": 12}},
                               {"c": {"d": 123}}]}},
                  {"a": {"b": [{"c": {"d": 1234}},
                               {"c": {"d": 12345}},
                               {"c": {"d": 123456}}]}},
                  {"a": {"b": [{"c": {"e": [{"f": "hello",
                                             "g": True},
                                            {"f": "goodbye",
                                             "g": True}]}}]}}]


def test__records__nested__tables():
    denested = error_check_denest(NESTED_SCHEMA, [], NESTED_RECORDS)

    print('denested:', denested)

    assert 3 == len(denested)

    for table_batch in denested:
        assert table_batch['streamed_schema']['path'] in \
               {tuple(),
                ('a', 'b'),
                ('a', 'b', 'c', 'e')}


def _get_table_batch_with_path(table_batches, path):
    for table_batch in table_batches:
        if path == table_batch['streamed_schema']['path']:
            return table_batch
    raise Exception('Could not find table_batch with path: {}'.format(path))


def test__records__nested__root_empty():
    denested = error_check_denest(NESTED_SCHEMA, [], NESTED_RECORDS)
    table_batch = _get_table_batch_with_path(denested,
                                             tuple())

    assert {} == table_batch['streamed_schema']['schema']['properties']

    assert 5 == len(table_batch['records'])

    for record in table_batch['records']:
        assert {} == record


def test__records__nested__child_table__a_b():
    denested = error_check_denest(NESTED_SCHEMA, [], NESTED_RECORDS)
    table_batch = _get_table_batch_with_path(denested,
                                             ('a', 'b'))

    assert 1 == len(table_batch['streamed_schema']['schema']['properties'][('c', 'd')]['anyOf'])
    assert {'type': ['integer']} == table_batch['streamed_schema']['schema']['properties'][('c', 'd')]['anyOf'][0]

    assert 7 == len(table_batch['records'])

    for record in table_batch['records']:
        # Don't try to access key "('c', 'd')" if record is empty
        if record == {}:
            continue
        assert 'integer' == record[('c', 'd')][0]
        assert int == type(record[('c', 'd')][1])


def test__records__nested__child_table__a_b_c_e():
    denested = error_check_denest(NESTED_SCHEMA, [], NESTED_RECORDS)
    table_batch = _get_table_batch_with_path(denested,
                                             ('a', 'b', 'c', 'e'))

    assert 1 == len(table_batch['streamed_schema']['schema']['properties'][('f',)]['anyOf'])
    assert {'type': ['string']} == table_batch['streamed_schema']['schema']['properties'][('f',)]['anyOf'][0]
    assert 1 == len(table_batch['streamed_schema']['schema']['properties'][('g',)]['anyOf'])
    assert {'type': ['boolean']} == table_batch['streamed_schema']['schema']['properties'][('g',)]['anyOf'][0]

    assert 2 == len(table_batch['records'])

    for record in table_batch['records']:
        assert 'string' == record[('f',)][0]
        assert str == type(record[('f',)][1])

        assert 'boolean' == record[('g',)][0]
        assert bool == type(record[('g',)][1])


def test__anyOf__schema__stitch_date_times():
    denested = error_check_denest(
        {'properties': {
            'a': {
                "anyOf": [
                    {
                        "type": "string",
                        "format": "date-time"
                    },
                    {"type": ["string", "null"]}]}}},
        [],
        [])
    table_batch = _get_table_batch_with_path(denested, tuple())

    anyof_schemas = table_batch['streamed_schema']['schema']['properties'][('a',)]['anyOf']

    assert 2 == len(anyof_schemas)
    assert 2 == len([x for x in anyof_schemas if json_schema.is_literal(x)])
    assert 2 == len([x for x in anyof_schemas if json_schema.is_nullable(x)])
    assert 1 == len([x for x in anyof_schemas if json_schema.is_datetime(x)])

def test__anyOf__schema__implicit_any_of():
    denested = error_check_denest(
        {
            'properties': {
                'every_type': {
                    'type': ['integer', 'null', 'number', 'boolean', 'string', 'array', 'object'],
                    'items': {'type': 'integer'},
                    'format': 'date-time',
                    'properties': {
                        'i': {'type': 'integer'},
                        'n': {'type': 'number'},
                        'b': {'type': 'boolean'}
                    }
                }
            }
        },
        [],
        [])
    assert 2 == len(denested)

    table_batch = _get_table_batch_with_path(denested, tuple())
    denested_props = table_batch['streamed_schema']['schema']['properties']

    assert 4 == len(denested_props)

    anyof_schemas = denested_props[('every_type',)]['anyOf']

    assert 4 == len(anyof_schemas)
    assert 4 == len([x for x in anyof_schemas if json_schema.is_literal(x)])
    assert 4 == len([x for x in anyof_schemas if json_schema.is_nullable(x)])
    assert 1 == len([x for x in anyof_schemas if json_schema.is_datetime(x)])


def test__anyOf__schema__implicit_any_of__arrays():
    denested = error_check_denest(
        {
            'properties': {
                'every_type': {
                    'type': ['null', 'string', 'array', 'object'],
                    'items': {
                        'anyOf': [
                            {'type': 'integer'},
                            {'type': 'number'}]
                    },
                    'format': 'date-time',
                    'properties': {
                        'i': {'type': 'integer'}
                    }
                }
            }
        },
        [],
        [])
    assert 2 == len(denested)

    table_batch = _get_table_batch_with_path(denested, ('every_type',))
    denested_props = table_batch['streamed_schema']['schema']['properties']
    anyof_schemas = denested_props[(singer.VALUE,)]['anyOf']

    assert 2 == len(anyof_schemas)
    assert 2 == len([x for x in anyof_schemas if json_schema.is_literal(x)])


def test__anyOf__schema__implicit_any_of__objects():
    denested = error_check_denest(
        {
            'properties': {
                'every_type': {
                    'type': ['integer', 'null', 'number', 'boolean', 'string', 'array', 'object'],
                    'items': {'type': 'integer'},
                    'format': 'date-time',
                    'properties': {
                        'i': {'anyOf': [
                            {'type': 'integer'},
                            {'type': 'number'},
                            {'type': 'boolean'}]
                        }
                    }
                }
            }
        },
        [],
        [])
    assert 2 == len(denested)

    table_batch = _get_table_batch_with_path(denested, tuple())
    denested_props = table_batch['streamed_schema']['schema']['properties']
    print(denested_props)
    anyof_schemas = denested_props[('every_type', 'i')]['anyOf']

    assert 3 == len(anyof_schemas)
    assert 3 == len([x for x in anyof_schemas if json_schema.is_literal(x)])
