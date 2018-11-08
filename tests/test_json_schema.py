import pytest

from target_postgres import json_schema
from fixtures import CATS_SCHEMA


def test_is_object():
    assert json_schema.is_object({'type': ['object']})
    assert json_schema.is_object({'properties': {}})
    assert json_schema.is_object({})


def test_simplify_empty():
    assert json_schema.simplify({}) == {}


def test_simplify_types_into_arrays():
    assert \
        json_schema.simplify(
            {'type': 'null'}
        ) \
        == {'type': ['null']}

    assert \
        json_schema.simplify(
            {'type': ['object'],
             'properties': {
                 'a': {'type': 'string'}}}) \
        == {'type': ['object'],
            'properties': {
                'a': {'type': ['string']}}}


def test_simplify_complex():
    assert \
        json_schema.simplify({
            'type': ['null', 'array'],
            'items': {
                'type': 'object',
                'properties': {
                    'type': {
                        'type': ['null', 'string']
                    },
                    'date_administered': {
                        'type': 'string',
                        'format': 'date-time'}}}}) \
        == {
            'type': ['null', 'array'],
            'items': {
                'type': ['object'],
                'properties': {
                    'type': {
                        'type': ['null', 'string']
                    },
                    'date_administered': {
                        'type': ['string'],
                        'format': 'date-time'}}}}

    assert \
        json_schema.simplify(CATS_SCHEMA['schema']) \
        == {
            'type': ['object'],
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
                            'type': ['null', 'string'],
                            'format': 'date-time'
                        },
                        'was_foster': {
                            'type': ['boolean']
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
                                        'format': 'date-time'}}}}}}}}


def test_simplify_refs():
    assert \
        json_schema.simplify(
            {
                'definitions': {
                    'singleton': {
                        'type': 'string'
                    }},

                'type': 'object',

                'properties': {
                    'singleton': {'$ref': '#/definitions/singleton'}}}) \
        == {'type': ['object'],
            'properties': {
                'singleton': {
                    'type': ['string']}}}

    assert \
        json_schema.simplify(
            {
                'definitions': {
                    'foo': {
                        'type': 'object',
                        'properties': {
                            'bar': {
                                'type': 'object',
                                'properties': {
                                    'baz': {
                                        'type': 'integer'
                                    }
                                }
                            }
                        }
                    }},

                'type': 'object',

                'properties': {
                    'nested': {'$ref': '#/definitions/foo/properties/bar/properties/baz'}}}) \
        == {'type': ['object'],
            'properties': {
                'nested': {
                    'type': ['integer']}}}

    assert \
        json_schema.simplify(
            {
                'definitions': {
                    'address': {
                        'type': 'object',
                        'properties': {
                            'street_address': {'type': 'string'},
                            'city': {'type': 'string'},
                            'state': {'type': 'string'}
                        },
                        'required': ['street_address', 'city', 'state']
                    }
                },

                'type': 'object',

                'properties': {
                    'billing_address': {'$ref': '#/definitions/address'},
                    'shipping_address': {'$ref': '#/definitions/address'}}}) \
        == {'type': ['object'],
            'properties': {
                'billing_address': {
                    'type': ['object'],
                    'properties': {
                        'street_address': {'type': ['string']},
                        'city': {'type': ['string']},
                        'state': {'type': ['string']}
                    }
                },
                'shipping_address': {
                    'type': ['object'],
                    'properties': {
                        'street_address': {'type': ['string']},
                        'city': {'type': ['string']},
                        'state': {'type': ['string']}}}}}


def test_simplify_refs_invalid_format():
    with pytest.raises(Exception, match=r'Invalid format.*'):
        json_schema.simplify(
            {
                'properties': {
                    'singleton': {'$ref': ''}}})

    with pytest.raises(Exception, match=r'Invalid format.*'):
        json_schema.simplify(
            {
                'properties': {
                    'singleton': {'$ref': '123BWDSG!@R1513bw4tnb24'}}})

    with pytest.raises(Exception, match=r'Invalid format.*'):
        json_schema.simplify(
            {
                'properties': {
                    'singleton': {'$ref': '#definitions/singleton'}}})


def test_simplify_refs_missing():
    with pytest.raises(Exception, match=r'.*not found.*'):
        json_schema.simplify(
            {
                'properties': {
                    'singleton': {'$ref': '#/foo'}}})

    with pytest.raises(Exception, match=r'.*not found.*'):
        json_schema.simplify(
            {
                'definitions': {
                    'foo': {
                        'type': 'null'
                    }
                },
                'properties': {
                    'singleton': {'$ref': '#/definitions/foo/bar'}}})


def test_simplify_refs_circular():
    with pytest.raises(Exception, match=r'.*is recursive.*'):
        json_schema.simplify(
            {
                'definitions': {
                    'alice': {
                        '$ref': '#/definitions/bob'
                    },
                    'bob': {
                        '$ref': '#/definitions/alice'}},
                'properties': {
                    'alice': {
                        '$ref': '#/definitions/alice'
                    }
                }
            })

    with pytest.raises(Exception, match=r'.*is recursive.*'):
        json_schema.simplify(
            {
                'definitions': {
                    'person': {
                        'type': 'object',
                        'properties': {
                            'name': {'type': 'string'},
                            'children': {
                                'type': 'array',
                                'items': {'$ref': '#/definitions/person'},
                                'default': []
                            }
                        }
                    }
                },

                'type': 'object',

                'properties': {
                    'person': {'$ref': '#/definitions/person'}}})
