import re

import pytest

from target_postgres import json_schema
from fixtures import CATS_SCHEMA


def test_is_object():
    assert json_schema.is_object({'type': ['object']})
    assert json_schema.is_object({'properties': {}})
    assert json_schema.is_object({})


def test_is_iterable():
    assert json_schema.is_iterable({'type': 'array', 'items': {'type': 'integer'}})
    assert json_schema.is_iterable({'type': ['array'], 'items': {'type': ['boolean']}})


def test_is_nullable():
    assert json_schema.is_iterable({'type': ['array', 'null'], 'items': {'type': ['boolean']}})
    assert not json_schema.is_iterable({'type': ['string']})
    assert not json_schema.is_nullable({})


def test_is_literal():
    assert json_schema.is_literal({'type': ['integer', 'null']})
    assert json_schema.is_literal({'type': ['string']})
    assert not json_schema.is_literal({'type': ['array'], 'items': {'type': ['boolean']}})
    assert not json_schema.is_literal({})


def test_complex_objects__logical_statements():
    every_type = {
        'type': ['null', 'integer', 'number', 'boolean', 'string', 'array', 'object'],
        'items': {'type': 'integer'},
        'format': 'date-time',
        'properties': {
            'a': {'type': 'integer'},
            'b': {'type': 'number'},
            'c': {'type': 'boolean'}
        }
    }

    assert json_schema.is_iterable(every_type)
    assert json_schema.is_nullable(every_type)
    assert json_schema.is_iterable(every_type)
    assert json_schema.is_object(every_type)


def test_simplify__empty_becomes_object():
    assert json_schema.simplify({}) == {'properties': {}, 'type': ['object']}


def test_simplify__types_into_arrays():
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


def test_simplify__complex():
    assert \
        json_schema.simplify({
            'properties': {
                'every_type': {
                    'type': ['null', 'integer', 'number', 'boolean', 'string', 'array', 'object'],
                    'items': {'type': 'integer'},
                    'format': 'date-time',
                    'properties': {
                        'i': {'type': 'integer'},
                        'n': {'type': 'number'},
                        'b': {'type': 'boolean'}
                    }
                }
            }
        }) \
        == {
            'type': ['object'],
            'properties': {
                'every_type': {
                    'type': ['null', 'integer', 'number', 'boolean', 'string', 'array', 'object'],
                    'items': {'type': ['integer']},
                    'format': 'date-time',
                    'properties': {
                        'i': {'type': ['integer']},
                        'n': {'type': ['number']},
                        'b': {'type': ['boolean']}
                    }
                }
            }
        }

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
                'paw_size': {
                    'type': ['integer'],
                    'default': 314159
                },
                'paw_colour': {
                    'type': ['string'],
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


def test_simplify__refs():
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


def test_simplify__refs__invalid_format():
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


def test_simplify__refs__missing():
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


def test_simplify__refs__circular():
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


def test_validation_errors():
    assert json_schema.validation_errors({}) \
           == []

    assert json_schema.validation_errors({'type': 'null'}) \
           == []

    assert json_schema.validation_errors({'type': ['object'],
                                          'properties': {
                                              'a': {'type': 'string'}}}) \
           == []

    assert json_schema.validation_errors(
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
           == []


def _non_string_elements(x):
    """
    Simple helper to check that all values of x are string. Returns all non string elements as (position, element).
    :param x: Iterable
    :return: [(int, !String), ...]
    """

    problems = []
    for i in range(0, len(x)):
        if not isinstance(x[i], str):
            problems.append((i, x[i]))
    return problems


def test_validation_errors__invalid_objects():
    def _invalid_type_ex(ret):
        return re.match(r'.*not a dict.*', ret[0])

    non_dict_object__int = json_schema.validation_errors(12345)
    assert _invalid_type_ex(non_dict_object__int)
    assert not _non_string_elements(non_dict_object__int)

    non_dict_object__string = json_schema.validation_errors('woah no')
    assert _invalid_type_ex(non_dict_object__string)
    assert not _non_string_elements(non_dict_object__string)

    non_dict_object__tuple = json_schema.validation_errors(('hello', 'world'))
    assert _invalid_type_ex(non_dict_object__tuple)
    assert not _non_string_elements(non_dict_object__tuple)

    non_dict_object__list = json_schema.validation_errors([1, 2, 3])
    assert _invalid_type_ex(non_dict_object__list)
    assert not _non_string_elements(non_dict_object__list)


def test_validation_errors__invalid_schemas():
    invalid_type_test = json_schema.validation_errors({'type': 'well this should not work'})
    assert invalid_type_test
    assert not _non_string_elements(invalid_type_test)

    recursive_schema_test = json_schema.validation_errors({
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

    assert recursive_schema_test
    assert not _non_string_elements(recursive_schema_test)

    non_standard_schema_version = json_schema.validation_errors({'$schema': 'clearly not a valid schema version'})
    assert non_standard_schema_version
    assert not _non_string_elements(non_standard_schema_version)


def test_validation_errors__invalid_draft_version():
    draft_3 = json_schema.validation_errors({'$schema': 'http://json-schema.org/draft-03/schema#'})
    assert draft_3
    assert not _non_string_elements(draft_3)

    draft_6 = json_schema.validation_errors({'$schema': 'http://json-schema.org/draft-06/schema#'})
    assert draft_6
    assert not _non_string_elements(draft_6)

    draft_7 = json_schema.validation_errors({'$schema': 'http://json-schema.org/draft-07/schema#'})
    assert draft_7
    assert not _non_string_elements(draft_7)


def test_make_nullable():
    assert {'type': ['boolean', 'null']} \
           == json_schema.make_nullable({'type': 'boolean'})
    assert {'type': ['null', 'boolean']} \
           == json_schema.make_nullable({'type': ['null', 'boolean']})
    assert {'type': ['null', 'string']} \
           == json_schema.make_nullable({'type': ['null', 'string']})

    ## Make sure we're not modifying the original
    schema = {'type': ['string']}
    assert json_schema.get_type(schema) == ['string']
    assert {'type': ['string', 'null']} \
           == json_schema.make_nullable(schema)
    assert json_schema.get_type(schema) == ['string']

    assert {
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
               'type': ['object', 'null'],
               'properties': {
                   'billing_address': {'$ref': '#/definitions/address'},
                   'shipping_address': {'$ref': '#/definitions/address'}}} \
           == json_schema.make_nullable(
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
                'shipping_address': {'$ref': '#/definitions/address'}}})


def test_sql_shorthand():
    assert 'b' == json_schema.sql_shorthand({'type': 'boolean'})
    assert 'b' == json_schema.sql_shorthand({'type': ['null', 'boolean']})
    assert 's' == json_schema.sql_shorthand({'type': ['null', 'string']})
