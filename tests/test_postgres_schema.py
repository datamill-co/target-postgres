import re

import pytest

from target_postgres import postgres_schema


def test_canonicalize_column_name():
    for normal_name in ['test', 'test123', 'test_12_3']:
        assert normal_name == postgres_schema.canonicalize_column_name(normal_name)


def test_canonicalize_column_name__error__empty():
    with pytest.raises(postgres_schema.SchemaError):
        postgres_schema.canonicalize_column_name('')


def test_canonicalize_column_name__error__non_alpha_underscore():
    with pytest.raises(postgres_schema.SchemaError):
        postgres_schema.canonicalize_column_name('123abcd')


def test_canonicalize_column_name__error__length():
    with pytest.raises(postgres_schema.SchemaError):
        postgres_schema.canonicalize_column_name('a' * (postgres_schema.NAMEDATALEN + 1))

    with pytest.raises(postgres_schema.SchemaError):
        postgres_schema.canonicalize_column_name('Z' * (postgres_schema.NAMEDATALEN + 10))


def test_canonicalize_column_name__capitalization():
    assert re.match(r'[a-z]+', postgres_schema.canonicalize_column_name('ABCDHIJK'))


def test_canonicalize_column_name__replacement():
    assert '_rep$lac___ement_test' == postgres_schema.canonicalize_column_name('_REP$lac!!!EMENT-test')
