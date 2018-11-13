from copy import deepcopy
import re

from target_postgres.singer_stream import (
    SINGER_RECEIVED_AT,
    SINGER_BATCHED_AT,
    SINGER_SEQUENCE,
    SINGER_TABLE_VERSION,
    SINGER_PK,
    SINGER_SOURCE_PK_PREFIX,
    SINGER_LEVEL
)

"""
NAMEDATALEN _defaults_ to 64 in PostgreSQL. The maxmimum length for an identifier is
  NAMEDATALEN - 1.

  TODO: Figure out way to `SELECT` value from commands
"""
NAMEDATALEN = 63


class SchemaError(Exception):
    """
    Raise this when there is an error with regards to Postgres Schemas
    """


def canonicalize_column_name(column_name):
    """
    Given a column name, canonicalize name such that it can be
    used as a Postgres column identifier.

    For details or a full list of restrictions, see: https://www.postgresql.org/docs/9.4/sql-syntax-lexical.html

    TODO: allow for non latin characters
    :param column_name: String
    :return: String
    """

    if not re.match(r'^[a-zA-Z_]', column_name):
        raise SchemaError(
            'Field "{}" cannot be canonicalized. Must start with an letter, or underscore'.format(
                column_name))

    if len(column_name) > NAMEDATALEN:
        raise SchemaError(
            'Field "{}" cannot be canonicalized. Length {} must be less than or equal to {}'.format(
                column_name,
                len(column_name),
                NAMEDATALEN))

    ## Subsequent characters in an identifier or key word can be letters, underscores, digits (0-9), or dollar signs ($)

    lowered_name = column_name.lower()
    return lowered_name[0] + re.sub(r'[^\w\d_$]', '_', lowered_name[1:])


def add_singer_columns(schema, key_properties):
    ret_schema = deepcopy(schema)
    properties = ret_schema['properties']

    if SINGER_RECEIVED_AT not in properties:
        properties[SINGER_RECEIVED_AT] = {
            'type': ['null', 'string'],
            'format': 'date-time'
        }

    if SINGER_SEQUENCE not in properties:
        properties[SINGER_SEQUENCE] = {
            'type': ['null', 'integer']
        }

    if SINGER_TABLE_VERSION not in properties:
        properties[SINGER_TABLE_VERSION] = {
            'type': ['null', 'integer']
        }

    if SINGER_BATCHED_AT not in properties:
        properties[SINGER_BATCHED_AT] = {
            'type': ['null', 'string'],
            'format': 'date-time'
        }

    if len(key_properties) == 0:
        properties[SINGER_PK] = {
            'type': ['string']
        }

    return ret_schema
