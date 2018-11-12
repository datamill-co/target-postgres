import re

from jsonschema import Draft4Validator
from jsonschema.exceptions import SchemaError


class JSONSchemaError(Exception):
    """
    Raise this when there is an error with regards to an instance of JSON Schema
    """


def get_type(schema):
    """
    Given a JSON Schema dict, extracts the simplified `type` value
    :param schema: dict, JSON Schema
    :return: [string ...]
    """
    type = schema.get('type', None)
    if not type:
        return ['object']

    if isinstance(type, str):
        return [type]

    return type


def _get_ref(schema, paths):
    if not paths:
        return schema

    if not paths[0] in schema:
        raise JSONSchemaError('`$ref` "{}" not found in provided JSON Schema'.format(paths[0]))

    return _get_ref(schema[paths[0]], paths[1:])


def get_ref(schema, ref):
    """
    Given a JSON Schema dict, and a valid ref (`$ref`), get the JSON Schema from within schema
    :param schema: dict, JSON Schema
    :param ref: string
    :return: dict, JSON Schema
    :raises: Exception
    """

    # Explicitly only allow absolute internally defined $ref's
    if not re.match(r'^#/.*', ref):
        raise JSONSchemaError('Invalid format for `$ref`: "{}"'.format(ref))

    return _get_ref(schema,
                    re.split('/', re.sub(r'^#/', '', ref)))


def is_ref(schema):
    """
    Given a JSON Schema compatible dict, returns True when the schema implements `$ref`

    NOTE: `$ref` OVERRIDES all other keys present in a schema
    :param schema:
    :return: Boolean
    """

    return '$ref' in schema


def is_object(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being an Object.
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return not is_ref(schema) \
           and ('object' in get_type(schema)
                or 'properties' in schema
                or not schema)


def is_iterable(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being iterable (ie, 'array')
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return not is_ref(schema) \
           and 'array' in get_type(schema) \
           and 'items' in schema


def is_nullable(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being 'null'
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return 'null' in get_type(schema)


def _helper_simplify(root_schema, child_schema):
    if not child_schema:
        return child_schema

    elif is_ref(child_schema):
        try:
            return _helper_simplify(root_schema, get_ref(root_schema, child_schema['$ref']))
        except RecursionError:
            raise JSONSchemaError('`$ref` path "{}" is recursive'.format(get_ref(root_schema, child_schema['$ref'])))

    elif is_object(child_schema):
        properties = {}
        for field, field_json_schema in child_schema.get('properties', {}).items():
            properties[field] = _helper_simplify(root_schema, field_json_schema)

        return {'type': get_type(child_schema),
                'properties': properties}

    elif is_iterable(child_schema):
        return {'type': get_type(child_schema),
                'items': _helper_simplify(root_schema, child_schema.get('items', {}))}
    else:
        ret_schema = {'type': get_type(child_schema)}

        if 'format' in child_schema:
            ret_schema['format'] = child_schema.get('format')

        return ret_schema


def simplify(schema):
    """
    Given a JSON Schema compatible dict, returns a simplified JSON Schema dict

    - Expands `$ref` fields to their reference
    - Expands `type` fields into array'ed type fields
    - Strips out all fields which are not `type`/`properties`

    :param schema: dict, JSON Schema
    :return: dict, JSON Schema
    :raises: Exception
    """

    return _helper_simplify(schema, schema)


def _valid_schema_version(schema):
    return '$schema' not in schema \
           or schema['$schema'] == 'http://json-schema.org/draft-04/schema#'


def _unexpected_validation_error(errors, exception):
    """

    :param errors: [String, ...]
    :param exception: Exception
    :return: [String, ...]
    """

    if not errors:
        return ['Unexpected exception encountered: {}'.format(str(exception))]

    return errors


def validation_errors(schema):
    """
    Given a dict, returns any known JSON Schema validation errors. If there are none,
    implies that the dict is a valid JSON Schema.
    :param schema: dict
    :return: [String, ...]
    """

    errors = []

    if not isinstance(schema, dict):
        errors.append('Parameter `schema` is not a dict, instead found: {}'.format(type(schema)))

    try:
        if not _valid_schema_version(schema):
            errors.append('Schema version must be Draft 4. Found: {}'.format('$schema'))
    except Exception as ex:
        errors = _unexpected_validation_error(errors, ex)

    try:
        Draft4Validator.check_schema(schema)
    except SchemaError as error:
        errors.append(str(error))
    except Exception as ex:
        errors = _unexpected_validation_error(errors, ex)

    try:
        simplify(schema)
    except JSONSchemaError as error:
        errors.append(str(error))
    except Exception as ex:
        errors = _unexpected_validation_error(errors, ex)

    return errors


def from_sql(sql_type, nullable):
    _format = None
    if sql_type == 'timestamp with time zone':
        json_type = 'string'
        _format = 'date-time'
    elif sql_type == 'bigint':
        json_type = 'integer'
    elif sql_type == 'double precision':
        json_type = 'number'
    elif sql_type == 'boolean':
        json_type = 'boolean'
    elif sql_type == 'text':
        json_type = 'string'
    else:
        raise Exception('Unsupported type `{}` in existing target table'.format(sql_type))

    json_type = [json_type]
    if nullable:
        json_type.append('null')

    ret_json_schema = {'type': json_type}
    if _format:
        ret_json_schema['format'] = _format

    return ret_json_schema


def to_sql(schema):
    _type = get_type(schema)
    not_null = True
    ln = len(_type)
    if ln == 1:
        _type = _type[0]
    if ln == 2 and 'null' in _type:
        not_null = False
        if _type.index('null') == 0:
            _type = _type[1]
        else:
            _type = _type[0]
    elif ln > 2:
        raise Exception('Multiple types per column not supported')

    sql_type = 'text'

    if 'format' in schema and \
            schema['format'] == 'date-time' and \
            _type == 'string':
        sql_type = 'timestamp with time zone'
    elif _type == 'boolean':
        sql_type = 'boolean'
    elif _type == 'integer':
        sql_type = 'bigint'
    elif _type == 'number':
        sql_type = 'double precision'

    if not_null:
        sql_type += ' NOT NULL'

    return sql_type
