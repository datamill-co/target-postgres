from copy import deepcopy
import decimal
import json
import re

from jsonschema import Draft4Validator
from jsonschema.exceptions import SchemaError
from target_postgres.exceptions import JSONSchemaError

NULL = 'null'
OBJECT = 'object'
ARRAY = 'array'
INTEGER = 'integer'
NUMBER = 'number'
BOOLEAN = 'boolean'
STRING = 'string'
DATE_TIME_FORMAT = 'date-time'

_PYTHON_TYPE_TO_JSON_SCHEMA = {
    int: INTEGER,
    float: NUMBER,
    bool: BOOLEAN,
    str: STRING,
    type(None): NULL,
    decimal.Decimal: NUMBER
}


def python_type(x):
    """
    Given a value `x`, return its Python Type as a JSONSchema type.
    :param x:
    :return:
    """
    if not type(x) in _PYTHON_TYPE_TO_JSON_SCHEMA:
        raise JSONSchemaError('Unknown type `{}`. Cannot translate to JSONSchema type.'.format(
            str(type(x))
        ))
    return _PYTHON_TYPE_TO_JSON_SCHEMA[type(x)]


def get_type(schema):
    """
    Given a JSON Schema dict, extracts the simplified `type` value
    :param schema: dict, JSON Schema
    :return: [string ...]
    """
    t = schema.get('type', None)
    if not t:
        return [OBJECT]

    if isinstance(t, str):
        return [t]

    return deepcopy(t)


def simple_type(schema):
    """
    Given a JSON Schema dict, extracts the simplified schema, ie, a schema which can only represent
    _one_ of the given types allowed (along with the Nullable modifier):
    - OBJECT
    - ARRAY
    - INTEGER
    - NUMBER
    - BOOLEAN
    - STRING
    - DATE_TIME

    :param schema: dict, JSON Schema
    :return: dict, JSON Schema
    """
    t = get_type(schema)

    if is_datetime(schema):
        return {'type': t,
                'format': DATE_TIME_FORMAT}

    return {'type': t}


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


def _is_ref(schema):
    """
    Given a JSON Schema compatible dict, returns True when the schema implements `$ref`

    NOTE: `$ref` OVERRIDES all other keys present in a schema
    :param schema:
    :return: Boolean
    """

    return '$ref' in schema


def _is_allof(schema):
    """
    Given a JSON Schema compatible dict, returns True when the schema implements `allOf`.

    :param schema:
    :return: Boolean
    """

    return not _is_ref(schema) and 'allOf' in schema


def is_anyof(schema):
    """
    Given a JSON Schema compatible dict, returns True when the schema implements `anyOf`.

    :param schema:
    :return: Boolean
    """

    return not _is_ref(schema) and not _is_allof(schema) and 'anyOf' in schema


def is_object(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being an Object.
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return not _is_ref(schema) and not is_anyof(schema) and not _is_allof(schema) \
           and (OBJECT in get_type(schema)
                or 'properties' in schema
                or not schema)


def is_iterable(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being iterable (ie, 'array')
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return not _is_ref(schema) \
           and ARRAY in get_type(schema) \
           and 'items' in schema


def is_nullable(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being 'null'
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return NULL in get_type(schema)


def is_literal(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being a literal
    (ie, 'integer', 'number', etc.)
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return not {STRING, INTEGER, NUMBER, BOOLEAN}.isdisjoint(set(get_type(schema)))


def is_datetime(schema):
    """
    Given a JSON Schema compatible dict, returns True when schema's type allows being a date-time
    :param schema: dict, JSON Schema
    :return: Boolean
    """

    return STRING in get_type(schema) and schema.get('format') == DATE_TIME_FORMAT


def make_nullable(schema):
    """
    Given a JSON Schema dict, returns the dict but makes the `type` `null`able.
    `is_nullable` will return true on the output.
    :return: dict, JSON Schema
    """
    t = get_type(schema)
    if NULL in t:
        return schema

    ret_schema = deepcopy(schema)
    ret_schema['type'] = t + [NULL]
    return ret_schema


class Cachable(dict):
    '''
    The simplified json_schemas we produce are idempotent. ie, if you simplify a simplified
    json_schema, it will return the same thing. We wrap the `dict` object with a few
    helpers which extend it so that we avoid recursion in some instances.
    '''
    def __init__(self, raw_dict, simplified=True):
        self._c = None
        super(Cachable, self).__init__(self, **raw_dict)

    def __hash__(self):
        return self._comparator().__hash__()

    def deepcopy(self):
        s = deepcopy(self)
        s._c = self._c
        return s

    def _comparator(self):
        if not self._c:
            self._c = json.dumps(self, sort_keys=True)

        return self._c

    def __lt__(self, other):
        return self._comparator() < other._comparator()


def _allof_sort_key(schema):
    '''
    We prefer scalars over combinations.
    With scalars we prefer date-times over strings.
    With combinations, we prefer objects.
    With all, we prefer nullables.
    '''
    if is_nullable(schema):
        sort_value = 0
    else:
        sort_value = 1

    if is_datetime(schema):
        sort_value += 0
    elif is_literal(schema):
        sort_value += 10
    elif is_object(schema):
        sort_value += 100
    elif is_iterable(schema):
        sort_value += 200
    else:
        # Unknown schema...maybe a $ref?
        sort_value += 1000

    return sort_value


def _simplify__allof__merge__objects(schemas):
    ret_schema = schemas[0]
    # Merge objects together preferring later allOfs over earlier
    next_schemas = schemas[1:]
    while next_schemas and is_object(next_schemas[0]):
        ret_schema['properties'] = {
            **ret_schema.get('properties', {}),
            **next_schemas[0].get('properties', {})}

        next_schemas = next_schemas[1:]

    return ret_schema


def _simplify__allof__merge__iterables(root_schema, schemas):
    ret_schema = schemas[0]
    # Recurse on all of the item schemas to create a single item schema
    item_schemas = []

    next_schemas = schemas
    while next_schemas and is_iterable(next_schemas[0]):
        item_schemas.append(next_schemas[0]['items'])

        next_schemas = next_schemas[1:]

    ret_schema['items'] = _helper_simplify(root_schema, {'allOf': item_schemas})
    return ret_schema


def _simplify__allof(root_schema, child_schema):
    simplified_schemas = [
        _helper_simplify(root_schema, schema)
        for schema in child_schema['allOf']]
    schemas = sorted(simplified_schemas, key=_allof_sort_key)

    ret_schema = schemas[0]

    if is_object(ret_schema):
        return _simplify__allof__merge__objects(schemas)

    if is_iterable(ret_schema):
        return _simplify__allof__merge__iterables(root_schema, schemas)

    return ret_schema


def _simplify__implicit_anyof(root_schema, schema):
    '''
    Typically literals are simple and have at most two types, one of which being NULL.
    However, they _can_ have many types wrapped up inside them as an implicit `anyOf`.

    Since we support `anyOf`, it is simpler to unwrap and "flatten" this implicit
    combination type.
    '''
    schemas = []
    types = set(get_type(schema))

    if types == {NULL}:
        return Cachable({'type': [NULL]})

    types.discard(NULL)

    if is_datetime(schema):
        schemas.append(Cachable({
            'type': [STRING],
            'format': DATE_TIME_FORMAT
        }))

        types.remove(STRING)

    if is_object(schema):
        properties = {}
        for field, field_json_schema in schema.get('properties', {}).items():
            properties[field] = _helper_simplify(root_schema, field_json_schema)

        schemas.append({
            'type': [OBJECT],
            'properties': properties
        })

        types.discard(OBJECT)

    if is_iterable(schema):
        schemas.append({
            'type': [ARRAY],
            'items': _helper_simplify(root_schema, schema.get('items', {}))
        })

        types.remove(ARRAY)

    schemas += [{'type': [t]} for t in types]

    if is_nullable(schema):
        schemas = [make_nullable(s) for s in schemas]


    return _helper_simplify(root_schema, {'anyOf': [Cachable(s) for s in schemas]})


def _simplify__anyof(root_schema, schema):
    '''
    `anyOf` clauses are merged/simplified according to the following rules (these _are_ recursive):

    - all literals are dedupped
    - all objects are merged into the same object schema, with sub-schemas being grouped as simplified `anyOf` schemas
    - all iterables' `items` schemas are merged as simplified `anyOf` schemas
    - all `anyOf`s are flattened to the topmost
    - if there is only a single element in an `anyOf`, that is denested
    - if any `anyOf`s are nullable, all are nullable
    '''

    schemas = [
            _helper_simplify(root_schema, schema)
            for schema in schema['anyOf']]

    literals = set()
    any_nullable = False
    any_merged_objects = False
    merged_object_properties = {}
    any_merged_iters = False
    merged_item_schemas = []

    while schemas:
        sub_schema = schemas.pop()
        any_nullable = any_nullable or is_nullable(sub_schema)

        if is_literal(sub_schema):
            literals.add(sub_schema)

        elif is_anyof(sub_schema):
            # Flatten potentially deeply nested `anyOf`s
            schemas += sub_schema['anyOf']

        elif is_object(sub_schema):
            any_merged_objects = True
            for k, s in sub_schema.get('properties', {}).items():
                if k in merged_object_properties:
                    merged_object_properties[k].append(s)
                else:
                    merged_object_properties[k] = [s]

        elif is_iterable(sub_schema):
            any_merged_iters = True
            merged_item_schemas.append(sub_schema['items'])

    merged_schemas = set()
    for l in literals:
        s = l
        if any_nullable:
            s = make_nullable(l)

        merged_schemas.add(Cachable(s))

    if any_merged_objects:
        for k, v in merged_object_properties.items():
            merged_object_properties[k] = _helper_simplify(root_schema, {'anyOf': v})

        s = {
            'type': [OBJECT],
            'properties': merged_object_properties
        }

        if any_nullable:
            s = make_nullable(s)

        merged_schemas.add(Cachable(s))

    if any_merged_iters:
        merged_item_schemas = _helper_simplify(root_schema, {'anyOf': merged_item_schemas})

        s = {
            'type': [ARRAY],
            'items': merged_item_schemas
        }

        if any_nullable:
            s = make_nullable(s)

        merged_schemas.add(Cachable(s))

    if len(merged_schemas) == 1:
        return merged_schemas.pop()

    return Cachable({'anyOf': sorted(merged_schemas)})


def _helper_simplify(root_schema, child_schema):
    # We check this value to make simplify a noop for schemas which have _already_ been simplified
    if isinstance(child_schema, Cachable):
        return child_schema

    ## Refs override all other type definitions
    if _is_ref(child_schema):
        try:
            ret_schema = _helper_simplify(root_schema, get_ref(root_schema, child_schema['$ref']))

        except RecursionError:
            raise JSONSchemaError('`$ref` path "{}" is recursive'.format(get_ref(root_schema, child_schema['$ref'])))

    elif _is_allof(child_schema):
        ret_schema = _simplify__allof(root_schema, child_schema)

    elif is_anyof(child_schema):
        ret_schema = _simplify__anyof(root_schema, child_schema)

    else:
        ret_schema = _simplify__implicit_anyof(root_schema, child_schema)

    if 'default' in child_schema:
        ret_schema['default'] = child_schema.get('default')

    return Cachable(ret_schema)


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
    if isinstance(schema, Cachable):
        return schema.deepcopy()

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


_shorthand_mapping = {
    NULL: '',
    'string': 's',
    'number': 'f',
    'integer': 'i',
    'boolean': 'b',
    'date-time': 't'
}


def _type_shorthand(type_s):
    if isinstance(type_s, list):
        shorthand = ''
        for t in sorted(type_s):
            shorthand += _type_shorthand(t)
        return shorthand

    if not type_s in _shorthand_mapping:
        raise JSONSchemaError('Shorthand not available for type {}. Expected one of {}'.format(
            type_s,
            list(_shorthand_mapping.keys())
        ))

    return _shorthand_mapping[type_s]


def shorthand(schema):
    t = deepcopy(get_type(schema))

    if 'format' in schema and 'date-time' == schema['format'] and STRING in t:
        t.remove(STRING)
        t.append('date-time')

    return _type_shorthand(t)
