import re


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

    return _get_ref(schema[paths[0]], paths[1:])


def get_ref(schema, ref):
    """
    Given a JSON Schema dict, and a valid ref (`$ref`), get the JSON Schema from within schema
    :param schema: dict, JSON Schema
    :param ref: string
    :return: dict, JSON Schema
    """

    return _get_ref(schema,
                    re.split('/', re.sub('#/', '', ref)))


def is_ref(schema):
    """
    Given a JSON Schema compatible dict, returns True when the schema implements `$ref`

    NOTE: `$ref` OVERRIDES all other keys present in a schema
    :param schema:
    :return:
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


def _helper_simplify(root_schema, child_schema):
    if not child_schema:
        return child_schema

    elif is_ref(child_schema):
        return _helper_simplify(root_schema, get_ref(root_schema, child_schema['$ref']))

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
    """

    return _helper_simplify(schema, schema)
