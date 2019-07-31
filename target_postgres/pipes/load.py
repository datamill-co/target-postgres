import json

from target_postgres import json_schema
from target_postgres.exceptions import InputError
from target_postgres.globals import LOGGER


def load(iterable):
    for line in iterable:
        try:
            line_data = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse JSON: {}".format(line))
            raise

        if 'type' not in line_data:
            raise InputError('`type` is a required key: {}'.format(line))

        if line_data['type'] == 'SCHEMA':
            if 'stream' not in line_data:
                raise InputError('`stream` is a required key: {}'.format(line))

            if 'schema' not in line_data:
                raise InputError('`schema` is a required key: {}'.format(line))

            schema_validation_errors = json_schema.validation_errors(line_data['schema'])
            if schema_validation_errors:
                raise InputError('`schema` is an invalid JSON Schema instance: {}'.format(line),
                                 *schema_validation_errors)

        elif line_data['type'] == 'RECORD':
            if 'stream' not in line_data:
                raise InputError('`stream` is a required key: {}'.format(line))

        elif line_data['type'] == 'ACTIVATE_VERSION':
            if 'stream' not in line_data:
                raise InputError('`stream` is a required key: {}'.format(line))

            if 'version' not in line_data:
                raise InputError('`version` is a required key: {}'.format(line))

        elif line_data['type'] == 'STATE':
            pass

        else:
            raise InputError('Unknown message type {} in message {}'.format(
                line_data['type'],
                line))

        yield line_data
