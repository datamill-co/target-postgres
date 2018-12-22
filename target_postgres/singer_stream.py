from jsonschema import Draft4Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from target_postgres.pysize import get_size


class SingerStreamError(Exception):
    """
    Raise when there is an Exception with Singer Streams.
    """


SINGER_RECEIVED_AT = '_sdc_received_at'
SINGER_BATCHED_AT = '_sdc_batched_at'
SINGER_SEQUENCE = '_sdc_sequence'
SINGER_TABLE_VERSION = '_sdc_table_version'
SINGER_PK = '_sdc_primary_key'
SINGER_SOURCE_PK_PREFIX = '_sdc_source_key_'
SINGER_LEVEL = '_sdc_level_{}_id'
SINGER_VALUE = '_sdc_value'


class BufferedSingerStream():
    def __init__(self,
                 stream,
                 schema,
                 key_properties,
                 *args,
                 invalid_records_detect=None,
                 invalid_records_threshold=None,
                 max_rows=200000,
                 max_buffer_size=104857600,  # 100MB
                 **kwargs):
        """
        :param invalid_records_detect: Defaults to True when value is None
        :param invalid_records_threshold: Defaults to 0 when value is None
        """
        self.update_schema(schema, key_properties)
        self.stream = stream
        self.invalid_records = []
        self.max_rows = max_rows
        self.max_buffer_size = max_buffer_size

        self.invalid_records_detect = invalid_records_detect
        self.invalid_records_threshold = invalid_records_threshold

        if self.invalid_records_detect is None:
            self.invalid_records_detect = True
        if self.invalid_records_threshold is None:
            self.invalid_records_threshold = 0

        self.__buffer = []
        self.__count = 0
        self.__size = 0
        self.__lifetime_max_version = None

    def update_schema(self, schema, key_properties):
        self.validator = Draft4Validator(schema, format_checker=FormatChecker())

        if len(key_properties) == 0:
            self.use_uuid_pk = True
            key_properties = [SINGER_PK]
            schema['properties'][SINGER_PK] = {
                'type': 'string'
            }
        else:
            self.use_uuid_pk = False

        self.schema = schema
        self.key_properties = key_properties

    @property
    def count(self):
        return self.__count

    @property
    def buffer_full(self):
        if self.__count >= self.max_rows:
            return True

        if self.__count > 0:
            if self.__size >= self.max_buffer_size:
                return True

        return False

    def __update_version(self, version):
        if version is None or (self.__lifetime_max_version is not None and self.__lifetime_max_version >= version):
            return None

        ## TODO: log warning about earlier records detected

        self.flush_buffer()
        self.__lifetime_max_version = version

    def add_record_message(self, record_message):
        add_record = True

        self.__update_version(record_message.get('version'))

        if self.__lifetime_max_version != record_message.get('version'):
            return None

        try:
            self.validator.validate(record_message['record'])
        except ValidationError as error:
            add_record = False
            self.invalid_records.append((error, record_message))

        if add_record:
            self.__buffer.append(record_message)
            self.__size += get_size(record_message)
            self.__count += 1
        elif self.invalid_records_detect \
                and len(self.invalid_records) >= self.invalid_records_threshold:
            raise SingerStreamError(
                'Invalid records detected above threshold: {}. See `.args` for details.'.format(
                    self.invalid_records_threshold),
                self.invalid_records)

    def peek_buffer(self):
        return self.__buffer

    def flush_buffer(self):
        _buffer = self.__buffer
        self.__buffer = []
        self.__size = 0
        self.__count = 0
        return _buffer

    def peek_invalid_records(self):
        return self.invalid_records
