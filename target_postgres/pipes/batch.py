from target_postgres.exceptions import TargetError
from target_postgres.globals import LOGGER
from target_postgres.singer_stream import BufferedSingerStream
from target_postgres.stream_tracker import StreamTracker


def _report_invalid_records(streams):
    for stream_buffer in streams.values():
        if stream_buffer.peek_invalid_records():
            LOGGER.warning("Invalid records detected for stream {}: {}".format(
                stream_buffer.stream,
                stream_buffer.peek_invalid_records()
            ))


def _stream_buffer_to_batch(stream_buffer):
    x = {'type': '__DataMill__BATCH',
         'stream': stream_buffer.stream,
         'schema': stream_buffer.schema,
         'key_properties': stream_buffer.key_properties,
         'max_version': stream_buffer.max_version,
         'records': stream_buffer.get_batch()}
    stream_buffer.flush_buffer()
    return x


def _line_handler(state_tracker,
                  invalid_records_detect, invalid_records_threshold, max_batch_rows, max_batch_size,
                  line_data):
    if line_data['type'] == 'SCHEMA':
        stream = line_data['stream']

        schema = line_data['schema']

        key_properties = line_data.get('key_properties', None)

        if stream not in state_tracker.streams:
            buffered_stream = BufferedSingerStream(stream,
                                                   schema,
                                                   key_properties,
                                                   invalid_records_detect=invalid_records_detect,
                                                   invalid_records_threshold=invalid_records_threshold)
            if max_batch_rows:
                buffered_stream.max_rows = max_batch_rows
            if max_batch_size:
                buffered_stream.max_buffer_size = max_batch_size

            state_tracker.register_stream(stream, buffered_stream)
        else:
            state_tracker.streams[stream].update_schema(schema, key_properties)

    elif line_data['type'] == 'RECORD':
        state_tracker.handle_record_message(line_data['stream'], line_data)

    elif line_data['type'] == 'ACTIVATE_VERSION':
        if line_data['stream'] not in state_tracker.streams:
            raise TargetError('A ACTIVATE_VERSION for stream {} was encountered before a corresponding schema'
                              .format(line_data['stream']))

        stream_buffer = state_tracker.streams[line_data['stream']]
        return [_stream_buffer_to_batch(stream_buffer), line_data]

    elif line_data['type'] == 'STATE':
        state_tracker.new_handle_state_message(line_data)
        state = state_tracker.emittable_state(False)
        if state:
            return [state]

    return []


def batch(config, iterable):
    state_support = config.get('state_support', True)
    state_tracker = StreamTracker(None, state_support)

    try:
        invalid_records_detect = config.get('invalid_records_detect')
        invalid_records_threshold = config.get('invalid_records_threshold')
        max_batch_rows = config.get('max_batch_rows')
        max_batch_size = config.get('max_batch_size')
        batch_detection_threshold = config.get('batch_detection_threshold', 5000)

        line_count = 0
        for line_data in iterable:
            for x in _line_handler(state_tracker,
                                   invalid_records_detect,
                                   invalid_records_threshold,
                                   max_batch_rows,
                                   max_batch_size,
                                   line_data):
                yield x

            if line_count > 0 and line_count % batch_detection_threshold == 0:
                for stream in state_tracker.flushable_streams(False):
                    yield _stream_buffer_to_batch(stream)
                state = state_tracker.emittable_state(False)
                if state:
                    yield state
            line_count += 1

        for stream in state_tracker.flushable_streams(True):
            yield _stream_buffer_to_batch(stream)
        state = state_tracker.emittable_state(True)
        if state:
            yield state

    finally:
        _report_invalid_records(state_tracker.streams)
