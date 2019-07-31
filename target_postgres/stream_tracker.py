from collections import deque
import json
import singer.statediff as statediff
import sys

from target_postgres.exceptions import TargetError


class StreamTracker:
    """
    Object to track the BufferedStream objects for each incoming stream to the target, and the STATE messages coming in. This object understands which streams need to be flushed before STATE messages can be safely emitted and does so.

    Because Singer taps don't have a standard way of expressing which streams correspond to which STATEs, the target can only safely
    emit a STATE message once all the records that came in prior to that STATE in the stream. Because target-postgres buffers
    the records in BufferedSingerStreams, the STATE messages need to be delayed until all the records that came before them have been
    saved to the database from their buffers.
    """

    def __init__(self, target, emit_states):
        self.target = target
        self.emit_states = emit_states

        self.streams = {}
        self.stream_add_watermarks = {}
        self.stream_flush_watermarks = {}

        self.state_queue = deque()  # contains dicts of {'state': <state blob>, 'watermark': number}
        self.message_counter = 0
        self.last_emitted_state = None

    def register_stream(self, stream, buffered_stream):
        self.streams[stream] = buffered_stream
        self.stream_add_watermarks[stream] = 0
        self.stream_flush_watermarks[stream] = 0

    def flushable_streams(self, force):
        flushables = []
        for (stream, stream_buffer) in self.streams.items():
            if force or stream_buffer.buffer_full:
                flushables.append(stream_buffer)
                self.stream_flush_watermarks[stream] = self.stream_add_watermarks[stream]

        return flushables

    def handle_state_message(self, value):
        self.state_queue.append({'state': value, 'watermark': self.message_counter})

    def handle_record_message(self, stream, line_data):
        if stream not in self.streams:
            raise TargetError('A record for stream {} was encountered before a corresponding schema'.format(stream))

        self.message_counter += 1
        self.stream_add_watermarks[stream] = self.message_counter
        self.streams[stream].add_record_message(line_data)

    def emittable_state(self, force):
        # State messages that occured before the least recently flushed record are safe to emit.
        # If they occurred after some records that haven't yet been flushed, they aren't safe to emit.
        # Because records arrive at different rates from different streams, we take the earliest unflushed record as the threshold for what
        # STATE messages are safe to emit.
        if not self.emit_states:
            return None

        all_flushed_watermark = min(self.stream_flush_watermarks.values(), default=0)
        emittable_state = None

        while len(self.state_queue) > 0 and (force or self.state_queue[0]['watermark'] <= all_flushed_watermark):
            emittable_state = self.state_queue.popleft()['state']

        state_to_emit = None

        if emittable_state:
            if len(statediff.diff(emittable_state, self.last_emitted_state or {})) > 0:
                state_to_emit = emittable_state

            self.last_emitted_state = emittable_state

        return state_to_emit
