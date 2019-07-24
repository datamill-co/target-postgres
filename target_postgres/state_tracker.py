import json
import sys

class StateTracker:
  def __init__(self, target, emit_states):
    self.target = target
    self.emit_states = emit_states

    self.streams = {}
    self.last_emitted_state = None
    self.state_queue = []

  def flush_streams(self, force=False):
    for stream_buffer in self.streams.values():
        if force or stream_buffer.buffer_full:
          self.target.write_batch(stream_buffer)
          stream_buffer.flush_buffer()


  def emit_state(self, value):
    if self.emit_states:
      line = json.dumps(value)
      sys.stdout.write("{}\n".format(line))
      sys.stdout.flush()