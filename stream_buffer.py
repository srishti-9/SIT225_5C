"""
StreamBuffer  

It accepts any number of named channels (three accelerometer axes, a single
temperature sensor, five strain-gauge channels, ...). Producers push samples
with add_sample(); consumers either read the whole window with get_trace(),
or - for genuine incremental graph updates - ask only for what is new since
their last read with get_since().
"""

from collections import deque
from datetime import datetime
import threading


class StreamBuffer:
    def __init__(self, channel_names, maxlen=200,
                 time_format="%Y-%m-%d %H:%M:%S.%f"):
        self.channel_names = list(channel_names)
        self.maxlen = maxlen
        self.time_format = time_format
        self.time_buffer = deque(maxlen=maxlen)
        self.buffers = {name: deque(maxlen=maxlen) for name in self.channel_names}
        self._total = 0          # samples ever added (monotonic sequence number)
        self._lock = threading.Lock()

    def _format_time(self, ts):
        s = ts.strftime(self.time_format)
        # %f gives microseconds; trim to milliseconds
        return s[:-3] if self.time_format.endswith("%f") else s

    def add_sample(self, timestamp=None, **channel_values):
        """Append one synchronised sample across all channels.

        timestamp      : datetime or None (None -> datetime.now())
        channel_values : e.g. add_sample(x=1.2, y=-0.3, z=9.8)
        Channels not supplied are stored as None.
        """
        unknown = set(channel_values) - set(self.channel_names)
        if unknown:
            raise KeyError(f"Unknown channel(s): {sorted(unknown)}")

        t = self._format_time(timestamp or datetime.now())
        with self._lock:
            self.time_buffer.append(t)
            for name in self.channel_names:
                self.buffers[name].append(channel_values.get(name))
            self._total += 1

    def get_trace(self, name):
        """Return (timestamps, values) for one channel - the full window."""
        with self._lock:
            return list(self.time_buffer), list(self.buffers[name])

    def get_since(self, last_seq):
        """Return only the samples added after sequence number `last_seq`.

        Returns (new_seq, timestamps, {channel: values}). Pass new_seq back in
        on the next call. Each consumer (e.g. each browser tab) keeps its own
        cursor, so several consumers can read the same buffer independently.
        If more than `maxlen` samples arrived since the last read, only the
        newest `maxlen` are returned (the older ones have already been dropped).
        """
        with self._lock:
            total = self._total
            if last_seq > total:      # cursor from a previous run - resync
                last_seq = 0
            n_new = min(total - last_seq, len(self.time_buffer))
            if n_new == 0:
                return total, [], {name: [] for name in self.channel_names}
            times = list(self.time_buffer)[-n_new:]
            values = {name: list(buf)[-n_new:]
                      for name, buf in self.buffers.items()}
            return total, times, values