"""Bounded rolling stage samples. Local durations use a monotonic clock."""
import math
import time
from collections import defaultdict, deque
from contextlib import contextmanager


class Timings:
    def __init__(self, capacity=4096):
        self.samples = defaultdict(lambda: deque(maxlen=capacity))
        self.counts = defaultdict(int)
        self.totals = defaultdict(float)

    def observe(self, stage, milliseconds):
        if math.isfinite(milliseconds) and milliseconds >= 0:
            self.samples[stage].append(milliseconds)
            self.counts[stage] += 1
            self.totals[stage] += milliseconds

    @contextmanager
    def measure(self, stage):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.observe(stage, (time.perf_counter() - started) * 1000)

    def snapshot(self):
        result = {}
        for stage, samples in self.samples.items():
            values = sorted(samples)
            result[stage] = {
                "count": self.counts[stage], "window_count": len(values),
                "total_ms": self.totals[stage],
                **{f"p{int(p * 100)}_ms": values[math.ceil(len(values) * p) - 1]
                   for p in (.5, .95, .99)},
            }
        return result


timings = Timings()
