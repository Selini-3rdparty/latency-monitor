# -*- coding: utf-8 -*-
"""
Base class for derived metric processors.
"""

import logging
import time

log = logging.getLogger(__name__)


class DerivedProcessor:
    """Base class for derived metric processors.

    Subclasses set ``subscribes_to`` and ``name``, then override
    ``process()`` and/or ``flush()`` to emit derived metrics.
    """

    subscribes_to = []
    name = ""

    def __init__(self, send_interval, **proc_opts):
        self.proc_opts = proc_opts
        self.window = proc_opts.get("window", send_interval)
        self.last_flush = time.time()

    def should_flush(self):
        """Return True when the processor window has elapsed."""
        return time.time() - self.last_flush > self.window

    def process(self, metric):  # pylint: disable=unused-argument
        """Process an incoming metric. Return list of derived metrics."""
        return []

    def flush(self):
        """Flush windowed state. Return list of derived metrics."""
        self.last_flush = time.time()
        return []

    def _build_metric(self, source_metric, suffix, points, tags):
        """Build a derived metric dict with standardized naming."""
        name = f"{source_metric}.{self.name}"
        if suffix:
            name = f"{name}.{suffix}"
        return {
            "metric": name,
            "points": points,
            "tags": list(tags),
        }
