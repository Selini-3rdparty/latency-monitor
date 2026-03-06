# -*- coding: utf-8 -*-
"""
Derived Metrics Framework
=========================

Pluggable processors that compute derived metrics (jitter, packet loss, etc.)
from raw probe metrics. Sits between the probe queue and the backend queue.
"""

import logging
import time

log = logging.getLogger(__name__)

INTERNAL_METRICS = {"udp.wan.probe_sent", "tcp.wan.probe_sent"}


class DerivedProcessor:
    """Base class for derived metric processors."""

    subscribes_to = []
    name = ""

    def __init__(self, send_interval, **proc_opts):
        self.proc_opts = proc_opts
        self.window = proc_opts.get("window", send_interval)
        self.last_flush = time.time()

    def should_flush(self):
        return time.time() - self.last_flush > self.window

    def process(self, metric):
        return []

    def flush(self):
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


class DerivedMetricsWorker:
    """Bridges the probe queue and backend queue, running derived processors."""

    def __init__(self, processors):
        self.processors = processors

    def start(self, input_q, output_q):
        log.info(
            "Starting DerivedMetricsWorker with %d processor(s)",
            len(self.processors),
        )
        while True:
            metric = input_q.get()
            log.debug("[DerivedMetrics] Received metric: %s", metric["metric"])

            if metric["metric"] not in INTERNAL_METRICS:
                output_q.put(metric)

            for proc in self.processors:
                if metric["metric"] in proc.subscribes_to:
                    for derived in proc.process(metric):
                        output_q.put(derived)

                if proc.should_flush():
                    for derived in proc.flush():
                        log.debug(
                            "[DerivedMetrics] Flushing %s: %s",
                            proc.name,
                            derived["metric"],
                        )
                        output_q.put(derived)

            time.sleep(0.01)


from latency_monitor.metrics.derived.jitter import Jitter  # noqa: E402
from latency_monitor.metrics.derived.packet_loss import PacketLoss  # noqa: E402

__derived__ = {
    "jitter": Jitter,
    "packet_loss": PacketLoss,
}
