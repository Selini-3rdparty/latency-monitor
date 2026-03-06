# -*- coding: utf-8 -*-
"""
Derived Metrics Framework
=========================

Pluggable processors that compute derived metrics (jitter, packet loss, etc.)
from raw probe metrics. Sits between the probe queue and the backend queue.
"""

import logging
import time

from latency_monitor.metrics.derived.base import DerivedProcessor
from latency_monitor.metrics.derived.jitter import Jitter
from latency_monitor.metrics.derived.packet_loss import PacketLoss

log = logging.getLogger(__name__)

INTERNAL_METRICS = {"udp.wan.probe_sent", "tcp.wan.probe_sent"}

__derived__ = {
    "jitter": Jitter,
    "packet_loss": PacketLoss,
}


class DerivedMetricsWorker:
    """Bridges the probe queue and backend queue, running derived processors."""

    def __init__(self, processors):
        self.processors = processors

    def start(self, input_q, output_q):
        """Read from input_q, run processors, forward results to output_q."""
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
