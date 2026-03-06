# -*- coding: utf-8 -*-
"""
Packet Loss Processor
=====================

Computes packet loss percentage and counts per direction over a time window.

Modes:
- "rtt": RTT-based (round-trip loss from client-side status counters)
- "owd": OWD-based (per-direction, cadence-based: expected vs received probes)
- "both": emit both RTT and OWD loss metrics (default)

OWD loss is computed at the receiving side by comparing the number of OWD
metrics actually received against the expected count based on the probe
interval and window duration. No cross-instance communication needed.

Loss percentage is stored as basis points (pct * 100) for integer precision
in ILP. E.g., 250 = 2.50%. Grafana divides by 100 for display.
"""

import time

from latency_monitor.metrics.derived.base import DerivedProcessor


class PacketLoss(DerivedProcessor):
    """Packet loss processor with RTT and OWD counting modes."""

    name = "packet_loss"
    subscribes_to = [
        "udp.wan.owd",
        "tcp.wan.owd",
        "udp.wan.rtt",
        "tcp.wan.rtt",
    ]

    def __init__(self, send_interval, **proc_opts):
        super().__init__(send_interval, **proc_opts)
        self.mode = proc_opts.get("mode", "both")
        self.interval_ms = proc_opts.get("interval", 1000)
        self.flows = {}

    def _get_flow(self, metric_name, tags):
        """Get or create a flow state for the given metric and tags."""
        flow_key = (metric_name, frozenset(tags))
        return self.flows.setdefault(flow_key, {"received": 0, "failed": 0})

    def process(self, metric):
        """Update per-flow counters from incoming metric."""
        meta = metric.get("meta", {})
        metric_name = metric["metric"]

        if ".rtt" in metric_name and self.mode in ("rtt", "both"):
            flow = self._get_flow(metric_name, metric["tags"])
            flow["received"] += 1
            if meta.get("status", "ok") != "ok":
                flow["failed"] += 1
            return []

        if ".owd" in metric_name and self.mode in ("owd", "both"):
            self._get_flow(metric_name, metric["tags"])["received"] += 1
            return []

        return []

    def _emit_flow(self, metric_name, tags, lost, total, now):
        """Build the three output metrics for a flow."""
        pct_bp = int((lost / total) * 10000) if total > 0 else 0
        return [
            self._build_metric(metric_name, None, [(now, pct_bp)], tags),
            self._build_metric(metric_name, "lost_count", [(now, lost)], tags),
            self._build_metric(metric_name, "probe_count", [(now, total)], tags),
        ]

    def flush(self):
        """Emit loss metrics and reset counters."""
        results = []
        now = time.time_ns()
        elapsed = time.time() - self.last_flush
        expected = int(elapsed / (self.interval_ms / 1000))

        for (metric_name, tags), flow in self.flows.items():
            if ".rtt" in metric_name and flow["received"] > 0:
                total = flow["received"]
                lost = flow["failed"]
                results.extend(self._emit_flow(metric_name, tags, lost, total, now))

            if ".owd" in metric_name and flow["received"] > 0:
                received = flow["received"]
                total = max(expected, received)
                lost = total - received
                results.extend(self._emit_flow(metric_name, tags, lost, total, now))

        self.flows = {}
        self.last_flush = time.time()
        return results
