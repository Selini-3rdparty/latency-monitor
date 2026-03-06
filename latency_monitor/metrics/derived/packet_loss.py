# -*- coding: utf-8 -*-
"""
Packet Loss Processor
=====================

Computes packet loss percentage and counts per direction over a time window.

Modes:
- "rtt": RTT-based (round-trip loss from client-side metrics)
- "owd": OWD-based (per-direction loss using probe_sent + OWD counts)
- "both": emit both RTT and OWD loss metrics (default)

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
        "udp.wan.probe_sent",
        "tcp.wan.probe_sent",
    ]

    def __init__(self, send_interval, **proc_opts):
        super().__init__(send_interval, **proc_opts)
        self.mode = proc_opts.get("mode", "both")
        self.flows = {}

    def _get_flow(self, metric_name, tags):
        """Get or create a flow state for the given metric and tags."""
        flow_key = (metric_name, frozenset(tags))
        return self.flows.setdefault(flow_key, {"total": 0, "ok": 0, "failed": 0})

    def process(self, metric):  # pylint: disable=too-many-return-statements
        """Update per-flow counters from incoming metric."""
        meta = metric.get("meta", {})
        metric_name = metric["metric"]

        if metric_name.endswith(".probe_sent"):
            if self.mode in ("owd", "both"):
                base = metric_name.replace(".probe_sent", ".owd")
                self._get_flow(base, metric["tags"])["total"] += 1
            return []

        if ".rtt" in metric_name:
            if self.mode in ("rtt", "both"):
                flow = self._get_flow(metric_name, metric["tags"])
                flow["total"] += 1
                if meta.get("status", "ok") == "ok":
                    flow["ok"] += 1
                else:
                    flow["failed"] += 1
            return []

        if ".owd" in metric_name:
            if self.mode in ("owd", "both"):
                self._get_flow(metric_name, metric["tags"])["ok"] += 1
            return []

        return []

    def flush(self):
        results = []
        now = time.time_ns()

        for (metric_name, tags), flow in self.flows.items():
            if ".rtt" in metric_name and flow["total"] > 0:
                lost = flow["failed"]
                total = flow["total"]
                pct_bp = int((lost / total) * 10000)
                results.append(
                    self._build_metric(metric_name, None, [(now, pct_bp)], tags)
                )
                results.append(
                    self._build_metric(metric_name, "lost_count", [(now, lost)], tags)
                )
                results.append(
                    self._build_metric(metric_name, "probe_count", [(now, total)], tags)
                )

            if ".owd" in metric_name and flow["total"] > 0:
                lost = flow["total"] - flow["ok"]
                total = flow["total"]
                pct_bp = int((lost / total) * 10000)
                results.append(
                    self._build_metric(metric_name, None, [(now, pct_bp)], tags)
                )
                results.append(
                    self._build_metric(metric_name, "lost_count", [(now, lost)], tags)
                )
                results.append(
                    self._build_metric(metric_name, "probe_count", [(now, total)], tags)
                )

            if ".owd" in metric_name and flow["total"] == 0 and flow["ok"] > 0:
                results.append(
                    self._build_metric(
                        metric_name, "probe_count", [(now, flow["ok"])], tags
                    )
                )

        self.flows = {}
        self.last_flush = time.time()
        return results
