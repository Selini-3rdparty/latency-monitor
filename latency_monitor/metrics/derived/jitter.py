# -*- coding: utf-8 -*-
"""
RFC 3550 Jitter Processor
=========================

Computes interarrival jitter using the RFC 3550 algorithm:
    D(i,j) = |OWD_j - OWD_i|
    J(i) = J(i-1) + (D(i,i-1) - J(i-1)) / 16
"""

import time

from latency_monitor.metrics.derived import DerivedProcessor


class Jitter(DerivedProcessor):
    name = "jitter"
    subscribes_to = [
        "udp.wan.owd",
        "tcp.wan.owd",
        "udp.wan.rtt",
        "tcp.wan.rtt",
    ]

    def __init__(self, send_interval, **proc_opts):
        super().__init__(send_interval, **proc_opts)
        self.flows = {}

    def process(self, metric):
        meta = metric.get("meta", {})
        if meta.get("status") not in (None, "ok"):
            return []

        results = []
        for ts, value in metric["points"]:
            if value <= 0:
                continue

            flow_key = (metric["metric"], frozenset(metric["tags"]))
            flow = self.flows.get(flow_key)

            if flow is None:
                self.flows[flow_key] = {"prev_value": value, "jitter": 0.0}
                continue

            d = abs(value - flow["prev_value"])
            flow["jitter"] += (d - flow["jitter"]) / 16.0
            flow["prev_value"] = value

            results.append(
                self._build_metric(
                    metric["metric"],
                    None,
                    [(ts, int(flow["jitter"]))],
                    metric["tags"],
                )
            )

        return results

    def flush(self):
        results = []
        now = time.time_ns()
        for (metric_name, tags), flow in self.flows.items():
            if flow["jitter"] > 0:
                results.append(
                    self._build_metric(
                        metric_name,
                        None,
                        [(now, int(flow["jitter"]))],
                        tags,
                    )
                )
        self.last_flush = time.time()
        return results
