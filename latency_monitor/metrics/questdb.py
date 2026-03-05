# -*- coding: utf-8 -*-
"""
QuestDB metrics backend
=======================

Ships metrics to QuestDB via InfluxDB Line Protocol (ILP) over TCP.
"""

import logging
import socket

from latency_monitor.metrics.accumulator import Accumulator

log = logging.getLogger(__name__)


class QuestDB(Accumulator):
    """
    Accumulate metrics and ship them to QuestDB at specific intervals
    using the InfluxDB Line Protocol (ILP) over a raw TCP socket.
    """

    def __init__(self, **opts):
        super().__init__(**opts)
        self.host = self.opts["metrics"]["host"]
        self.port = self.opts["metrics"].get("port", 9009)
        self.sock = None
        self._connect()

    def _connect(self):
        """Open TCP socket to QuestDB ILP endpoint."""
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        log.info("Connected to QuestDB ILP at %s:%d", self.host, self.port)

    def _push_metrics(self, metrics):
        """Convert metrics to ILP lines and send over TCP."""
        lines = []
        for metric in metrics:
            measurement = metric["metric"].replace(".", "_")
            tags = ",".join(
                f"{k}={v}" for k, v in (t.split(":", 1) for t in metric["tags"])
            )
            for ts, value in metric["points"]:
                lines.append(f"{measurement},{tags} value={value}i {ts}\n")
        payload = "".join(lines)
        try:
            self.sock.sendall(payload.encode("utf-8"))
        except (BrokenPipeError, ConnectionResetError, OSError):
            log.warning("QuestDB connection lost, reconnecting")
            self._connect()
            self.sock.sendall(payload.encode("utf-8"))
