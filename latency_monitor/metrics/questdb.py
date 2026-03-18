# -*- coding: utf-8 -*-
"""
QuestDB metrics backend
=======================

Ships metrics to QuestDB via the official questdb Python client
using ILP over HTTP. Supports basic auth and TLS.
"""

import logging

from questdb.ingress import (  # pylint: disable=E0611
    IngressError,
    Sender,
    TimestampNanos,
)

from latency_monitor.metrics.accumulator import Accumulator

log = logging.getLogger(__name__)


class QuestDB(Accumulator):
    """
    Accumulate metrics and ship them to QuestDB at specific intervals
    using the official questdb client over HTTP.
    """

    def __init__(self, **opts):
        super().__init__(**opts)
        metrics_cfg = self.opts["metrics"]
        self.host = metrics_cfg["host"]
        self.port = metrics_cfg.get("port", 9000)
        self.username = metrics_cfg.get("username")
        self.password = metrics_cfg.get("password")
        self.tls = metrics_cfg.get("tls", False)
        self.sender = None

    def _build_conf(self):
        """Build a questdb connection string from config."""
        protocol = "https" if self.tls else "http"
        conf = f"{protocol}::addr={self.host}:{self.port};auto_flush=off;"
        if self.username:
            conf += f"username={self.username};"
        if self.password:
            conf += f"password={self.password};"
        return conf

    def _connect(self):
        """Create and establish a Sender connection."""
        if self.sender:
            try:
                self.sender.close()
            except Exception:  # pylint: disable=W0718
                pass
        conf = self._build_conf()
        self.sender = Sender.from_conf(conf)
        self.sender.establish()
        log.info(
            "Connected to QuestDB at %s:%d (tls=%s, auth=%s)",
            self.host,
            self.port,
            self.tls,
            bool(self.username),
        )

    def _push_metrics(self, metrics):
        """Convert metrics to rows and flush via the official client."""
        if not self.sender:
            self._connect()
        for metric in metrics:
            measurement = metric["metric"].replace(".", "_")
            tag_dict = dict(t.split(":", 1) for t in metric["tags"])
            for ts, value in metric["points"]:
                self.sender.row(
                    measurement,
                    symbols=tag_dict,
                    columns={"value": value},
                    at=TimestampNanos(ts),
                )
        try:
            self.sender.flush()
        except IngressError:
            log.warning("QuestDB flush failed, reconnecting", exc_info=True)
            self._connect()
