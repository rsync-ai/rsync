#!/usr/bin/env python3
"""
Kafka MCP Sink Connector
Thin Python MCP wrapper that spawns Go workers for Kafka consumption

Category: streaming
Version: 1.0.0
"""

import sys
import os
import logging
import subprocess
import json
import asyncio
import threading
import time
from typing import Dict, Any, Optional
import httpx
import socket

# Add parent directory for base_connector imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from base_connector import BaseMCPConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# A worker that dies within this many seconds of being (re)spawned is treated as a
# fast-failing crash loop (bad config, dest permanently down, immediate fatal exit)
# and its next respawn is backed off exponentially. A worker that survives past this
# window resets the backoff, so a one-off crash after long healthy uptime still
# restarts immediately.
RAPID_RESTART_WINDOW_SECONDS = 30.0

# Circuit breaker: after this many CONSECUTIVE rapid restarts (each death within
# RAPID_RESTART_WINDOW_SECONDS of the prior spawn), the supervisor stops respawning
# and marks the worker `crashed` — a terminal state that health()/sink_status
# surface. Without this cap a permanently-broken worker (DLQ down, a poison record it
# can never DLQ, a destination that always rejects) respawns every 60s forever,
# invisibly, delivering nothing while the pipeline reads green. The cap converts an
# invisible infinite crash-loop into an observable failure the orchestrator can act on.
# A worker that survives a window resets rapid_restarts to 0, so this only trips on a
# genuine tight loop, never on an occasional crash after healthy uptime. Cleared by an
# explicit start_sink (which drops the dead worker and spawns a fresh one).
MAX_RAPID_RESTARTS = 5


class KafkaMCPSinkConnector(BaseMCPConnector):
    """Thin Python MCP wrapper that manages Go workers for Kafka → Destination sink"""

    def __init__(self):
        super().__init__()

        self.connector_type = "kafka-mcp-sink"
        self.connector_category = "streaming"
        self.supports_source = False
        self.supports_destination = True
        self.supports_cdc = True

        self.supported_formats = ["json"]
        self.max_batch_size = 10000

        # Track active workers. Mutated by request handlers AND the supervisor
        # thread, so every access is guarded by `self._lock` (re-entrant: a
        # handler may take the lock and call a helper that takes it again).
        self.workers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

        # Metrics ports handed out to workers whose start_sink is still in flight
        # (picked but not yet registered in self.workers). All workers share this
        # container, so two concurrent start_sink calls could otherwise probe-and-
        # close onto the same ephemeral port and both advertise the same /status
        # endpoint — a caller polling metrics_url would then cross-read a sibling
        # worker's progress. Guarded by self._lock alongside self.workers.
        self._reserved_ports: set[int] = set()

        # Supervisor: a daemon thread that respawns any worker whose process has
        # died unexpectedly (crash / OOM / SIGKILL), so a dead sink does not
        # silently stall the pipeline until the next start_sink call. Workers the
        # user has intentionally stopped carry intentional_stop=True and are NOT
        # resurrected. Daemon so it never blocks container shutdown.
        self._supervisor_stop = threading.Event()
        self._supervisor_thread = threading.Thread(
            target=self._supervisor_loop, name="sink-supervisor", daemon=True
        )
        self._supervisor_thread.start()

        self.log("Kafka MCP Sink Connector initialized")

    def _pick_free_local_port(self) -> int:
        """
        Pick and RESERVE a TCP port inside the container that no other live worker
        already holds. All sink workers share this container, so the naive
        bind-to-0-then-close probe has a TOCTOU: once the probe socket closes the OS
        may re-hand the same port to a concurrent start_sink, leaving two workers on
        one /status port (callers polling metrics_url then cross-read a sibling's
        progress). Reserving under self._lock against both registered workers and
        other in-flight reservations closes that window. The caller MUST release the
        port via self._reserved_ports.discard(port) once the worker is registered
        (start_sink does this in its finally).
        """
        for _ in range(50):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind(("127.0.0.1", 0))
                port = int(s.getsockname()[1])
            finally:
                s.close()
            with self._lock:
                in_use = {w.get("metrics_port") for w in self.workers.values()}
                if port not in in_use and port not in self._reserved_ports:
                    self._reserved_ports.add(port)
                    return port
        raise RuntimeError("could not reserve a free metrics port after 50 attempts")

    @staticmethod
    def _normalize_topics(topics_raw: Any) -> list:
        """Coerce a topics value (str | list | None) into a clean list[str].

        Shared by start_sink's worker-config build and its worker_id-collision
        guard so both compare topics the same way.
        """
        if isinstance(topics_raw, str):
            t = topics_raw.strip()
            return [t] if t else []
        if isinstance(topics_raw, list):
            return [str(t).strip() for t in topics_raw if str(t).strip()]
        return []

    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        """
        Override BaseMCPConnector.get_capabilities because this connector is
        a control-plane sink (start/stop/status), not a generic streaming producer.
        """
        ops = [
            {
                "name": "start_sink",
                "method": f"{self.connector_type}_start_sink",
                "description": "Start a Kafka sink worker",
                "type": "destination",
            },
            {
                "name": "stop_sink",
                "method": f"{self.connector_type}_stop_sink",
                "description": "Stop a running Kafka sink worker",
                "type": "destination",
            },
            {
                "name": "sink_status",
                "method": f"{self.connector_type}_sink_status",
                "description": "Get status of a Kafka sink worker",
                "type": "core",
            },
            {
                "name": "health",
                "method": f"{self.connector_type}_health",
                "description": "Health check",
                "type": "core",
            },
            {
                "name": "test_connection",
                "method": f"{self.connector_type}_test_connection",
                "description": "Test connector availability",
                "type": "core",
            },
            {
                "name": "validate_config",
                "method": f"{self.connector_type}_validate_config",
                "description": "Validate sink configuration",
                "type": "core",
            },
        ]

        return {
            "success": True,
            "connector_type": self.connector_type,
            "connector_category": self.connector_category,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "default_source_operation": None,
            "default_destination_operation": "start_sink",
            "terminology": {"entity": "topic", "record": "message", "field": "field"},
            "operations": ops,
            "optimization_category": self.optimization_category,
            "performance_target_ms": self._performance_target_ms,
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supports_cdc": self.supports_cdc,
            },
        }

    def _get_worker_id(self, params: Dict) -> str:
        """Get a stable worker identifier from params"""
        config = params.get("config", {})
        # Use consumer_group as the stable ID (pipeline_id may be optional)
        worker_id = config.get("consumer_group") or config.get("pipeline_id")
        if not worker_id:
            raise ValueError("Missing 'consumer_group' or 'pipeline_id' in config")
        return worker_id

    def _replica_count(self, config: Dict) -> int:
        """Number of worker PROCESSES to run for one consumer group.

        Default 1 => single worker (behavior unchanged). Values >1 spawn extra
        workers on the SAME Kafka consumer group so the broker rebalances the
        topic partitions across them — parallelizing consumption N-ways. Each
        process is a full, independent worker (own reader, batchers, offsets),
        so exactly-once and per-partition ordering are preserved: a partition is
        owned by exactly one member at a time and its offsets/high-water are
        committed only by that member. Bounded so a misconfig can't fork-bomb the
        256MB+ container.
        """
        try:
            n = int((config or {}).get("sink_worker_replicas", 1) or 1)
        except (TypeError, ValueError):
            n = 1
        return max(1, min(n, 8))

    def _replica_ids(self, base_worker_id: str) -> list:
        """All worker_ids belonging to one consumer group: the base plus any
        '<base>#<n>' sibling replicas. Snapshot of the current keys."""
        prefix = base_worker_id + "#"
        return [wid for wid in list(self.workers.keys())
                if wid == base_worker_id or wid.startswith(prefix)]

    def start_sink(self, params: Dict = None) -> Dict[str, Any]:
        """
        Start a Kafka sink worker (idempotent)

        Expected params.config:
        - topics: str (Kafka topic)
        - consumer_group: str
        - destination_connector: str
        - destination_config: dict (with 'table' key)
        - kafka_bootstrap_servers: str (optional; falls back to KAFKA_BROKERS /
          KAFKA_BOOTSTRAP_SERVERS, then kafka:29092)
        - pipeline_id: str (optional)
        """
        params = params or {}
        config = params.get("config", {})
        # Metrics port reserved for this worker (released in finally). None until picked.
        metrics_port = None

        try:
            worker_id = self._get_worker_id(params)

            # Check if already running
            with self._lock:
                if worker_id in self.workers:
                    worker = self.workers[worker_id]
                    if self._is_worker_alive(worker_id):
                        # Idempotent re-call is only safe for the SAME topics. A
                        # different topic set under the same worker_id means two
                        # pipelines collided on consumer_group/pipeline_id (e.g.
                        # both deriving it from a bare int(time.time())). Returning
                        # this worker's metrics_url would make the caller poll a
                        # SIBLING's /status, believe its own sink is running, and
                        # never start it — the destination is then never written and
                        # the only symptom is a downstream timeout. Fail closed so
                        # the collision is loud, not a silent cross-wire.
                        existing_topics = set(
                            (worker.get("config") or {}).get("topics") or []
                        )
                        requested_topics = set(
                            self._normalize_topics(config.get("topics"))
                        )
                        if (
                            requested_topics
                            and existing_topics
                            and requested_topics != existing_topics
                        ):
                            return {
                                "success": False,
                                "status": "worker_id_collision",
                                "worker_id": worker_id,
                                "error": (
                                    f"worker_id '{worker_id}' already runs topics "
                                    f"{sorted(existing_topics)}; refusing to alias it "
                                    f"to {sorted(requested_topics)}. Use a unique "
                                    f"consumer_group/pipeline_id per sink."
                                ),
                            }
                        return {
                            "success": True,
                            "status": "already_running",
                            "worker_id": worker_id,
                            "pid": worker.get("pid"),
                            "restart_attempts": worker.get("restart_attempts", 0),
                            "consumer_group": config.get("consumer_group"),
                            "metrics_url": f"http://localhost:{worker['metrics_port']}/status"
                        }
                    else:
                        # Was running but died - clean up and restart
                        self.log(f"Worker {worker_id} died, restarting", level="warn")
                        self._cleanup_worker(worker_id)

            # Prepare worker config
            topics_list = self._normalize_topics(config.get("topics"))

            # Reserve the metrics port up front so the finally can release it on any path.
            metrics_port = self._pick_free_local_port()

            worker_config = {
                "pipeline_id": config.get("pipeline_id", worker_id),
                "execution_id": config.get("execution_id", ""),
                # Back-compat: the Go worker supports both `topic` (single) and `topics` (multi).
                "topic": topics_list[0] if topics_list else "",
                "topics": topics_list,
                "consumer_group": config.get("consumer_group"),
                # Fall back to the deployment's own env before the compose-only
                # literal: on a customer's Kafka there is no host named "kafka", so a
                # caller that omits this would send the worker to a name that cannot
                # resolve — reported as a broker outage rather than a missing field.
                "kafka_bootstrap_servers": (
                    config.get("kafka_bootstrap_servers")
                    or os.getenv("KAFKA_BROKERS", "").strip()
                    or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
                    or "kafka:29092"
                ),
                # DLQ publishing brokers (override). Empty => worker falls back to
                # kafka_bootstrap_servers. Must be forwarded or the worker can never
                # see a DLQ-down condition (and the fail-closed-on-DLQ-down contract
                # can't engage / can't be tested).
                "dlq_bootstrap_servers": config.get("dlq_bootstrap_servers", ""),
                "destination_connector": config.get("destination_connector"),
                # Required for versioned-only runtime. Must be a concrete version (e.g., "v1.0.0"),
                # not "latest", because there is no stable `rsync-ai-<id>-mcp` container.
                "destination_version": config.get("destination_version") or config.get("connector_version") or "latest",
                "destination_config": config.get("destination_config", {}),
                # Per-pipeline destination namespace (schema/db) for relational sinks.
                # MUST be allowlisted here or it is dropped before reaching the Go
                # worker (this dict is an implicit whitelist). Empty => unchanged.
                "destination_namespace": config.get("destination_namespace", ""),
                "metrics_port": metrics_port,
                # Optional CDC controls
                "sink_mode": config.get("sink_mode", ""),
                "start_offset": config.get("start_offset", ""),
            }

            # Supervisor policy: auto-restart a worker that dies unexpectedly
            # (default ON). Set auto_restart=False to leave a fail-closed halt
            # (DLQ-down, destructive schema change) observable as status=stopped.
            auto_restart = config.get("auto_restart", True)

            # Validate required fields
            if not worker_config["topic"] and not worker_config.get("topics"):
                return {"success": False, "error": "Missing 'topics' in config"}
            if not worker_config["consumer_group"]:
                return {"success": False, "error": "Missing 'consumer_group' in config"}
            if not worker_config["destination_connector"]:
                return {"success": False, "error": "Missing 'destination_connector' in config"}

            # Start Go worker
            process = self._spawn_worker_process(worker_config)

            # Store worker info
            with self._lock:
                self.workers[worker_id] = {
                    "process": process,
                    "pid": process.pid,
                    "config": worker_config,
                    "metrics_port": worker_config["metrics_port"],
                    "restart_attempts": 0,
                    "intentional_stop": False,
                    "auto_restart": auto_restart,
                }

            # Wait for worker to be ready (best-effort)
            self._wait_for_worker_ready(worker_config["metrics_port"], timeout=5)

            # A worker that booted then immediately exited (bad destination creds,
            # DLQ-down fail-closed halt, fatal config) must NOT be reported as a green
            # "started" — that defers the failure deep into the data plane. If the
            # process is already gone, clean it up (so the supervisor doesn't crash-loop
            # a known-bad worker) and fail the call with the worker's last error.
            if not self._is_worker_alive(worker_id):
                last_err = self._worker_last_error(worker_config["metrics_port"])
                with self._lock:
                    self._cleanup_worker(worker_id)
                return {
                    "success": False,
                    "status": "exited",
                    "worker_id": worker_id,
                    "error": last_err or "worker exited during startup",
                }

            # Horizontal scale-out (Fix #3): optionally spawn additional workers in
            # the SAME consumer group so kafka-go rebalances partitions across them.
            # Default replicas=1 => this loop is a no-op and behavior is unchanged.
            # A replica that fails to start is logged and skipped (degrade, don't fail
            # the whole sink) — the surviving members still cover all partitions.
            replicas = self._replica_count(config)
            replica_urls = []
            for i in range(1, replicas):
                sib_id = f"{worker_id}#{i}"
                sib_port = None
                try:
                    with self._lock:
                        if sib_id in self.workers and self._is_worker_alive(sib_id):
                            replica_urls.append(
                                f"http://localhost:{self.workers[sib_id]['metrics_port']}/status"
                            )
                            continue
                    sib_port = self._pick_free_local_port()
                    sib_cfg = dict(worker_config)
                    sib_cfg["metrics_port"] = sib_port
                    sib_proc = self._spawn_worker_process(sib_cfg)
                    with self._lock:
                        self.workers[sib_id] = {
                            "process": sib_proc,
                            "pid": sib_proc.pid,
                            "config": sib_cfg,
                            "metrics_port": sib_port,
                            "restart_attempts": 0,
                            "intentional_stop": False,
                            "auto_restart": auto_restart,
                        }
                    self._wait_for_worker_ready(sib_port, timeout=5)
                    replica_urls.append(f"http://localhost:{sib_port}/status")
                except Exception as e:
                    self.log(f"Failed to start replica {sib_id} (continuing with fewer): {e}", level="warn")
                finally:
                    if sib_port is not None:
                        with self._lock:
                            self._reserved_ports.discard(sib_port)

            return {
                "success": True,
                "status": "started",
                "worker_id": worker_id,
                "pid": process.pid,
                "restart_attempts": 0,
                "pipeline_id": worker_config["pipeline_id"],
                "consumer_group": worker_config["consumer_group"],
                "topic": worker_config["topic"],
                "replicas": replicas,
                "replica_metrics_urls": replica_urls,
                "metrics_url": f"http://localhost:{worker_config['metrics_port']}/status"
            }

        except Exception as e:
            self.log(f"Failed to start sink: {e}", level="error")
            return {"success": False, "error": str(e)}
        finally:
            # Release the in-flight reservation: on success the port now lives in
            # self.workers; on any failure it is free again. discard() is idempotent.
            if metrics_port is not None:
                with self._lock:
                    self._reserved_ports.discard(metrics_port)

    def stop_sink(self, params: Dict = None) -> Dict[str, Any]:
        """
        Stop a running sink worker

        Expected params.config:
        - consumer_group: str (or pipeline_id)
        """
        params = params or {}

        try:
            base = self._get_worker_id(params)

            with self._lock:
                ids = self._replica_ids(base)
                if not ids:
                    return {"success": False, "error": f"Worker not found: {base}"}
                # Mark ALL replicas intentional BEFORE terminating so the supervisor
                # (which may observe a dead process before cleanup) does not resurrect
                # any of them.
                targets = []
                for wid in ids:
                    w = self.workers.get(wid)
                    if w:
                        w["intentional_stop"] = True
                        targets.append((wid, w))

            # Send SIGTERM for graceful shutdown (outside the lock — wait() can take
            # up to 30s and must not block sink_status / the supervisor poll).
            for wid, worker in targets:
                try:
                    worker["process"].terminate()
                    try:
                        worker["process"].wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        self.log(f"Worker {wid} did not stop gracefully, force killing", level="warn")
                        worker["process"].kill()
                except Exception as e:
                    self.log(f"Error stopping worker {wid}: {e}", level="warn")

            # Clean up all replicas.
            with self._lock:
                for wid, _ in targets:
                    self._cleanup_worker(wid)

            return {
                "success": True,
                "status": "stopped",
                "worker_id": base,
                "stopped": [wid for wid, _ in targets],
            }

        except Exception as e:
            self.log(f"Failed to stop sink: {e}", level="error")
            return {"success": False, "error": str(e)}

    def sink_status(self, params: Dict = None) -> Dict[str, Any]:
        """
        Get status of a sink worker

        Expected params.config:
        - consumer_group: str (or pipeline_id)
        """
        params = params or {}

        try:
            worker_id = self._get_worker_id(params)

            with self._lock:
                if worker_id not in self.workers:
                    return {
                        "success": False,
                        "status": "not_found",
                        "worker_id": worker_id
                    }

                worker = self.workers[worker_id]
                # Snapshot the fields under the lock; the (network) metrics query
                # below runs without it. pid/restart_attempts are what the
                # auto-recovery e2e test polls to prove the supervisor respawned.
                pid = worker.get("pid")
                restart_attempts = worker.get("restart_attempts", 0)
                metrics_port = worker["metrics_port"]
                alive = self._is_worker_alive(worker_id)
                crashed = bool(worker.get("crashed"))
                crashed_error = worker.get("last_error")

            # Check if process is alive
            if not alive:
                # Distinguish a terminal crash-loop (circuit breaker tripped; the
                # supervisor has GIVEN UP respawning) from a momentary down / awaiting
                # respawn, so the caller can escalate a crashed sink instead of waiting.
                result = {
                    "success": True,
                    "status": "crashed" if crashed else "stopped",
                    "worker_id": worker_id,
                    "pid": pid,
                    "restart_attempts": restart_attempts,
                }
                if crashed and crashed_error:
                    result["error"] = crashed_error
                return result

            # Query Go worker's /status endpoint
            try:
                import httpx
                response = httpx.get(
                    f"http://localhost:{metrics_port}/status",
                    timeout=5.0
                )
                metrics = response.json()

                result = {
                    "success": True,
                    "status": "running",
                    "worker_id": worker_id,
                    "pid": pid,
                    "restart_attempts": restart_attempts,
                    "metrics": metrics
                }
                # Promote a worker-reported fatal error to the top level so callers don't
                # have to dig into metrics. Gate on a real failure signal (failed>0 or
                # dlq_publish_failures>0) so a stale last_error from a since-recovered
                # transient doesn't raise a false alarm.
                if isinstance(metrics, dict):
                    last_error = (metrics.get("last_error") or "").strip()
                    failed = metrics.get("failed") or 0
                    dlq_fail = metrics.get("dlq_publish_failures_total") or 0
                    if last_error and ((failed and failed > 0) or (dlq_fail and dlq_fail > 0)):
                        result["error"] = last_error
                return result
            except Exception as e:
                self.log(f"Failed to query worker metrics: {e}", level="warn")
                return {
                    "success": True,
                    "status": "running",
                    "worker_id": worker_id,
                    "pid": pid,
                    "restart_attempts": restart_attempts,
                    "metrics": None,
                    "error": f"Failed to query metrics: {str(e)}"
                }

        except Exception as e:
            self.log(f"Failed to get sink status: {e}", level="error")
            return {"success": False, "error": str(e)}

    def health(self, params: Dict = None) -> Dict[str, Any]:
        """Health check — reports TRUE per-worker liveness, not merely how many are registered.

        The old body returned ``active_workers = len(self.workers)`` with a hard-coded
        ``status: healthy``, so a crash-looping or dead worker still counted as active:
        a pipeline whose sink had died read green with 0 rows landing, indefinitely, no
        alarm. That "green but dead" state is the #1 "the pipeline isn't working" report.

        Still returns HTTP 200 (the Dockerfile HEALTHCHECK only checks the code, and this
        one container hosts EVERY pipeline's sink — flipping it unhealthy would kill them
        all). The truth is carried in the body: overall ``status`` (healthy | degraded) +
        alive / dead / crashed counts + the unhealthy workers, so a poller (orchestrator
        dependency probe / sink sentinel) can detect a dead or crashed sink.
        """
        runtime_version = (os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or "1.0.0").strip()
        if runtime_version and not runtime_version.startswith("v"):
            runtime_version = f"v{runtime_version}"

        alive = 0
        dead = 0
        crashed = 0
        unhealthy: list = []
        with self._lock:
            total = len(self.workers)
            for worker_id, worker in self.workers.items():
                if worker.get("crashed"):
                    # Circuit-breaker tripped: terminal, not being respawned.
                    crashed += 1
                    unhealthy.append({
                        "worker_id": worker_id,
                        "state": "crashed",
                        "pid": worker.get("pid"),
                        "restart_attempts": worker.get("restart_attempts", 0),
                        "last_error": worker.get("last_error"),
                    })
                elif self._is_worker_alive(worker_id):
                    alive += 1
                else:
                    # Process not alive but breaker not tripped → transiently down /
                    # awaiting a supervisor respawn. Still not delivering right now.
                    dead += 1
                    unhealthy.append({
                        "worker_id": worker_id,
                        "state": "down",
                        "pid": worker.get("pid"),
                        "restart_attempts": worker.get("restart_attempts", 0),
                    })

        healthy = (dead == 0 and crashed == 0)
        return {
            "success": True,
            # A dead/crashed sink is degraded even though the connector process is up.
            "status": "healthy" if healthy else "degraded",
            "connector": "kafka-mcp-sink",
            "version": runtime_version or "1.0.0",
            # Back-compat field, now honest: ALIVE workers only (was len(self.workers)).
            "active_workers": alive,
            "total_workers": total,
            "dead_workers": dead,
            "crashed_workers": crashed,
            "unhealthy_workers": unhealthy,
        }

    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        """Test connection (not applicable for sink)"""
        return {"success": True, "message": "Kafka sink connector is operational"}

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        """Validate configuration"""
        params = params or {}
        config = params.get("config", {})

        errors = []

        if not config.get("topics"):
            errors.append("Missing 'topics'")
        if not config.get("consumer_group"):
            errors.append("Missing 'consumer_group'")
        if not config.get("destination_connector"):
            errors.append("Missing 'destination_connector'")

        dest_config = config.get("destination_config", {})
        if not dest_config.get("table"):
            errors.append("Missing 'destination_config.table'")

        if errors:
            return {"success": False, "errors": errors}

        return {"success": True, "message": "Configuration is valid"}

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        """Not applicable for sink connector"""
        return {"success": False, "error": "discover_schema not supported for sink connector"}

    def export(self, params: Dict = None) -> Dict[str, Any]:
        """Not applicable for sink connector"""
        return {"success": False, "error": "export not supported for sink connector"}

    def _spawn_worker_process(self, worker_config: Dict[str, Any]) -> subprocess.Popen:
        """Launch the Go sink worker for ``worker_config`` and return the process.

        Shared by ``start_sink`` (first launch) and ``_restart_worker``
        (supervisor respawn) so both go through identical env/launch semantics.
        """
        return subprocess.Popen(
            ["/app/worker/kafka-sink-worker"],
            env={
                **os.environ,
                "CONFIG": json.dumps(worker_config),
                "REDIS_URL": os.getenv("REDIS_URL", "redis:6379")
            },
            # IMPORTANT: Do not PIPE without a reader thread, or the worker can deadlock
            # when stdout/stderr buffers fill. Let Docker capture logs.
            stdout=None,
            stderr=None
        )

    def _is_worker_alive(self, worker_id: str) -> bool:
        """Check if a worker process is alive"""
        if worker_id not in self.workers:
            return False
        return self.workers[worker_id]["process"].poll() is None

    def _cleanup_worker(self, worker_id: str):
        """Clean up a worker"""
        if worker_id in self.workers:
            del self.workers[worker_id]

    # ------------------------------------------------------------------ #
    # Supervisor — auto-restart workers that die unexpectedly
    # ------------------------------------------------------------------ #
    def _supervisor_loop(self):
        """Background daemon: poll every few seconds and respawn dead workers.

        A worker whose OS process has exited (crash, OOM, external SIGKILL) is
        restarted from its saved config, with ``restart_attempts`` incremented so
        ``sink_status`` exposes that a recovery happened. Workers flagged
        ``intentional_stop`` (via ``stop_sink``) are skipped and left for cleanup.
        """
        POLL_INTERVAL = 3.0
        while not self._supervisor_stop.is_set():
            self._supervisor_stop.wait(POLL_INTERVAL)
            if self._supervisor_stop.is_set():
                break
            try:
                with self._lock:
                    for worker_id in list(self.workers.keys()):
                        worker = self.workers.get(worker_id)
                        if not worker or worker.get("intentional_stop"):
                            continue
                        # Circuit breaker already tripped: this worker is a terminal
                        # `crashed`, not a respawn candidate. Kept in the registry so
                        # health()/sink_status keep surfacing the failure until an
                        # explicit start_sink replaces it.
                        if worker.get("crashed"):
                            continue
                        # auto_restart=False => a fail-closed halt is intentional
                        # and must stay observable as status=stopped. Leave it.
                        if not worker.get("auto_restart", True):
                            continue
                        if not self._is_worker_alive(worker_id):
                            now = time.monotonic()
                            # Respect the crash-loop backoff window: a fast-failing
                            # worker must not be respawned every poll (tight loop that
                            # burns CPU, spams logs, and hammers the destination).
                            if now < worker.get("restart_not_before", 0.0):
                                continue
                            # Update the crash-loop counter and decide: respawn, or trip
                            # the circuit breaker (terminal `crashed`). Extracted so the
                            # safety-critical trip is deterministically unit-testable.
                            if not self._record_rapid_restart_and_maybe_trip(worker, worker_id, now):
                                continue
                            self._restart_worker(worker_id)
                            backoff = self._restart_backoff_seconds(worker.get("rapid_restarts", 0))
                            worker["restart_not_before"] = time.monotonic() + backoff
            except Exception as e:  # never let the supervisor thread die
                self.log(f"Supervisor loop error: {e}", level="error")

    def _record_rapid_restart_and_maybe_trip(self, worker: Dict[str, Any], worker_id: str, now: float) -> bool:
        """Account for a just-observed dead worker and decide whether to respawn it.

        Increments ``rapid_restarts`` when the worker died within
        RAPID_RESTART_WINDOW_SECONDS of its last spawn (resets it otherwise), then trips
        the circuit breaker once the count reaches MAX_RAPID_RESTARTS: the worker is
        marked terminal ``crashed`` (with a last_error) and NOT respawned. Returns True to
        respawn, False if the breaker tripped. Caller holds ``self._lock``.
        """
        last_spawn = worker.get("last_spawn_monotonic", 0.0)
        if last_spawn and (now - last_spawn) < RAPID_RESTART_WINDOW_SECONDS:
            worker["rapid_restarts"] = worker.get("rapid_restarts", 0) + 1
        else:
            worker["rapid_restarts"] = 0

        if worker.get("rapid_restarts", 0) >= MAX_RAPID_RESTARTS:
            worker["crashed"] = True
            worker["crashed_at"] = time.time()
            worker["last_error"] = (
                worker.get("last_error")
                or f"crash-looped: {worker['rapid_restarts']} rapid restarts "
                   f"(>= MAX_RAPID_RESTARTS={MAX_RAPID_RESTARTS}); not respawning"
            )
            self.log(
                f"Worker {worker_id} crash-looped {worker['rapid_restarts']}x within "
                f"{RAPID_RESTART_WINDOW_SECONDS}s windows; tripping circuit breaker -> "
                f"status=crashed (restart_attempts={worker.get('restart_attempts', 0)})",
                level="error",
            )
            return False
        return True

    @staticmethod
    def _restart_backoff_seconds(rapid_restarts: int) -> float:
        """Exponential backoff (seconds) before respawning a fast-failing worker.

        0 rapid restarts -> 0 (respawn immediately, the healthy case); then
        1, 2, 4, 8, ... capped at 60s. Driven by ``rapid_restarts``, which the
        supervisor increments only when a worker dies inside
        ``RAPID_RESTART_WINDOW_SECONDS`` of its last spawn and resets otherwise.
        """
        if rapid_restarts <= 0:
            return 0.0
        return float(min(2 ** (rapid_restarts - 1), 60))

    def _restart_worker(self, worker_id: str):
        """Respawn a dead worker from its saved config. Caller holds ``self._lock``."""
        worker = self.workers.get(worker_id)
        if not worker or worker.get("intentional_stop") or worker.get("crashed") or not worker.get("auto_restart", True):
            return
        old_pid = worker.get("pid")
        worker_config = worker.get("config")
        if not worker_config:
            self.log(f"Worker {worker_id} has no saved config; cannot restart", level="error")
            return
        try:
            process = self._spawn_worker_process(worker_config)
        except Exception as e:
            self.log(f"Failed to restart worker {worker_id}: {e}", level="error")
            return
        worker["process"] = process
        worker["pid"] = process.pid
        worker["last_spawn_monotonic"] = time.monotonic()
        worker["restart_attempts"] = worker.get("restart_attempts", 0) + 1
        self.log(
            f"Supervisor restarted worker {worker_id}: old_pid={old_pid} "
            f"new_pid={process.pid} restart_attempts={worker['restart_attempts']}",
            level="warn",
        )

    def _wait_for_worker_ready(self, port: int, timeout: int = 5):
        """Wait for worker to be ready (best-effort)"""
        import time
        import httpx

        start = time.time()
        while time.time() - start < timeout:
            try:
                response = httpx.get(f"http://localhost:{port}/health", timeout=1.0)
                if response.status_code == 200:
                    return
            except:
                pass
            time.sleep(0.5)

        # Timeout - log warning but don't fail
        self.log(f"Worker did not become ready within {timeout}s (may still be starting)", level="warn")

    def _worker_last_error(self, port: int):
        """Best-effort: fetch the worker's reported last_error from /status, or None
        if the metrics endpoint is unreachable (e.g. the process already exited) or
        reports none. Used to surface the real failure reason instead of a green
        'started' / a 'running' status that hides a fatal worker error."""
        try:
            import httpx
            data = httpx.get(f"http://localhost:{port}/status", timeout=2.0).json()
            if isinstance(data, dict):
                err = (data.get("last_error") or "").strip()
                return err or None
        except Exception:
            pass
        return None


# =========================================================================
# HTTP SERVER (FastAPI)
# =========================================================================

def create_http_app():
    """Create FastAPI app for HTTP-based MCP server"""
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
    from typing import Optional, Any

    runtime_version = os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or "1.0.0"
    runtime_version = str(runtime_version).strip()
    if runtime_version and not runtime_version.startswith("v"):
        runtime_version = f"v{runtime_version}"

    app = FastAPI(
        title="Kafka MCP Sink Connector",
        description="Kafka → Destination sink connector",
        version=runtime_version or "1.0.0"
    )

    server = KafkaMCPSinkConnector()

    class MCPRequest(BaseModel):
        method: str
        params: Optional[dict] = None

    @app.get("/health")
    async def health():
        return server.health()

    @app.post("/mcp")
    async def mcp_handler(request: MCPRequest):
        """Handle MCP JSON-RPC requests over HTTP"""
        try:
            result = server.handle_request({
                "jsonrpc": "2.0",
                "id": 1,
                "method": request.method,
                "params": request.params or {}
            })
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/invoke/{tool_name}")
    async def invoke_tool(tool_name: str, params: dict = {}):
        """Direct tool invocation endpoint"""
        try:
            # Map tool names to methods
            method_name = tool_name.replace("-", "_")
            # Security: never expose private/internal methods (e.g. _spawn_worker_process,
            # _cleanup_worker, upload_to_staging) through the unauthenticated HTTP surface.
            if not method_name or method_name.startswith("_"):
                raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
            if hasattr(server, method_name):
                method = getattr(server, method_name)
                return method(params)

            raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/capabilities")
    async def get_capabilities():
        return server.get_capabilities()

    @app.post("/start_sink")
    async def start_sink(params: dict = {}):
        return server.start_sink(params)

    @app.post("/stop_sink")
    async def stop_sink(params: dict = {}):
        return server.stop_sink(params)

    @app.post("/sink_status")
    async def sink_status(params: dict = {}):
        return server.sink_status(params)

    return app


if __name__ == "__main__":
    import os

    # Check if running in HTTP mode (Docker)
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))

    if http_mode:
        # HTTP mode (Docker)
        import uvicorn
        app = create_http_app()
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        # STDIO mode (development)
        server = KafkaMCPSinkConnector()
        # BaseMCPConnector's stdio entry point is run(); there is no run_stdio() —
        # the old name raised AttributeError and made every non-HTTP launch crash.
        server.run()

