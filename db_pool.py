"""
Shared SAP HANA connection pool.

Replaces the old per-query `dbapi.connect()` pattern (a full TCP + auth
handshake on every single query, plus a 10s timeout penalty whenever the
first IP was unreachable). Instead we:

  * Probe the candidate hosts ONCE, remember the one that works, and reuse it.
  * Keep a small pool of persistent connections and hand them out / take them
    back, validating liveness cheaply via `isconnected()`.

This is the single biggest performance win for the dashboards: cold tab loads
drop from "open N fresh remote logins" to "borrow N warm connections".

All settings can be overridden via environment variables (see below) so
credentials/hosts no longer have to live in source.
"""
import os
import queue
import threading
import time

from hdbcli import dbapi

# ── Config (env-overridable; defaults preserve existing behaviour) ──────────
_HOSTS = [h.strip() for h in os.getenv(
    "HANA_HOSTS", "192.168.1.182,103.89.45.192").split(",") if h.strip()]
_PORT = int(os.getenv("HANA_PORT", "30015"))
_USER = os.getenv("HANA_USER", "DATA1")
_PASSWORD = os.getenv("HANA_PASSWORD", "Jivo@1989")
_POOL_SIZE = int(os.getenv("HANA_POOL_SIZE", "8"))
# Per-connect timeout in seconds. Only paid when actually opening a socket,
# and host-probing only happens once (the working host is then remembered).
_CONNECT_TIMEOUT = int(os.getenv("HANA_CONNECT_TIMEOUT", "8"))
# How long acquire() waits for a free connection when the pool is exhausted.
_ACQUIRE_TIMEOUT = int(os.getenv("HANA_ACQUIRE_TIMEOUT", "30"))


class HanaPool:
    def __init__(self, label="HANA"):
        self.label = label
        self._pool = queue.LifoQueue(maxsize=_POOL_SIZE)
        self._lock = threading.Lock()
        self._good_host = None     # remembered reachable host
        self._created = 0          # live connections owned by the pool

    # ── internal: open one connection, resolving the host if needed ─────────
    def _connect_to(self, host):
        return dbapi.connect(address=host, port=_PORT, user=_USER,
                             password=_PASSWORD, timeout=_CONNECT_TIMEOUT)

    def _new_conn(self):
        # Fast path: reuse the host we already know works.
        host = self._good_host
        if host:
            try:
                return self._connect_to(host)
            except Exception as e:
                print(f"[{self.label}] known host {host} failed ({e}); re-probing")
                self._good_host = None
        # Slow path (rare): probe every candidate host once.
        last_err = None
        for h in _HOSTS:
            try:
                print(f"[{self.label}] connecting to SAP HANA at {h}...")
                c = self._connect_to(h)
                self._good_host = h
                print(f"[{self.label}] connected at {h}")
                return c
            except Exception as e:
                last_err = e
                print(f"[{self.label}] connect failed at {h}: {e}")
        raise Exception(f"[{self.label}] could not connect to any HANA host: {last_err}")

    # ── borrow a connection ─────────────────────────────────────────────────
    def acquire(self):
        c = None
        try:
            c = self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                may_create = self._created < _POOL_SIZE
                if may_create:
                    self._created += 1
            if may_create:
                try:
                    return self._new_conn()
                except Exception:
                    with self._lock:
                        self._created -= 1
                    raise
            # Pool full and all checked out: wait for one to come back.
            c = self._pool.get(timeout=_ACQUIRE_TIMEOUT)

        # Validate the borrowed connection; replace it if dead.
        try:
            if not c.isconnected():
                raise Exception("stale connection")
        except Exception:
            try:
                c.close()
            except Exception:
                pass
            c = self._new_conn()   # _created count unchanged (1 out, 1 in)
        return c

    # ── return a connection ─────────────────────────────────────────────────
    def release(self, c, broken=False):
        if c is None:
            return
        if broken:
            try:
                c.close()
            except Exception:
                pass
            with self._lock:
                self._created -= 1
            return
        try:
            self._pool.put_nowait(c)
        except queue.Full:
            try:
                c.close()
            except Exception:
                pass
            with self._lock:
                self._created -= 1

    # ── pre-open connections so the first request is warm ───────────────────
    def warm(self, n=None):
        n = min(n or _POOL_SIZE, _POOL_SIZE)
        opened = []
        for _ in range(n):
            try:
                opened.append(self.acquire())
            except Exception as e:
                print(f"[{self.label}] warm() stopped: {e}")
                break
        for c in opened:
            self.release(c)
        if opened:
            print(f"[{self.label}] warmed {len(opened)} connection(s)")


# Both divisions use identical credentials/host, so they share one pool.
pool = HanaPool("HANA")
