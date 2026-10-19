"""存储：StarRocks agent_meta（主）+ SQLite（SR 不可用时降级）

按 docs/environment.md 拍板 #5：agent 存储用 StarRocks（agent_meta 库 + dq_agent 账号），
SR 挂掉时排查主链路照走，读写降级本地 SQLite；进程内后端选择 sticky，恢复后不自动回切
（避免双写错乱），重启进程即回到 StarRocks。
"""

import json
import logging
import os
import sqlite3
import threading
from dataclasses import asdict
from datetime import datetime, timedelta

from .config import settings

log = logging.getLogger(__name__)

# 单机 BE 必须 replication_num=1，否则建表默认三副本会挂起
_REPLICATION = 'PROPERTIES ("replication_num" = "1")'
_DDL_SR = [
    """CREATE TABLE IF NOT EXISTS log_snapshot(
      dag_id VARCHAR(190) NOT NULL, task_id VARCHAR(190) NOT NULL, run_id VARCHAR(256) NOT NULL,
      snapshot_json STRING, created_at DATETIME)
    ENGINE=OLAP PRIMARY KEY(dag_id, task_id, run_id)
    DISTRIBUTED BY HASH(dag_id) BUCKETS 1 """ + _REPLICATION,
    """CREATE TABLE IF NOT EXISTS analysis_record(
      dag_id VARCHAR(190) NOT NULL, task_id VARCHAR(190) NOT NULL, run_id VARCHAR(256) NOT NULL,
      result_json STRING, model VARCHAR(64), created_at DATETIME)
    ENGINE=OLAP PRIMARY KEY(dag_id, task_id, run_id)
    DISTRIBUTED BY HASH(dag_id) BUCKETS 1 """ + _REPLICATION,
    """CREATE TABLE IF NOT EXISTS knowledge_case(
      category VARCHAR(64), symptom STRING, root_cause STRING, solution STRING,
      source_run VARCHAR(256), created_at DATETIME)
    ENGINE=OLAP DUPLICATE KEY(category)
    DISTRIBUTED BY HASH(category) BUCKETS 1 """ + _REPLICATION,
    """CREATE TABLE IF NOT EXISTS alert_log(
      dag_id VARCHAR(190), task_id VARCHAR(190), run_id VARCHAR(256),
      alert_at DATETIME, snapshot_built TINYINT)
    ENGINE=OLAP DUPLICATE KEY(dag_id, task_id)
    DISTRIBUTED BY HASH(dag_id) BUCKETS 1 """ + _REPLICATION,
]


def _now():
    return datetime.now().replace(microsecond=0)


def _ref_tuple(ref):
    return (ref.dag_id, ref.task_id, ref.run_id or "")


def _parse_history(rows) -> list:
    """(run_id, result_json, created_at) -> [{run_id, category, summary, created_at}]"""
    out = []
    for run_id, rj, created_at in rows:
        try:
            d = json.loads(rj)
        except (json.JSONDecodeError, TypeError):
            continue
        out.append({"run_id": run_id, "category": d.get("category"),
                    "summary": d.get("summary"), "created_at": str(created_at)})
    return out


class _StarRocksBackend:
    def __init__(self):
        import pymysql
        self._lock = threading.Lock()
        self._conn = pymysql.connect(
            host=os.getenv("SR_HOST", "127.0.0.1"),
            port=int(os.getenv("SR_PORT", "9030")),
            user=os.getenv("SR_USER", "dq_agent"),
            password=os.getenv("SR_PASSWORD", ""),
            database=os.getenv("SR_DB", "agent_meta"),
            charset="utf8mb4", autocommit=True, connect_timeout=5,
        )
        with self._lock, self._conn.cursor() as cur:
            for ddl in _DDL_SR:
                cur.execute(ddl)

    def _exec(self, sql, args=(), fetch=False):
        with self._lock:
            self._conn.ping(reconnect=True)
            with self._conn.cursor() as cur:
                cur.execute(sql, args)
                return cur.fetchall() if fetch else None

    def upsert_snapshot(self, ref, snapshot):
        self._exec(
            "INSERT INTO log_snapshot(dag_id, task_id, run_id, snapshot_json, created_at)"
            " VALUES(%s,%s,%s,%s,%s)",
            _ref_tuple(ref) + (json.dumps(asdict(snapshot), ensure_ascii=False), _now()),
        )

    def upsert_analysis(self, ref, result):
        self._exec(
            "INSERT INTO analysis_record(dag_id, task_id, run_id, result_json, model, created_at)"
            " VALUES(%s,%s,%s,%s,%s,%s)",
            _ref_tuple(ref) + (json.dumps(asdict(result), ensure_ascii=False),
                               result.model, _now()),
        )

    def get_analysis(self, ref, ttl_hours):
        rows = self._exec(
            "SELECT result_json, created_at FROM analysis_record"
            " WHERE dag_id=%s AND task_id=%s AND run_id=%s", _ref_tuple(ref), fetch=True)
        if not rows:
            return None
        result_json, created_at = rows[0]
        if created_at and created_at < _now() - timedelta(hours=ttl_hours):
            return None
        return json.loads(result_json)

    def record_alert(self, ref, snapshot_built):
        self._exec(
            "INSERT INTO alert_log(dag_id, task_id, run_id, alert_at, snapshot_built)"
            " VALUES(%s,%s,%s,%s,%s)", _ref_tuple(ref) + (_now(), int(snapshot_built)))

    def recent_alerts(self, hours):
        rows = self._exec(
            "SELECT dag_id, task_id, run_id, alert_at, snapshot_built FROM alert_log"
            " WHERE alert_at > %s ORDER BY alert_at DESC", (_now() - timedelta(hours=hours),),
            fetch=True)
        keys = ("dag_id", "task_id", "run_id", "alert_at", "snapshot_built")
        return [dict(zip(keys, r)) for r in rows]

    # ---- B2/B3：历史上下文与知识库 ----
    def task_history(self, dag_id, task_id, exclude_run=None, limit=5):
        rows = self._exec(
            "SELECT run_id, result_json, created_at FROM analysis_record"
            " WHERE dag_id=%s AND task_id=%s AND run_id<>%s"
            " ORDER BY created_at DESC LIMIT %s",
            (dag_id, task_id, exclude_run or "", limit), fetch=True)
        return _parse_history(rows)

    def find_similar_by_signature(self, sig, exclude_run=None, limit=3):
        """错误签名精确匹配历史快照（同签名=同一错误），返回已分析过的案例"""
        rows = self._exec(
            "SELECT dag_id, task_id, run_id FROM log_snapshot"
            " WHERE snapshot_json LIKE %s AND run_id<>%s LIMIT 20",
            (f"%{sig}%", exclude_run or ""), fetch=True)
        out = []
        for dag_id, task_id, run_id in rows:
            r = self._exec(
                "SELECT result_json FROM analysis_record"
                " WHERE dag_id=%s AND task_id=%s AND run_id=%s",
                (dag_id, task_id, run_id), fetch=True)
            if not r:
                continue
            try:
                d = json.loads(r[0][0])
            except (json.JSONDecodeError, TypeError):
                continue
            out.append({"dag_id": dag_id, "task_id": task_id, "run_id": run_id,
                        "category": d.get("category"), "summary": d.get("summary")})
            if len(out) >= limit:
                break
        return out

    def add_knowledge_case(self, category, symptom, root_cause, solution, source_run):
        if self.has_case(source_run):
            return
        self._exec(
            "INSERT INTO knowledge_case(category, symptom, root_cause, solution,"
            " source_run, created_at) VALUES(%s,%s,%s,%s,%s,%s)",
            (category, symptom, root_cause, solution, source_run, _now()))

    def has_case(self, source_run):
        return bool(self._exec(
            "SELECT 1 FROM knowledge_case WHERE source_run=%s LIMIT 1",
            (source_run,), fetch=True))


class _SqliteBackend:
    def __init__(self):
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(settings.db_path, check_same_thread=False)
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS snapshots(
          dag_id TEXT, task_id TEXT, run_id TEXT, snapshot_json TEXT, created_at TEXT,
          PRIMARY KEY(dag_id, task_id, run_id));
        CREATE TABLE IF NOT EXISTS analyses(
          dag_id TEXT, task_id TEXT, run_id TEXT, result_json TEXT, model TEXT, created_at TEXT,
          PRIMARY KEY(dag_id, task_id, run_id));
        CREATE TABLE IF NOT EXISTS alerts(
          dag_id TEXT, task_id TEXT, run_id TEXT, alert_at TEXT, snapshot_built INTEGER);
        CREATE TABLE IF NOT EXISTS knowledge_cases(
          id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT, symptom TEXT,
          root_cause TEXT, solution TEXT, source_run TEXT, created_at TEXT);
        CREATE INDEX IF NOT EXISTS idx_alerts_time ON alerts(alert_at);
        """)
        self._conn.row_factory = sqlite3.Row

    def upsert_snapshot(self, ref, snapshot):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?)",
                _ref_tuple(ref) + (json.dumps(asdict(snapshot), ensure_ascii=False),
                                   _now().isoformat()))

    def upsert_analysis(self, ref, result):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO analyses VALUES(?,?,?,?,?,?)",
                _ref_tuple(ref) + (json.dumps(asdict(result), ensure_ascii=False),
                                   result.model, _now().isoformat()))

    def get_analysis(self, ref, ttl_hours):
        with self._lock:
            row = self._conn.execute(
                "SELECT result_json, created_at FROM analyses"
                " WHERE dag_id=? AND task_id=? AND run_id=?", _ref_tuple(ref)).fetchone()
        if not row:
            return None
        try:
            if datetime.fromisoformat(row["created_at"]) < _now() - timedelta(hours=ttl_hours):
                return None
        except ValueError:
            pass
        return json.loads(row["result_json"])

    def record_alert(self, ref, snapshot_built):
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO alerts VALUES(?,?,?,?,?)",
                _ref_tuple(ref) + (_now().isoformat(), int(snapshot_built)))

    def recent_alerts(self, hours):
        since = (_now() - timedelta(hours=hours)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM alerts WHERE alert_at > ? ORDER BY alert_at DESC",
                (since,)).fetchall()
        return [dict(r) for r in rows]

    # ---- B2/B3：历史上下文与知识库（与 SR 后端同 API）----
    def task_history(self, dag_id, task_id, exclude_run=None, limit=5):
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id, result_json, created_at FROM analyses"
                " WHERE dag_id=? AND task_id=? AND run_id<>?"
                " ORDER BY created_at DESC LIMIT ?",
                (dag_id, task_id, exclude_run or "", limit)).fetchall()
        return _parse_history([(r[0], r[1], r[2]) for r in rows])

    def find_similar_by_signature(self, sig, exclude_run=None, limit=3):
        with self._lock:
            rows = self._conn.execute(
                "SELECT dag_id, task_id, run_id FROM snapshots"
                " WHERE snapshot_json LIKE ? AND run_id<>? LIMIT 20",
                (f"%{sig}%", exclude_run or "")).fetchall()
        out = []
        for dag_id, task_id, run_id in rows:
            row = None
            with self._lock:
                row = self._conn.execute(
                    "SELECT result_json FROM analyses"
                    " WHERE dag_id=? AND task_id=? AND run_id=?",
                    (dag_id, task_id, run_id)).fetchone()
            if not row:
                continue
            try:
                d = json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                continue
            out.append({"dag_id": dag_id, "task_id": task_id, "run_id": run_id,
                        "category": d.get("category"), "summary": d.get("summary")})
            if len(out) >= limit:
                break
        return out

    def add_knowledge_case(self, category, symptom, root_cause, solution, source_run):
        if self.has_case(source_run):
            return
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO knowledge_cases(category, symptom, root_cause, solution,"
                " source_run, created_at) VALUES(?,?,?,?,?,?)",
                (category, symptom, root_cause, solution, source_run, _now().isoformat()))

    def has_case(self, source_run):
        with self._lock:
            return bool(self._conn.execute(
                "SELECT 1 FROM knowledge_cases WHERE source_run=? LIMIT 1",
                (source_run,)).fetchone())


_backend_instance = None


def _backend():
    global _backend_instance
    if _backend_instance is None:
        forced = settings.get("storage.backend", "starrocks")
        if forced != "sqlite" and os.getenv("SR_HOST"):
            try:
                _backend_instance = _StarRocksBackend()
                log.info("存储后端: StarRocks %s/%s", os.getenv("SR_DB"), os.getenv("SR_HOST"))
            except Exception as e:
                log.warning("StarRocks 不可用（%s），降级本地 SQLite", e)
                _backend_instance = _SqliteBackend()
        else:
            _backend_instance = _SqliteBackend()
    return _backend_instance


def _degrade_and_retry(method, args):
    """SR 运行中断连：整体切到 SQLite 并重放本次操作（进程内 sticky）"""
    global _backend_instance
    log.warning("StarRocks 操作 %s 失败，降级 SQLite", method)
    _backend_instance = _SqliteBackend()
    return getattr(_backend_instance, method)(*args)


def _call(method, *args):
    b = _backend()
    try:
        return getattr(b, method)(*args)
    except Exception:
        if isinstance(b, _StarRocksBackend):
            return _degrade_and_retry(method, args)
        raise


# ---- 对外 API（与旧版签名一致，调用方无感） ----
def upsert_snapshot(ref, snapshot):
    _call("upsert_snapshot", ref, snapshot)


def upsert_analysis(ref, result):
    _call("upsert_analysis", ref, result)


def get_analysis(ref, ttl_hours=None):
    return _call("get_analysis", ref,
                 ttl_hours or settings.get("limits.cache_ttl_hours", 12))


def record_alert(ref, snapshot_built):
    _call("record_alert", ref, snapshot_built)


def recent_alerts(hours=24):
    return _call("recent_alerts", hours)


def find_task_by_token(token, hours=24):
    token = token.lower()
    for a in recent_alerts(hours):
        if token in (a["task_id"] or "").lower() or token in (a["dag_id"] or "").lower():
            return (a["dag_id"], a["task_id"], a["run_id"] or None)
    return None


# ---- B2/B3 对外 API ----
def task_history(dag_id, task_id, exclude_run=None, limit=5):
    return _call("task_history", dag_id, task_id, exclude_run, limit)


def find_similar_by_signature(sig, exclude_run=None, limit=3):
    return _call("find_similar_by_signature", sig, exclude_run, limit)


def add_knowledge_case(category, symptom, root_cause, solution, source_run):
    _call("add_knowledge_case", category, symptom, root_cause, solution, source_run)
