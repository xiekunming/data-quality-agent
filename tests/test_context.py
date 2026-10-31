"""B2/B3 历史上下文与知识库测试（SQLite 后端，不依赖外部服务）"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq_agent import store                       # noqa: E402
from dq_agent.models import AnalysisResult, AttemptInfo, LogSnapshot, TaskRef  # noqa: E402


@pytest.fixture
def backend(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "settings",
                        SimpleNamespace(db_path=tmp_path / "t.db"))
    return store._SqliteBackend()


def _snap(run_id, sig="ab12cd34"):
    return LogSnapshot(
        dag_id="d1", task_id="t1", run_id=run_id, try_count=1,
        retry_pattern="all_same_error", sql_text="SELECT 1", error_segment="boom",
        attempts=[AttemptInfo(attempt=1, signature=sig)])


def _res(category="code_schema", summary="测试结论"):
    return AnalysisResult(category=category, summary=summary,
                          root_cause="r", suggestion="s", model="m", created_at="2026-10-31T10:00:00")


def test_task_history_excludes_current_run(backend, monkeypatch):
    from datetime import datetime, timedelta
    ticks = iter(datetime(2026, 10, 31, 10, 0) + timedelta(minutes=i) for i in range(10))
    monkeypatch.setattr(store, "_now", lambda: next(ticks))
    for run in ("r1", "r2", "r3"):
        ref = TaskRef("d1", "t1", run)
        backend.upsert_analysis(ref, _res(summary=f"结论-{run}"))
    hist = backend.task_history("d1", "t1", exclude_run="r3")
    assert [h["run_id"] for h in hist] == ["r2", "r1"]      # 新到旧，排除当前
    assert hist[0]["category"] == "code_schema"


def test_find_similar_by_signature(backend):
    backend.upsert_snapshot(TaskRef("d1", "t1", "r1"), _snap("r1", sig="ab12cd34"))
    backend.upsert_analysis(TaskRef("d1", "t1", "r1"), _res(summary="列拼写错误"))
    # 当前 run 快照同签名但应被排除；未分析过的快照不算案例
    backend.upsert_snapshot(TaskRef("d1", "t1", "r2"), _snap("r2", sig="ab12cd34"))

    hits = backend.find_similar_by_signature("ab12cd34", exclude_run="r2")
    assert len(hits) == 1 and hits[0]["run_id"] == "r1"
    assert hits[0]["summary"] == "列拼写错误"

    assert backend.find_similar_by_signature("ffffffff") == []


def test_knowledge_case_idempotent(backend):
    backend.add_knowledge_case("code_schema", "s", "r", "sol", "d1.t1@r1")
    backend.add_knowledge_case("code_schema", "s", "r", "sol", "d1.t1@r1")  # 重复写跳过
    assert backend.has_case("d1.t1@r1")
    with backend._lock:
        n = backend._conn.execute("SELECT COUNT(*) FROM knowledge_cases").fetchone()[0]
    assert n == 1
