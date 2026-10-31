"""@消息任务定位解析测试（resolve_task_ref：标识行/告警行/task=/裸词/歧义）"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq_agent import orchestrator                     # noqa: E402


def _mk_logs(base: Path, dag: str, task: str, run: str = "r1"):
    d = base / f"dag_id={dag}" / f"run_id={run}" / f"task_id={task}"
    d.mkdir(parents=True)
    (d / "attempt=1.log").write_text("x")


@pytest.fixture
def fake_sources(tmp_path, monkeypatch):
    """隔离外部依赖：告警表置空、日志目录重定向到临时目录"""
    monkeypatch.setattr(orchestrator.store, "find_task_by_token", lambda *a, **k: None)
    monkeypatch.setattr(
        orchestrator, "search_task_dirs",
        lambda token, logs_dir=None: _fake_search(tmp_path, token))
    return tmp_path


def _fake_search(base: Path, token: str):
    hits = []
    for td in sorted(base.glob(f"dag_id=*/run_id=*/task_id=*{token}*")):
        task_id = td.name.removeprefix("task_id=")
        dag_id = td.parent.parent.name.removeprefix("dag_id=")
        hits.append((dag_id, task_id, td.stat().st_mtime))
    exact = [h for h in hits if h[1] == token]
    pool = exact or hits
    return sorted(pool, key=lambda h: -h[2])


def test_full_spec(fake_sources):
    ref, cands = orchestrator.resolve_task_ref(
        "排查 dag=demo_dag | task=etl_loan_report | run=manual__x")
    assert ref and ref.dag_id == "demo_dag" and ref.task_id == "etl_loan_report"
    assert ref.run_id == "manual__x" and not cands


def test_alarm_line_paste(fake_sources):
    """生产告警行直接粘贴：2026-10-24 14:04:28: <DAG: XXX>-20261023-aaaa  Failed"""
    ref, cands = orchestrator.resolve_task_ref(
        "2026-10-24 14:04:28: <DAG: demo_dag>-20261023-etl_loan_report  Failed")
    assert ref and ref.dag_id == "demo_dag" and ref.task_id == "etl_loan_report" and not cands


def test_bare_name_exact_prefers_exact(fake_sources):
    """裸词同时命中精确名与更长名（ads_x / ads_x_check）时取精确匹配"""
    _mk_logs(fake_sources, "d1", "ads_x")
    _mk_logs(fake_sources, "d1", "ads_x_check")
    ref, cands = orchestrator.resolve_task_ref("ads_x")
    assert ref and ref.task_id == "ads_x" and not cands


def test_same_task_multiple_dags_is_ambiguous(fake_sources):
    _mk_logs(fake_sources, "dag_a", "etl_load")
    _mk_logs(fake_sources, "dag_b", "etl_load")
    ref, cands = orchestrator.resolve_task_ref("etl_load")
    assert ref is None and len(cands) == 2
    assert {(c.dag_id, c.task_id) for c in cands} == {("dag_a", "etl_load"), ("dag_b", "etl_load")}


def test_task_only_via_alert_table(fake_sources, monkeypatch):
    monkeypatch.setattr(orchestrator.store, "find_task_by_token",
                        lambda *a, **k: ("d_alert", "ads_x", "run9"))
    ref, _ = orchestrator.resolve_task_ref("task=ads_x 查一下")
    assert ref and ref.dag_id == "d_alert" and ref.run_id == "run9"


def test_plain_chinese_no_match(fake_sources):
    ref, cands = orchestrator.resolve_task_ref("今天为啥挂了")
    assert ref is None and not cands
