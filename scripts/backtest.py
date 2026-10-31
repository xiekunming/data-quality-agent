"""离线回测：扫描本地日志里的历史失败任务，批量跑分析并生成人工对照报告

用法：
  python scripts/backtest.py --dry-run            # 只列出候选失败任务
  python scripts/backtest.py --limit 20           # 实跑（需 GLM_API_KEY）
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq_agent import analyzer, store              # noqa: E402
from dq_agent.config import settings              # noqa: E402
from dq_agent.log_extractor import build_snapshot  # noqa: E402
from dq_agent.log_locator import TaskLogs, _collect_attempts, resolve_latest_run  # noqa: E402
from dq_agent.models import TaskRef               # noqa: E402
from dq_agent.orchestrator import _FAILED_MARKS, build_analysis_context, record_outcome  # noqa: E402


def find_failed_tasks(base: Path) -> list:
    """每个 (dag, task) 只取最近一次出现过失败的 run"""
    latest = {}
    for task_dir in base.glob("dag_id=*/run_id=*/task_id=*"):
        attempts = _collect_attempts(task_dir)
        if not attempts:
            continue
        try:
            tail = attempts[-1][1].read_text(errors="replace")[-8000:].lower()
            if not any(mark in tail for mark in _FAILED_MARKS):
                continue
        except OSError:
            continue
        dag_id = task_dir.parent.parent.name.removeprefix("dag_id=")
        run_id = task_dir.parent.name.removeprefix("run_id=")
        key = (dag_id, task_dir.name.removeprefix("task_id="))
        mtime = task_dir.stat().st_mtime
        if key not in latest or mtime > latest[key][2]:
            latest[key] = (run_id, attempts, mtime)
    return sorted(latest.items(), key=lambda kv: -kv[1][2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = settings.logs_dir
    tasks = find_failed_tasks(base)[: args.limit]
    print(f"日志目录 {base} 下共 {len(tasks)} 个失败任务（已按时间取每个任务最近一次失败）\n")

    if args.dry_run:
        for (dag_id, task_id), (run_id, attempts, _) in tasks:
            print(f"  {dag_id}.{task_id}  run={run_id}  attempts={len(attempts)}")
        return

    report_dir = Path(__file__).resolve().parents[1] / "reports"
    report_dir.mkdir(exist_ok=True)
    out = report_dir / f"backtest-{datetime.now():%Y%m%d-%H%M}.md"
    rows, ok = [], 0
    for (dag_id, task_id), (run_id, attempts, _) in tasks:
        ref = TaskRef(dag_id, task_id, run_id)
        try:
            snap = build_snapshot(ref, TaskLogs(run_id, attempts))
            store.upsert_snapshot(ref, snap)          # 快照落库：供签名召回
            ctx = build_analysis_context(ref, snap)
            res = analyzer.analyze(snap, ctx)
            res.similar_cases = ctx.get("similar_cases", [])
            store.upsert_analysis(ref, res)           # 结论落库：供缓存/任务历史
            record_outcome(ref, res)
            ok += 1
            rows.append((dag_id, task_id, run_id, res.category, res.confidence,
                         res.summary, res.root_cause.replace("\n", " ")))
            print(f"  ✔ {dag_id}.{task_id}: [{res.category}] {res.summary}")
        except Exception as e:
            rows.append((dag_id, task_id, run_id, "ERROR", "-", str(e), ""))
            print(f"  ✘ {dag_id}.{task_id}: {e}")

    with out.open("w", encoding="utf-8") as f:
        f.write(f"# 回测报告 {datetime.now():%Y-%m-%d %H:%M}\n\n")
        f.write(f"共 {len(rows)} 个失败任务，成功分析 {ok} 个。请人工核对下表根因是否正确。\n\n")
        f.write("| dag.task | run | 分类 | 置信度 | 一句话结论 | 根因 |\n|---|---|---|---|---|---|\n")
        for dag_id, task_id, run_id, cat, conf, summary, rc in rows:
            f.write(f"| {dag_id}.{task_id} | {run_id[:40]} | {cat} | {conf} "
                    f"| {summary} | {rc[:120]} |\n")
    print(f"\n报告已生成: {out}")


if __name__ == "__main__":
    main()
