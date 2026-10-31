"""CLI 冒烟：不用钉钉直接在终端跑一次完整排查（含只读核查工具与历史上下文）

用法：
  # 离线回放（推荐第一次跑）：只用内置样例日志，不需要本机 Airflow/StarRocks
  python scripts/analyze_cli.py --sample                      # 默认 e01 语法错误样本
  python scripts/analyze_cli.py --sample e03_not_null_violation

  # 真实任务（读本机 Airflow 日志目录）
  python scripts/analyze_cli.py <dag_id> <task_id>            # 最近一次 run
  python scripts/analyze_cli.py <dag_id> <task_id> <run_id> --force
  python scripts/analyze_cli.py <dag_id> <task_id> --no-llm   # 只看快照+提示词预览（不花token）
"""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dq_agent import analyzer, store                    # noqa: E402
from dq_agent.dingtalk import render                    # noqa: E402
from dq_agent.log_extractor import build_snapshot       # noqa: E402
from dq_agent.log_locator import locate_task_logs       # noqa: E402
from dq_agent.models import TaskRef                     # noqa: E402

SAMPLES_LOG_DIR = PROJECT_ROOT / "samples" / "logs"
SAMPLE_DAG = "dq_error_demo"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dag_id", nargs="?", help="DAG id（--sample 模式下不需要）")
    ap.add_argument("task_id", nargs="?", help="task id")
    ap.add_argument("run_id", nargs="?", default=None)
    ap.add_argument("--sample", nargs="?", const="e01_sql_syntax_error", metavar="TASK",
                    help=f"离线回放 samples/logs 里的样例任务（默认 e01_sql_syntax_error；"
                         f"可选：{', '.join(sorted(p.name for p in SAMPLES_LOG_DIR.glob('run_id=*/task_id=*')))})")
    ap.add_argument("--force", action="store_true", help="忽略缓存重新分析")
    ap.add_argument("--no-llm", action="store_true", help="不调 LLM，只打印快照与提示词预览")
    ap.add_argument("--json", action="store_true", help="以 JSON 输出结果")
    args = ap.parse_args()

    logs_dir = None
    if args.sample:
        ref = TaskRef(SAMPLE_DAG, args.sample, args.run_id)
        logs_dir = SAMPLES_LOG_DIR          # 从内置样例目录读日志，不碰本机 Airflow
        print(f"[离线回放] 样例任务 {SAMPLE_DAG}.{args.sample}")
    elif args.dag_id and args.task_id:
        ref = TaskRef(args.dag_id, args.task_id, args.run_id)
    else:
        ap.error("请指定 --sample 或 <dag_id> <task_id>（--help 看用法）")

    logs = locate_task_logs(ref, logs_dir=logs_dir)
    ref.run_id = logs.run_id
    print(f"run_id={logs.run_id}  attempts={[no for no, _ in logs.files]}")

    snap = build_snapshot(ref, logs)
    store.upsert_snapshot(ref, snap)
    print(f"快照: operator={snap.operator} try_count={snap.try_count} "
          f"retry_pattern={snap.retry_pattern}")
    print(f"      sql={len(snap.sql_text or '')} chars, error={len(snap.error_segment)} chars")

    if args.no_llm:
        msgs = analyzer.build_messages(snap)
        print("\n==== system prompt（前 500 字）====\n" + msgs[0]["content"][:500])
        print("\n==== user payload（前 1200 字）====")
        print(json.dumps(json.loads(msgs[1]["content"]), ensure_ascii=False, indent=2)[:1200])
        return

    if not args.force:
        if store.get_analysis(ref):
            print("\n[命中缓存，--force 可强制重析]")
    from dq_agent.orchestrator import build_analysis_context, record_outcome
    ctx = build_analysis_context(ref, snap)
    result = analyzer.analyze(snap, ctx)
    result.similar_cases = ctx.get("similar_cases", [])
    store.upsert_analysis(ref, result)
    record_outcome(ref, result)

    if result.tools_used:
        print("\n核查工具调用:")
        for t in result.tools_used:
            print(f"  - {t}")

    if args.json:
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return

    title, md = render.render_result(result, ref)
    print("\n" + md)


if __name__ == "__main__":
    main()
