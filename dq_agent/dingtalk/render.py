"""分析结论 -> 钉钉 markdown 消息"""

from ..models import AnalysisResult, TaskRef

_CONF = {"high": "高", "medium": "中", "low": "低"}


def render_result(res: AnalysisResult, ref: TaskRef, cached: bool = False) -> tuple:
    """返回 (title, markdown_text)"""
    lines = [
        f"### 🔍 {ref.dag_id}.{ref.task_id} 排查结果",
        "",
        f"**结论**：{res.summary or res.root_cause[:60]}"
        f"（置信度：{_CONF.get(res.confidence, res.confidence)}{'，命中缓存' if cached else ''}）",
        "",
        f"- **分类**：{res.category_label}",
        f"- **根因**：{res.root_cause}",
        f"- **建议**：{res.suggestion or '—'}",
        f"- **需要人工**：{'是' if res.need_human else '否'}",
    ]
    if getattr(res, "tools_used", None):
        lines.append(f"- **数据核查**：已执行 {len(res.tools_used)} 次只读库查询核实")
    similar = getattr(res, "similar_cases", None)
    if similar:
        lines += ["", "**📚 相似历史案例**（错误签名一致）"]
        lines += [f"- `{c.get('run_id', '')[:44]}`（{c.get('category', '')}）："
                  f"{(c.get('summary') or '')[:60]}" for c in similar[:3]]
    if res.evidence:
        lines += ["", "**关键证据**"]
        lines += [f"> {str(e)[:200]}" for e in res.evidence[:5]]
    lines += ["", f"`run_id={ref.run_id} | {res.model or 'GLM'} 分析于 {res.created_at}`"]
    return f"{ref.task_id} 排查结果", "\n".join(lines)


HELP_MD = """### 🤖 数据质量排查 Agent 用法
在群里 @我 并带上任务信息，例如：
- `@机器人 dag=demo_dag | task=etl_loan_report | run=manual__2026-10-18T...`（最精确）
- `@机器人 etl_loan_report`（任务名，自动匹配；同名任务在多个 DAG 时会列出候选）
- 直接粘贴告警行：`@机器人 <DAG: demo_dag>-20261023-etl_loan_report Failed`
- 光 @我 不带任务：列出最近失败的任务，回复**序号**即可选择

说明：机器人收不到引用消息的原文，引用告警后请务必带上任务名或粘贴告警行。
同一任务同一 run 的结论有缓存；排查中重复@会提示等待；超时自动转后台，完成后补推。"""


def render_help() -> tuple:
    return "排查Agent使用说明", HELP_MD


def render_candidates(refs: list, note: str) -> tuple:
    """候选任务列表 -> (title, markdown)，用户回复序号选择"""
    lines = [note, ""]
    for i, r in enumerate(refs, 1):
        run = f"（run={r.run_id}）" if r.run_id else ""
        lines.append(f"{i}. `{r.dag_id}.{r.task_id}`{run}")
    lines += ["", "直接回复**序号**选择，或带上任务名重新 @我（10 分钟内有效）。"]
    return "任务选择", "\n".join(lines)
