"""编排：任务定位解析 / 预采集 / 缓存检查 / 分析"""

import logging
import re

from . import analyzer, store
from .config import settings
from .log_extractor import build_snapshot
from .log_locator import _collect_attempts, locate_task_logs, search_task_dirs
from .models import AnalysisResult, TaskRef

log = logging.getLogger(__name__)

_RESULT_FIELDS = ("category", "confidence", "summary", "root_cause", "evidence",
                  "suggestion", "need_human", "model", "created_at", "tools_used",
                  "similar_cases")


def result_from_dict(d: dict) -> AnalysisResult:
    """缓存 dict -> AnalysisResult；旧条目缺新字段时安全补空"""
    r = AnalysisResult(**{k: d.get(k) for k in _RESULT_FIELDS})
    r.tools_used = r.tools_used or []
    r.similar_cases = r.similar_cases or []
    return r

# 告警/引用消息里的任务标识：dag=x | task=y | run=z（分隔符兼容 | , 空格）
_REF_FULL = re.compile(
    r"dag[_ ]?(?:id)?=([\w.\-]+).*?task[_ ]?(?:id)?=([\w.\-]+)"
    r"(?:.*?run[_ ]?(?:id)?=([\w:\.\-+]+))?",
    re.I,
)
_REF_TASK_ONLY = re.compile(r"task[_ ]?(?:id)?=([\w.\-]+)", re.I)
# 现网告警行：2026-10-24 14:04:28: <DAG: demo_dag>-20261023-etl_loan_report  Failed
_REF_ALARM_LINE = re.compile(r"<DAG:\s*([\w.\-]+)>-\d{8}-([\w.\-]+)")

# 失败标记统一小写匹配（Airflow 日志里 Marking task as FAILED 全大写）
_FAILED_MARKS = ("task failed with exception", "marking task as failed",
                 "failed to execute job")


def _first_meaningful_token(text: str) -> str | None:
    """裸任务名探测：只认 ASCII 词（字母/数字/._-），排除中文与纯数字"""
    for tok in re.split(r"[\s,，。？！@：:]+", text.strip()):
        if re.fullmatch(r"[A-Za-z0-9_.\-]{4,}", tok) and not tok.isdigit():
            return tok
    return None


def _ref_from_hits(task_id: str, hits: list) -> tuple:
    """从日志目录命中里取任务；同任务名存在于多个 DAG 时返回 (None, 候选列表)"""
    exact = [(d, t) for d, t, _ in hits if t == task_id]
    dags = sorted({d for d, _ in exact})
    if len(dags) > 1:
        return None, [TaskRef(d, task_id) for d in dags[:5]]
    if exact:
        return TaskRef(exact[0][0], exact[0][1]), []
    d, t, _ = hits[0]
    return TaskRef(d, t), []


def resolve_task_ref(text: str) -> tuple:
    """解析@消息 → (TaskRef|None, 歧义候选列表)。带 dag 的输入不会歧义。"""
    m = _REF_FULL.search(text)
    if m:
        return TaskRef(m.group(1), m.group(2), m.group(3)), []

    m = _REF_ALARM_LINE.search(text)
    if m:
        return TaskRef(m.group(1), m.group(2)), []

    m = _REF_TASK_ONLY.search(text)
    task_id = m.group(1) if m else _first_meaningful_token(text)
    if not task_id:
        return None, []

    try:
        hit = store.find_task_by_token(task_id)
    except Exception:
        hit = None  # 存储不可用不应阻断解析
    if hit:
        return TaskRef(hit[0], hit[1], hit[2]), []

    dirs = search_task_dirs(task_id)
    if not dirs:
        return None, []
    return _ref_from_hits(task_id, dirs)


def parse_task_ref(text: str) -> TaskRef | None:
    ref, _ = resolve_task_ref(text)
    return ref


def recent_failure_candidates(limit: int = 5) -> list:
    """最近失败任务候选：优先告警表（预采集写入），为空时扫描本地日志目录兜底"""
    out, seen, scanned = [], set(), set()

    def push(dag_id, task_id, run_id):
        if (dag_id, task_id) in seen:
            return
        seen.add((dag_id, task_id))
        out.append(TaskRef(dag_id, task_id, run_id or None))

    try:
        for a in store.recent_alerts(24):
            push(a["dag_id"], a["task_id"], a["run_id"])
            if len(out) >= limit:
                return out
    except Exception:
        log.warning("读取告警记录失败，改用日志目录扫描", exc_info=True)

    base = settings.logs_dir
    if not base.is_dir():
        return out
    dirs = sorted(base.glob("dag_id=*/run_id=*/task_id=*"),
                  key=lambda p: p.stat().st_mtime, reverse=True)
    for td in dirs:
        if len(out) >= limit:
            break
        key = (td.parent.parent.name.removeprefix("dag_id="),
               td.name.removeprefix("task_id="))
        if key in seen or key in scanned:
            continue
        scanned.add(key)   # ⚠️ 只标记已扫描；不能提前进 seen，否则 push 会拒绝追加
        attempts = _collect_attempts(td)
        if not attempts:
            continue
        try:
            tail = attempts[-1][1].read_text(errors="replace")[-8000:].lower()
        except OSError:
            continue
        if any(mark in tail for mark in _FAILED_MARKS):
            push(key[0], key[1], td.parent.name.removeprefix("run_id="))
    return out


def candidate_by_index(idx: int) -> TaskRef | None:
    """序号选择：与 recent_failure_candidates 同序（序号基于最近一次发出的列表）"""
    cands = recent_failure_candidates(9)
    return cands[idx - 1] if 1 <= idx <= len(cands) else None


def check_cached(ref: TaskRef) -> AnalysisResult | None:
    """定位日志并查缓存，不调 LLM。日志缺失抛 LogNotFound"""
    logs = locate_task_logs(ref)
    ref.run_id = logs.run_id
    cached = store.get_analysis(ref)
    return result_from_dict(cached) if cached else None


def precollect(ref: TaskRef) -> bool:
    """告警预采集：只建快照不调 LLM（毫秒级，@排查时直接命中）"""
    try:
        logs = locate_task_logs(ref)
        snap = build_snapshot(ref, logs)
        ref.run_id = logs.run_id
        store.upsert_snapshot(ref, snap)
        store.record_alert(ref, snapshot_built=True)
        log.info("预采集完成 %s.%s run=%s", ref.dag_id, ref.task_id, ref.run_id)
        return True
    except Exception as e:
        log.warning("预采集失败 %s.%s: %s", ref.dag_id, ref.task_id, e)
        store.record_alert(ref, snapshot_built=False)
        return False


def build_analysis_context(ref: TaskRef, snap) -> dict:
    """B2/B3：给模型的历史上下文——该任务过往失败 + 同错误签名的历史案例"""
    ctx = {}
    try:
        history = store.task_history(ref.dag_id, ref.task_id, exclude_run=ref.run_id)
        if history:
            ctx["task_history"] = history
    except Exception:
        log.warning("查询任务历史失败", exc_info=True)
    try:
        sig = snap.attempts[-1].signature if snap.attempts else None
        if sig:
            similar = store.find_similar_by_signature(sig, exclude_run=ref.run_id)
            if similar:
                ctx["similar_cases"] = similar
    except Exception:
        log.warning("签名相似召回失败", exc_info=True)
    return ctx


def record_outcome(ref: TaskRef, result) -> None:
    """分析结论写入知识库（同一 run 幂等）"""
    try:
        store.add_knowledge_case(
            result.category, result.summary, result.root_cause, result.suggestion,
            f"{ref.dag_id}.{ref.task_id}@{ref.run_id}")
    except Exception:
        log.warning("知识库写入失败", exc_info=True)


def analyze_task(ref: TaskRef, force: bool = False) -> tuple:
    """返回 (AnalysisResult, cached: bool)；日志未找到时抛 LogNotFound"""
    logs = locate_task_logs(ref)
    ref.run_id = logs.run_id
    if not force:
        cached = store.get_analysis(ref)
        if cached:
            return result_from_dict(cached), True
    snap = build_snapshot(ref, logs)
    store.upsert_snapshot(ref, snap)
    ctx = build_analysis_context(ref, snap)
    result = analyzer.analyze(snap, ctx)
    result.similar_cases = ctx.get("similar_cases", [])
    store.upsert_analysis(ref, result)
    record_outcome(ref, result)
    return result, False
