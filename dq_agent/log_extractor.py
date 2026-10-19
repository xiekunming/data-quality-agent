"""日志规则提取器：原始 attempt 日志 -> 结构化快照（脱敏 + 截断 + attempt 签名对比）

全部为确定性规则代码，不调 LLM。Airflow 本地日志格式（LocalTaskJob + 文件 handler）稳定，
提取失败的段会整体降级为"取尾部 N 行"，保证快照永远能生成。
"""

import hashlib
import re
from pathlib import Path

from .config import settings
from .log_locator import TaskLogs
from .models import AttemptInfo, LogSnapshot, TaskRef

# Airflow 日志行前缀：[2026-10-18T17:19:42.844+0800] {module.py:123} LEVEL - ...
_TS = re.compile(r"^\[\d{4}-\d{2}-\d{2}T[0-9:.]+\+\d{4}\]\s*\{[\w.]+:\d+\}\s*(\w+)\s+-\s?")
_RE_OPERATOR = re.compile(r"INFO - Executing <Task\((\w+)\): (\w+)>")
_RE_SUBDIR = re.compile(r"--subdir',\s*'([^']+)'")
_RE_ATTEMPT = re.compile(r"Starting attempt (\d+) of (\d+)")
_RE_SQL_EXEC = re.compile(r"\{sql\.py:\d+\}\s+INFO - (?:Executing|Running): ?(.*)$")
_RE_ERR_MARKER = re.compile(r"ERROR - Task failed with exception")

# ---- 脱敏 ----
_MASKS = [
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "***@***"),                 # 邮箱
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "*.*.*.*"),             # IPv4
    (re.compile(r"(?i)(password|passwd|secret|token)\s*[=:]\s*\S+"), r"\1=***"),
    (re.compile(r"/Users/[\w.-]+/"), "~/"),                              # 本机用户名
]


def mask_text(text: str) -> str:
    for pat, rep in _MASKS:
        text = pat.sub(rep, text)
    return text


def _truncate(text: str, max_chars: int, head: int, tail: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:head]}\n...[中间部分截断]...\n{text[-tail:]}"


def _collapse_airflow_frames(error_text: str) -> str:
    """traceback 里成片的 airflow 框架栈帧压缩成一行，保留业务/驱动栈帧"""
    lines, drop, out = error_text.splitlines(), 0, []
    for line in lines:
        if '/site-packages/airflow/' in line and re.match(r'\s*File "', line):
            drop += 1
            continue
        if drop:
            out.append(f"  [... airflow 框架栈帧 x{drop} 已省略 ...]")
            drop = 0
        out.append(line)
    if drop:
        out.append(f"  [... airflow 框架栈帧 x{drop} 已省略 ...]")
    return "\n".join(out)


def parse_attempt(text: str) -> dict:
    """解析单个 attempt 日志，返回元信息 / SQL 段 / 错误段（未脱敏）"""
    lines = text.splitlines()
    meta = {"operator": None, "task_echo": None, "dag_file": None,
            "attempt_no": None, "total": None}

    sql_lines, in_sql = [], False
    err_start = None
    err_marks = []

    for i, line in enumerate(lines):
        if not meta["operator"]:
            m = _RE_OPERATOR.search(line)
            if m:
                meta["operator"], meta["task_echo"] = m.group(1), m.group(2)
        if not meta["dag_file"]:
            m = _RE_SUBDIR.search(line)
            if m:
                meta["dag_file"] = m.group(1)
        if not meta["attempt_no"]:
            m = _RE_ATTEMPT.search(line)
            if m:
                meta["attempt_no"], meta["total"] = int(m.group(1)), int(m.group(2))

        m = _RE_SQL_EXEC.search(line)
        if m:
            in_sql = True
            sql_lines.append(m.group(1))
            continue
        if in_sql:
            if _TS.match(line):          # SQL 正文结束：回到带时间戳的日志行
                in_sql = False
            else:
                sql_lines.append(line)
                continue

        if " ERROR - " in line:
            err_marks.append(i)
            if _RE_ERR_MARKER.search(line) and err_start is None:
                err_start = i

    if err_start is None:
        err_start = err_marks[0] if err_marks else max(0, len(lines) - 60)
    error = "\n".join(lines[err_start:]).strip()
    if not error:
        error = "\n".join(lines[-60:]).strip()

    return {
        "meta": meta,
        "sql": "\n".join(sql_lines).strip() or None,
        "error": error,
        "has_error_marker": bool(err_marks),
    }


# traceback 末尾的异常类行（列首、以 Error/Exception 等结尾的限定名开头），如
# mysql.connector.errors.DatabaseError: 1064 (HY000): ...
_RE_EXC_LINE = re.compile(r"^[\w.]+(?:Error|Exception|Interrupt|Exit|Timeout)\b.*$")


def error_signature(error: str) -> str:
    """错误身份哈希：优先只用异常类行，避免 UP_FOR_RETRY/FAILED 状态行、
    job id、PID 等每次重试必然不同的噪声干扰"错误是否一致"的判断；
    没有异常行（如被 kill）时退化为整段归一化哈希。"""
    exc_lines = [l for l in error.splitlines() if _RE_EXC_LINE.match(l)]
    if exc_lines:
        return hashlib.sha256("\n".join(exc_lines).encode()).hexdigest()[:16]
    norm = re.sub(r"^\[[^\]]*\]\s*", "", error, flags=re.M)   # 行首时间戳
    norm = re.sub(r"job \d+", "job N", norm)
    norm = re.sub(r"\(\d+\)", "(N)", norm)                    # PID / 作业号
    norm = re.sub(r"\d{8}T\d{6}", "TS", norm)
    norm = re.sub(r"\s+", " ", norm).strip()
    return hashlib.sha256(norm.encode()).hexdigest()[:16]


def build_snapshot(ref: TaskRef, logs: TaskLogs) -> LogSnapshot:
    """读取全部 attempt（>8 个时只取首尾），生成脱敏截断后的快照"""
    entries = logs.files
    if len(entries) > 8:
        entries = entries[:2] + entries[-6:]
    max_err = settings.get("extract.error_max_chars", 12000)
    max_sql = settings.get("extract.sql_max_chars", 20000)

    attempts, parsed_latest, sql_latest = [], None, None
    for no, _path in entries:
        parsed = parse_attempt(logs.read(no))
        if no == logs.last_attempt:
            parsed_latest, sql_latest = parsed, parsed["sql"]
        attempts.append(AttemptInfo(attempt=no, signature=error_signature(parsed["error"])))

    sigs = {a.signature for a in attempts}
    pattern = (
        "all_same_error" if len(sigs) == 1 and parsed_latest and parsed_latest["has_error_marker"]
        else ("varied" if len(sigs) > 1 else "no_error")
    )

    snap = LogSnapshot(
        dag_id=ref.dag_id, task_id=ref.task_id, run_id=logs.run_id,
        operator=parsed_latest["meta"]["operator"] if parsed_latest else None,
        dag_file=parsed_latest["meta"]["dag_file"] if parsed_latest else None,
        try_count=len(entries),
        retry_pattern=pattern,
        sql_text=None,
        error_segment="",
        attempts=attempts,
    )
    if sql_latest:
        snap.sql_text = _truncate(mask_text(sql_latest), max_sql, 8000, 4000)
    if parsed_latest:
        err = _collapse_airflow_frames(parsed_latest["error"])
        snap.error_segment = _truncate(mask_text(err), max_err, 4000, 6000)
    return snap
