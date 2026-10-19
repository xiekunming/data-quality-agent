"""日志定位：dag/task/run -> attempt 日志文件列表

优先读本地文件（路径规则固定），REST API 作为远端部署时的兜底。
"""

import logging
import os
import re
from pathlib import Path

import requests

from .config import settings
from .models import TaskRef

log = logging.getLogger(__name__)


def _rest_auth():
    """Airflow REST API basic auth：优先 .env 的 AIRFLOW_USERNAME/PASSWORD"""
    user = os.getenv("AIRFLOW_USERNAME") or settings.get("airflow.username")
    password = os.getenv("AIRFLOW_PASSWORD") or settings.get("airflow.password")
    return (user, password) if user and password else None


class LogNotFound(Exception):
    pass


class TaskLogs:
    def __init__(self, run_id: str, files: list):
        self.run_id = run_id          # files: [(attempt_no, Path), ...] 按 attempt 升序
        self.files = files

    @property
    def last_attempt(self) -> int:
        return self.files[-1][0] if self.files else 0

    def read(self, attempt_no: int) -> str:
        for no, path in self.files:
            if no == attempt_no:
                return path.read_text(errors="replace")
        raise LogNotFound(f"attempt={attempt_no} 不存在")


def _parse_attempt_no(path: Path) -> int:
    m = re.match(r"attempt=(\d+)", path.name)
    return int(m.group(1)) if m else 0


def _collect_attempts(task_dir: Path) -> list:
    files = []
    for p in sorted(task_dir.glob("attempt=*.log")):
        files.append((_parse_attempt_no(p), p))
    if not files:  # 兼容 attempt=N/1.log 子目录结构
        for sub in sorted(task_dir.glob("attempt=*/")):
            logs = sorted(sub.glob("*.log"))
            if logs:
                files.append((_parse_attempt_no(sub), logs[-1]))
    return sorted(files, key=lambda x: x[0])


def resolve_latest_run(dag_id: str, task_id: str, logs_dir: Path = None) -> str:
    """取该任务最近一次 run（按 run 目录 mtime）"""
    base = (logs_dir or settings.logs_dir).expanduser()
    dag_dir = base / f"dag_id={dag_id}"
    if not dag_dir.is_dir():
        raise LogNotFound(f"找不到 DAG 目录：{dag_dir}")
    runs = [
        d for d in dag_dir.glob("run_id=*")
        if (d / f"task_id={task_id}").is_dir() and _collect_attempts(d / f"task_id={task_id}")
    ]
    if not runs:
        raise LogNotFound(f"DAG {dag_id} 下没有任务 {task_id} 的日志")
    return max(runs, key=lambda d: d.stat().st_mtime).name.removeprefix("run_id=")


def locate_task_logs(ref: TaskRef, logs_dir: Path = None) -> TaskLogs:
    base = (logs_dir or settings.logs_dir).expanduser()
    run_id = ref.run_id or resolve_latest_run(ref.dag_id, ref.task_id, base)
    task_dir = base / f"dag_id={ref.dag_id}" / f"run_id={run_id}" / f"task_id={ref.task_id}"
    files = _collect_attempts(task_dir) if task_dir.is_dir() else []
    if not files and settings.get("airflow.base_url") and _rest_auth():
        files = _fetch_via_rest(ref, run_id)
    if not files:
        raise LogNotFound(f"找不到日志：{task_dir}")
    return TaskLogs(run_id, files)


def _fetch_via_rest(ref: TaskRef, run_id: str) -> list:
    """Airflow REST API 兜底（basic auth）：返回 [(attempt, Path)]，内容落到临时目录"""
    base_url = settings.get("airflow.base_url").rstrip("/")
    auth = _rest_auth()
    url = f"{base_url}/api/v1/dags/{ref.dag_id}/dagRuns/{run_id}/taskInstances/{ref.task_id}"
    try:
        ti = requests.get(url, auth=auth, timeout=15).json()
        try_number = ti.get("try_number") or 1
    except Exception as e:
        log.warning("REST 获取任务实例失败: %s", e)
        return []
    out = []
    tmp_dir = Path(settings.db_path).parent / "rest_logs" / f"{ref.dag_id}.{ref.task_id}.{run_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for no in range(1, try_number + 1):
        log_url = f"{url}/logs/{no}"
        try:
            text = requests.get(log_url, auth=auth, timeout=30).text
            p = tmp_dir / f"attempt={no}.log"
            p.write_text(text)
            out.append((no, p))
        except Exception as e:
            log.warning("REST 获取 attempt=%s 日志失败: %s", no, e)
    return out


def search_task_dirs(token: str, logs_dir: Path = None) -> list:
    """按任务名搜索日志库里的 (dag_id, task_id)：精确名 > 前缀匹配 > 包含，同级按时间倒序"""
    base = (logs_dir or settings.logs_dir).expanduser()
    if not base.is_dir() or not token or len(token) < 3:
        return []
    hits, seen = [], set()
    for task_dir in base.glob(f"dag_id=*/run_id=*/task_id=*{token}*"):
        task_id = task_dir.name.removeprefix("task_id=")
        dag_id = task_dir.parent.parent.name.removeprefix("dag_id=")
        if (dag_id, task_id) not in seen:
            seen.add((dag_id, task_id))
            hits.append((dag_id, task_id, task_dir.stat().st_mtime))
    def rank(h):
        task_id = h[1]
        return (0 if task_id == token else 1 if task_id.startswith(token) else 2, -h[2])
    return sorted(hits, key=rank)
