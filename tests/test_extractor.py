"""提取器单元测试：合成日志（确定性）+ 本机真实日志（存在才跑）"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq_agent.log_extractor import (  # noqa: E402
    build_snapshot, error_signature, mask_text, parse_attempt,
)
from dq_agent.log_locator import TaskLogs  # noqa: E402
from dq_agent.models import TaskRef  # noqa: E402

REAL_LOG_DIR = (Path(__file__).parent / "fixtures/logs/dag_id=dq_error_demo/"
                "run_id=manual__2026-10-31T07:17:38+00:00/task_id=e01_sql_syntax_error")


def _attempt_log(attempt: int, ts: str, extra: str = "") -> str:
    return f"""[2026-10-18T{ts}+0800] {{local_task_job_runner.py:123}} INFO - ::group::Pre task execution logs
[2026-10-18T{ts}+0800] {{taskinstance.py:2614}} INFO - Dependencies all met
[2026-10-18T{ts}+0800] {{taskinstance.py:2867}} INFO - Starting attempt {attempt} of 2
[2026-10-18T{ts}+0800] {{taskinstance.py:2890}} INFO - Executing <Task(SQLExecuteQueryOperator): t_demo> on 2026-10-18
[2026-10-18T{ts}+0800] {{standard_task_runner.py:104}} INFO - Running: ['airflow', 'tasks', 'run', 'd_demo', 't_demo', 'r1', '--subdir', 'DAGS_FOLDER/demo.py', '--cfg-path', '/tmp/x']
[2026-10-18T{ts}+0800] {{sql.py:295}} INFO - Executing: INSERT INTO ads_demo
SELECT a.x, i.custstat
FROM dwd_a a JOIN dim_i i ON a.id = i.id
WHERE dt = '2026-10-18';
[2026-10-18T{ts}+0800] {{taskinstance.py:3313}} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/Users/tester/.venv/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 3185, in _execute_task
    result = self._execute_task(context, task_orig)
mysql.connector.errors.DatabaseError: 1064 (HY000): Getting analyzing error. Detail message: Column '`i`.`custstat`' cannot be resolved.
[2026-10-18T{ts}+0800] {{taskinstance.py:1226}} INFO - Marking task as {'UP_FOR_RETRY' if attempt == 1 else 'FAILED'}. start_date=20261018T085438, end_date=20261018T085439
[2026-10-18T{ts}+0800] {{standard_task_runner.py:124}} ERROR - Failed to execute job {40 + attempt} for task t_demo (1064; pid {9000 + attempt}; contact 10.0.0.8 ops@corp.com){extra}
"""


class FakeTaskLogs(TaskLogs):
    """绕过文件读取，直接给内容"""

    def __init__(self, run_id, contents: dict):
        super().__init__(run_id, [(no, None) for no in contents])
        self.contents = contents

    def read(self, attempt_no):
        return self.contents[attempt_no]


def test_parse_attempt_extracts_meta_sql_error():
    parsed = parse_attempt(_attempt_log(1, "10:00:00.001"))
    assert parsed["meta"]["operator"] == "SQLExecuteQueryOperator"
    assert parsed["meta"]["task_echo"] == "t_demo"
    assert parsed["meta"]["dag_file"] == "DAGS_FOLDER/demo.py"
    assert parsed["meta"]["attempt_no"] == 1
    assert parsed["sql"].startswith("INSERT INTO ads_demo")
    assert "SELECT a.x, i.custstat" in parsed["sql"]
    assert "cannot be resolved" in parsed["error"]
    assert parsed["has_error_marker"]


def test_signature_stable_across_timestamps():
    """重试间必然不同的噪声（时间戳/UP_FOR_RETRY vs FAILED/job id/PID）不应改变签名"""
    e1 = parse_attempt(_attempt_log(1, "10:00:00.001"))["error"]
    e2 = parse_attempt(_attempt_log(2, "11:30:45.999"))["error"]
    assert "UP_FOR_RETRY" in e1 and "FAILED" in e2   # 状态行确实不同
    assert error_signature(e1) == error_signature(e2)


def test_signature_differs_when_exception_changes():
    e1 = parse_attempt(_attempt_log(1, "10:00:00.001"))["error"]
    e2 = parse_attempt(_attempt_log(2, "11:30:45.999"))["error"].replace(
        "custstat", "balance_amt")
    assert error_signature(e1) != error_signature(e2)


def test_build_snapshot_masks_and_dedups():
    logs = FakeTaskLogs("r1", {1: _attempt_log(1, "10:00:00.001"),
                               2: _attempt_log(2, "11:30:45.999")})
    snap = build_snapshot(TaskRef("d_demo", "t_demo"), logs)
    assert snap.try_count == 2
    assert snap.retry_pattern == "all_same_error"
    assert len({a.signature for a in snap.attempts}) == 1
    combined = (snap.sql_text or "") + snap.error_segment
    assert "ops@corp.com" not in combined and "10.0.0.8" not in combined
    assert "***@***" in combined and "*.*.*.*" in combined
    assert "/Users/tester/" not in combined  # 用户名路径脱敏


def test_collapse_airflow_frames():
    err = ('Traceback (most recent call last):\n'
           '  File "/x/.venv/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 1\n'
           '  File "/x/.venv/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 2\n'
           '  File "/x/.venv/lib/python3.12/site-packages/mysql/connector/cursor_cext.py", line 3\n'
           'mysql.connector.errors.DatabaseError: boom')
    from dq_agent.log_extractor import _collapse_airflow_frames
    collapsed = _collapse_airflow_frames(err)
    assert "airflow 框架栈帧 x2" in collapsed
    assert "cursor_cext.py" in collapsed and "boom" in collapsed


def test_real_style_logs_from_fixtures():
    """对脱敏后的真实结构日志做端到端提取验证（确定性用例，不依赖本机 Airflow）"""
    from dq_agent.log_locator import _collect_attempts
    files = _collect_attempts(REAL_LOG_DIR)
    assert len(files) == 3
    snap = build_snapshot(TaskRef("dq_error_demo", "e01_sql_syntax_error"),
                          TaskLogs("manual__2026-10-31T07:17:38+00:00", files))
    assert snap.operator == "SQLExecuteQueryOperator"
    assert snap.try_count == 3
    assert snap.retry_pattern == "all_same_error"
    assert len({a.signature for a in snap.attempts}) == 1
    assert snap.sql_text and "dq_demo_customer" in snap.sql_text
    assert "SELECTT" in snap.error_segment   # 1064 错误的相似输入提示里带原词
    assert len(snap.error_segment) < 12000
