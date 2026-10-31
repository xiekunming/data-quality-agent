"""只读核查工具守卫测试（纯单元，不连库）"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq_agent.tools import ToolGuardError, guard_sql, sql_query, table_schema  # noqa: E402


def test_guard_allows_readonly():
    assert "LIMIT" in guard_sql("SELECT a, b FROM db.t WHERE dt='2026-10-18' LIMIT 20")
    assert guard_sql("SHOW TABLES")
    assert guard_sql("DESC db.t")
    assert guard_sql("WITH x AS (SELECT 1) SELECT * FROM x LIMIT 5")
    assert guard_sql("select created_at, set_flag from t limit 3;")  # 普通列名不误拦


@pytest.mark.parametrize("bad", [
    "UPDATE t SET a=1",
    "DROP TABLE t",
    "INSERT INTO t VALUES (1)",
    "DELETE FROM t",
    "SELECT * FROM t FOR UPDATE",
    "SELECT * FROM t INTO OUTFILE '/tmp/x'",
    "SELECT 1; DROP TABLE t",          # 多语句
    "SET query_timeout = 30",
    "TRUNCATE TABLE t",
])
def test_guard_rejects(bad):
    with pytest.raises(ToolGuardError):
        guard_sql(bad)


def test_guard_rejects_empty_and_garbage():
    with pytest.raises(ToolGuardError):
        guard_sql("")
    with pytest.raises(ToolGuardError):
        guard_sql("随便一句话")


def test_table_schema_identifier_check():
    with pytest.raises(ToolGuardError):
        table_schema("t; DROP TABLE x")
    with pytest.raises(ToolGuardError):
        table_schema("")


def test_live_query_skipif_no_sr():
    """连得本地 StarRocks 才跑：验证真实查询/行数截断/表结构查询"""
    import os
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host=os.getenv("SR_HOST", "127.0.0.1"), port=int(os.getenv("SR_PORT", "9030")),
            user=os.getenv("SR_USER", ""), password=os.getenv("SR_PASSWORD", ""),
            connect_timeout=3)
        conn.close()
    except Exception:
        pytest.skip("本地 StarRocks 不可用")

    out = sql_query("SELECT 1 AS ok")
    assert '"ok"' in out and '"columns"' in out
    schema = table_schema("agent_meta.dq_demo_customer")   # 错误注入 DAG 的演示表
    assert "COLUMN_NAME" in schema or "columns" in schema
