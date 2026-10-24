"""只读核查工具：对业务库执行 SELECT 类查询。

三层防线：语句守卫（正则白名单+写操作关键词拒绝）→ 行数截断 → 查询超时；
底层再叠加只读账号（dq_agent 对业务库仅 SELECT）兜底。连接凭据走 .env 的 SR_*。
"""

import json
import logging
import os
import re

import pymysql

from .config import settings

log = logging.getLogger(__name__)

_ALLOWED_HEAD = re.compile(r"(?i)^\s*(select|with|show|desc|describe|explain)\b")
_FORBIDDEN = re.compile(
    r"(?i)\b(insert|update|delete|drop|truncate|alter|create|rename|grant|revoke|"
    r"set|call|load|export|submit|cancel|kill|"
    r"into\s+(outfile|dumpfile)|for\s+update|lock\s+in)\b")
_MULTI = re.compile(r";.+\S")          # 分号后仍有内容 = 多语句
_TRAILING_SEMI = re.compile(r";\s*$")
_IDENT = re.compile(r"^[\w.\-]+$")


class ToolGuardError(Exception):
    """语句被守卫拦截（模型可据此改写后重试）"""


def guard_sql(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        raise ToolGuardError("空语句")
    if _MULTI.search(s):
        raise ToolGuardError("一次只能执行一条语句")
    s = _TRAILING_SEMI.sub("", s).strip()
    if not _ALLOWED_HEAD.match(s):
        raise ToolGuardError("仅允许 SELECT/SHOW/DESC/EXPLAIN/WITH 开头的只读语句")
    m = _FORBIDDEN.search(s)
    if m:
        raise ToolGuardError(f"语句含写操作/受限关键词: {m.group(0)}")
    return s


def _business_db() -> str:
    """核查工具默认库：.env 的 SR_BUSINESS_DB 优先（真实值不进代码/配置），否则用 config 默认"""
    return os.getenv("SR_BUSINESS_DB") or settings.get("tools.business_db", "demo_dw")


def _connect(db: str | None):
    return pymysql.connect(
        host=os.getenv("SR_HOST", "127.0.0.1"),
        port=int(os.getenv("SR_PORT", "9030")),
        user=os.getenv("SR_USER", ""),
        password=os.getenv("SR_PASSWORD", ""),
        database=db or _business_db(),
        charset="utf8mb4", autocommit=True,
        connect_timeout=5,
        read_timeout=int(settings.get("tools.query_timeout", 30)),
    )


def _cell(v) -> str:
    t = str(v)
    return t if len(t) <= 500 else t[:500] + "…"


def sql_query(sql: str, db: str = None) -> str:
    """执行只读 SQL，返回紧凑 JSON 文本（列名 + 最多 max_rows 行 + 截断标记）"""
    s = guard_sql(sql)
    max_rows = int(settings.get("tools.max_rows", 50))
    conn = _connect(db)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET query_timeout = {int(settings.get('tools.query_timeout', 30))}")
            cur.execute(s)
            rows = cur.fetchmany(max_rows + 1)
            cols = [d[0] for d in cur.description] if cur.description else []
    finally:
        conn.close()
    truncated = len(rows) > max_rows
    out = {
        "columns": cols,
        "row_count": len(rows[:max_rows]),
        "truncated": truncated,
        "rows": [[_cell(v) for v in r] for r in rows[:max_rows]],
    }
    return json.dumps(out, ensure_ascii=False, default=str)


def table_schema(table: str, db: str = None) -> str:
    """查表的实际列定义（列名/类型/可空），用于核对 SQL 引用的列是否存在"""
    if not _IDENT.match(table or ""):
        raise ToolGuardError("表名不合法")
    parts = table.split(".")
    tbl = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else (db or _business_db())
    for p in (schema, tbl):
        if not _IDENT.match(p):
            raise ToolGuardError("库名/表名不合法")
    sql = (
        "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE "
        f"FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{tbl}' "
        "ORDER BY ORDINAL_POSITION"
    )
    return sql_query(sql, db="information_schema")


TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "sql_query",
            "description": (
                "在业务数仓执行只读 SQL 核查数据。仅允许 SELECT/SHOW/DESC/EXPLAIN/WITH "
                "单条语句；务必带 LIMIT（最多返回 50 行）；超时 30 秒。典型用途：统计脏数据"
                "行数、抽样问题行、核对分区/行数。若被守卫拦截请改写语句（如给列名加反引号）。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string",
                            "description": "只读SQL，如 SELECT col, COUNT(*) FROM db.t "
                                           "WHERE dt='2026-10-18' AND col IS NULL LIMIT 20"},
                    "db": {"type": "string", "description": "可选，默认业务库"},
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "table_schema",
            "description": "查一张表的实际列定义（列名/类型/可空），用于核对 SQL 中引用的列名是否真实存在。",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "表名，可带库名前缀 db.table"},
                    "db": {"type": "string", "description": "可选，默认库"},
                },
                "required": ["table"],
            },
        },
    },
]


def dispatch(name: str, args: dict) -> str:
    if name == "sql_query":
        return sql_query(args["sql"], db=args.get("db"))
    if name == "table_schema":
        return table_schema(args["table"], db=args.get("db"))
    return f"未知工具: {name}"
