"""错误注入样本 DAG：给排查 Agent 提供多种类型的失败样本（dq_error_demo）

每个任务故意以一种方式失败，根因已知（写在 docstring 里）——用于：
  1. 回测准确率计算（ground truth 明确）
  2. 知识库相似案例的第一批多样本
  3. 新错误分类/提示词调优的试验田

预期分类对照表（task_id → 预期 category）：
  e01_sql_syntax_error    code_schema        （SQL 关键字拼错，retries=2 演示 all_same_error）
  e02_unknown_column      code_schema        （引用不存在的列）
  e03_not_null_violation  data_quality       （向 NOT NULL 列插 NULL）
  e04_bad_type_cast       data_quality       （字符串插 INT 列，类型转换失败）
  e05_connection_refused  permission_connection（连接不存在的端口）
  e06_sensor_timeout      upstream_data_missing（传感器等依赖超时）
  e07_python_code_bug     code_schema        （Python 业务代码 bug）

依赖：Airflow 连接 dq_demo_sr（dq_agent@agent_meta，演示表 dq_demo_* 所在）、
     dq_bad_conn（指向 127.0.0.1:9999，制造连接拒绝）。
触发：手动触发（schedule=None），全链路几十秒内失败完成。
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.python import PythonSensor

DEMO_CONN = "dq_demo_sr"        # dq_agent@127.0.0.1:9030/agent_meta
BAD_CONN = "dq_bad_conn"        # 127.0.0.1:9999 永远拒绝连接
BUSINESS_CONN = "demo_dw_sr" # 真实业务连接（只读失败类样本用）

with DAG(
    dag_id="dq_error_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "dq_agent", "retries": 0},
    tags=["dq_agent", "error_demo"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    # ---- code_schema：SQL 语法错误（retries=2，演示确定性错误签名一致）----
    e01 = SQLExecuteQueryOperator(
        task_id="e01_sql_syntax_error",
        conn_id=DEMO_CONN,
        sql="SELECTT id FROM agent_meta.dq_demo_customer LIMIT 1",  # SELECTT 拼错
        retries=2,
        retry_delay=timedelta(seconds=5),
    )

    # ---- code_schema：引用不存在的列 ----
    e02 = SQLExecuteQueryOperator(
        task_id="e02_unknown_column",
        conn_id=DEMO_CONN,
        sql="SELECT cust_nme FROM agent_meta.dq_demo_customer LIMIT 1",  # 列名拼错
    )

    # ---- data_quality：向 NOT NULL 列插 NULL ----
    e03 = SQLExecuteQueryOperator(
        task_id="e03_not_null_violation",
        conn_id=DEMO_CONN,
        sql=(
            "INSERT INTO agent_meta.dq_demo_customer(id, name, amount) "
            "VALUES (901, NULL, 100.00)"  # name 非空，插 NULL
        ),
    )

    # ---- data_quality：类型转换失败（脏数据写入 INT 列）----
    e04 = SQLExecuteQueryOperator(
        task_id="e04_bad_type_cast",
        conn_id=DEMO_CONN,
        sql=(
            "INSERT INTO agent_meta.dq_demo_customer(id, name, amount) "
            "VALUES (902, '脏数据样本', '十二块三毛')"  # 字符串金额 → DECIMAL 转换失败
        ),
    )

    # ---- permission_connection：连接拒绝（端口无人监听）----
    e05 = SQLExecuteQueryOperator(
        task_id="e05_connection_refused",
        conn_id=BAD_CONN,
        sql="SELECT 1",
    )

    # ---- upstream_data_missing：传感器等待依赖超时 ----
    def _upstream_partition_ready(**_):
        return False  # 模拟上游分区永远不就绪

    e06 = PythonSensor(
        task_id="e06_sensor_timeout",
        python_callable=_upstream_partition_ready,
        timeout=20,          # 等 20 秒必超时
        poke_interval=5,
        mode="reschedule",
    )

    # ---- code_schema：Python 业务代码 bug ----
    def _business_job(**_):
        record = {"amt": "1,362,000"}
        return float(record["amout"])  # 键名拼错 → KeyError

    e07 = PythonOperator(
        task_id="e07_python_code_bug",
        python_callable=_business_job,
    )

    end = EmptyOperator(task_id="end")

    start >> [e01, e02, e03, e04, e05, e06, e07] >> end
