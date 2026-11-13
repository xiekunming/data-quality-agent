# 安装搭建指南（从零到跑通）

> 按"账号门槛从零到高"分四档，**跑到哪一档算哪一档**，每档末尾有验收标准。
> 只想 30 秒看看效果 → 只做第 0 档；想在自己群里用起来 → 走到第 2 档。
> 排障/运维视角的手册见 [operations.md](operations.md)，本文只管"装起来"。

---

## 前置要求

| 依赖 | 说明 |
|---|---|
| Python 3.10+ | 本地开发用 3.12 验证 |
| git | 克隆本仓库 |
| （可选）Airflow 2.x | 第 1 档起需要，日志格式在 2.10 验证 |
| （可选）StarRocks / MySQL 协议兼容库 | 第 3 档需要；**没有也能跑**（自动降级 SQLite） |

## 第 0 档：零账号，30 秒离线体验

不需要任何 Airflow / 数据库 / 钉钉，只需要一个 LLM API Key（不调 LLM 连 Key 都不用）。

```bash
git clone https://github.com/xiekunming/data-quality-agent.git && cd data-quality-agent
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

cp .env.example .env        # 第 0 档可留空，只有下面调 LLM 时才需要填 GLM_API_KEY

# 看规则提取的快照长什么样（不调 LLM、不花 token）
.venv/bin/python scripts/analyze_cli.py --sample --no-llm

# 调 LLM 出完整结论（需在 .env 填 GLM_API_KEY，https://open.bigmodel.cn）
.venv/bin/python scripts/analyze_cli.py --sample
```

说明：`--sample` 用的是 `samples/logs/` 下的**脱敏样例日志**（错误注入 DAG
`samples/dq_error_demo_dag.py` 产生的真实失败日志，7 类已知根因），可加任务名指定样本：
`--sample e03_not_null_violation`。

> **✅ 验收**：`--no-llm` 能打印出快照（含 SQL 段/错误段/重试签名）；填 Key 后 `--sample`
> 输出结构化结论（分类/根因/证据/建议/置信度）。

## 第 1 档：接你自己的 Airflow（终端排查，还不用钉钉）

前提：你有一套跑着的 Airflow（任意 scheduler 触发过任务、本地留有日志）。

1. **指对日志目录**：默认读 `~/airflow/logs`（`config.yaml` 的 `airflow.logs_dir`），
   装在别处就改成你的路径。REST API 兜底可不配（留空即只读本地文件）。
2. **终端排查任意失败任务**（`--force` 跳过缓存强制分析）：

```bash
.venv/bin/python scripts/analyze_cli.py <dag_id> <task_id> --force
```

3. （可选）制造失败样本：把 `samples/dq_error_demo_dag.py` 软链/复制到你的
   `dags/` 目录，建好 DAG 里引用的两个 Connection（见文件头注释），UI 触发一次，
   就有了 7 类错误的现成失败日志。

> **✅ 验收**：对你库里真实挂过的任务执行上面命令，能输出快照 + 结论。

## 第 2 档：接入钉钉群（完整体验）

前提：你有权限在 [钉钉开放平台](https://open-dev.dingtalk.com) 创建企业内部应用。

1. **创建应用**：开放平台 → 应用开发 → 企业内部开发 → 创建应用（名称随意，如"排查小助手"）。
2. **添加机器人**：应用详情 → 添加应用能力 → 机器人。
3. **开启 Stream 模式**：机器人配置页 → 消息接收模式选 **Stream 模式**（免公网 IP，
   本地进程即可收消息）。
4. **拿凭据**：应用 → 凭证与基础信息 → 复制 `Client ID` / `Client Secret`，
   填入 `.env` 的 `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET`。
5. **发布应用**：版本管理与发布 → 发布（**不发布收不到消息**，这是最常见的"装完没反应"原因）。
6. **拉进群**：建一个测试群 → 群设置 → 机器人 → 添加刚发布的机器人。
7. **启动**：

```bash
.venv/bin/python -m dq_agent          # 前台启动，看日志
# 长期常驻（开机自启+崩溃自愈）见 operations.md §4.1
```

8. 群里 `@机器人 帮助` 看用法；`@机器人 <任务名>` 或直接粘贴告警机器人播报的那行失败消息。

> **✅ 验收**：群里 @机器人 一个失败任务名，1-3 分钟内收到结构化结论。

## 第 3 档：StarRocks 存储 + 业务库核查（可选，不配也能用）

不配任何 `SR_*` 时自动降级 SQLite（`data/dq_agent.db`），个人使用完全够；
配了才有：跨重启的历史上下文、错误签名召回、多端共享的分析记录。

1. **建库**：在 StarRocks 建库 `agent_meta`（表由程序自动建，`log_snapshot`
   等四表见 operations.md §5）。
2. **凭据**：`.env` 填 `SR_HOST/PORT/USER/PASSWORD`，`SR_DB=agent_meta`。
   建议为 agent 建专用账号，权限 = agent_meta 读写 + 业务库**只读**。
3. **业务库核查**：`.env` 填 `SR_BUSINESS_DB=<你的业务库名>`（LLM 只读工具会查它，
   三层防线：语句守卫仅 SELECT 类单条 → 50 行截断 → 30 秒超时）。
4. `config.yaml` 的 `storage.backend` 保持 `starrocks` 即可（连不上自动降级 SQLite，
   恢复后不自动切回，重启进程生效）。

> **✅ 验收**：`.venv/bin/python -m pytest tests/ -q` 全绿（本机起了 StarRocks 时
> 含真实查库用例）；第二次分析同类失败时结论里出现"📚 相似历史案例"。

---

## 常见问题

- **群里 @ 了没反应**：按顺序查 ①应用发布了吗 ②Stream 模式开了吗 ③进程活着吗
  （`pgrep -fl "python -m dq_agent"`）④是不是起了两个进程（同凭据 Stream 互踢，
  只能跑一个）。详见 operations.md §3 症状1。
- **提示 API Key 错误**：检查 `.env` 的 `GLM_API_KEY`；换其他 OpenAI 兼容网关改
  `config.yaml` 的 `llm.base_url` / `model`。
- **不用 GLM 用别的模型行吗**：行，任何 OpenAI 兼容 chat/completions 接口都可以，
  改 `config.yaml` 的 llm 段即可，代码零改动；思考档位参数见同文件注释。
