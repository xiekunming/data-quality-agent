# 运维手册：数据质量排查 Agent

> 面向"改东西不知道改哪、坏了不知道从哪查"的场景。三张表 + 一份部署清单。
> 建立于 2026-10-25，2026-11-14 全面更新。§1-§3 描述本地形态（已完整实现）；
> §4 为生产化 playbook（两条候选路线，按 §4.2 决策规则选型）。
> 搭建安装见 `docs/setup.md`（从零到跑通）；本文管"跑起来之后怎么改怎么修"。

---

## 1. 架构总览

### 1.1 一次@排查的完整链路

```
钉钉群（人工 @排查小助手）
   │  Stream 长连接（WebSocket，免公网 IP）
   ▼
dingtalk/bot.py        收消息→回复的全程编排（并发/去重/超时转后台/候选序号）
   │
   ├─► orchestrator.py   从@文字解析任务：dag=|task=|run= 标识行 / 告警行粘贴 /
   │                     task= / 裸任务名（歧义时回候选列表让用户回序号）
   ├─► log_locator.py    定位日志：本地 ~/airflow/logs 优先；REST API（basic auth）兜底
   ├─► log_extractor.py  规则解析→快照：元信息/SQL只留一份/错误段/重试签名/脱敏
   │                     （210KB 原始日志 → ~19KB 快照，纯代码不花 token）
   ├─► store.py          快照/结论/告警写 StarRocks agent_meta（断连降级 SQLite）
   ├─► 历史上下文组装     分析前自动注入（B2/B3）：
   │     ├─ task_history     该任务过往失败结论（新到旧 5 条）
   │     └─ similar_cases   同错误签名的历史案例（同签名=同一错误，精确召回 3 条）
   ├─► analyzer.py       组装提示词+快照+上下文 → 调 GLM（工具循环）→ 解析结论 JSON
   │     ├─ prompts/analyze_log.md   分析规则+输出JSON格式+上下文使用说明（热更新）
   │     ├─ prompts/taxonomy.yaml    错误分类法（热更新）
   │     ├─ llm.py                   HTTP 调模型（纯管道，OpenAI 兼容）
   │     └─ tools.py                 只读查库工具（语句守卫/50行截断/30s超时）
   ├─► 知识沉淀           分析完成后自动写 knowledge_case（同 run 幂等）
   └─► dingtalk/render.py  结论 → 群里的 markdown 文案（含"相似历史案例"段）
```

### 1.2 模块清单

| 文件 | 职责 | 生产改动频率 |
|---|---|---|
| `dq_agent/dingtalk/bot.py` | 钉钉收发、交互状态机（候选/序号/去重/超时后台续推） | 低 |
| `dq_agent/dingtalk/render.py` | 所有回复文案与格式 | **高（调样式都改这）** |
| `dq_agent/orchestrator.py` | @消息→任务解析（顶部正则区）+ 缓存判断 + 预采集 + 上下文组装（build_analysis_context）+ 知识沉淀（record_outcome） | 中 |
| `dq_agent/log_locator.py` | 日志文件定位（本地/REST 两通道） | 低 |
| `dq_agent/log_extractor.py` | 日志格式解析规则（Airflow 格式变了才动） | 低 |
| `dq_agent/store.py` | StarRocks 主 + SQLite 降级双后端；含任务历史/签名召回/知识库 API | 低 |
| `dq_agent/analyzer.py` | 工具循环 + 结论组装（分类校验） | 中 |
| `dq_agent/llm.py` | 模型 HTTP 客户端（重试/429退避/thinking参数透传） | 低 |
| `dq_agent/tools.py` | sql_query + table_schema 只读工具 | 低 |
| `dq_agent/taxonomy.py` | 分类法加载器（读 taxonomy.yaml，mtime 缓存） | 不动 |
| `dq_agent/alert_server.py` | 本地预采集 HTTP(8765)，告警即建快照不调LLM | 不动 |
| `prompts/analyze_log.md` | 模型的"工作说明书" | **高（调优主入口）** |
| `prompts/taxonomy.yaml` | 分类/线索/标签唯一事实源 | **高（积累案例）** |
| `scripts/analyze_cli.py` | 终端排查冒烟（--no-llm 看快照不花钱） | 运维工具 |
| `scripts/backtest.py` | 历史失败批量回测（含快照/结论落库、知识沉淀） | 运维工具 |
| `samples/dq_error_demo_dag.py` | **错误注入样本 DAG**：7 类已知根因的失败任务（回测 ground truth + 知识库样本源），软链到 dags 目录 | 样本维护 |
| `tests/` | 28 个单元/集成测试（extractor/mention/tools/context 四组） | 随改动更新 |

---

## 2. 改动查找表（背这张表就够了）

| 想改什么 | 改哪里 | 生效方式 |
|---|---|---|
| 群里回复的文字/格式/emoji/标题 | `dingtalk/render.py` | **重启进程** |
| 分析的判断规则、输出JSON字段、工具使用纪律 | `prompts/analyze_log.md` | **热更新**（下次分析生效） |
| 错误分类、判断线索、分类中文标签 | `prompts/taxonomy.yaml` | **热更新** |
| 某种@格式/告警行识别不了 | `orchestrator.py` 顶部 `_REF_*` 正则区 | 重启 |
| 候选列表数量/有效期、序号选择逻辑 | `orchestrator.py` / `bot.py`（_pending TTL 600s） | 重启 |
| 换模型/网关 | `config.yaml` 的 llm 段 + `.env` 的 GLM_API_KEY | 重启 |
| 思考档位（GLM 或私有化网关） | `config.yaml` 的 llm.thinking / tools.thinking（JSON 按网关文档填） | 重启 |
| 回复慢：工具轮数/思考档位/并发 | `config.yaml` 的 tools.max_rounds / thinking / limits.concurrency | 重启 |
| 缓存有效期 | `config.yaml` 的 limits.cache_ttl_hours（默认12h） | 重启 |
| 日志解析不出 SQL/错误段（Airflow 升级后） | `log_extractor.py`（先跑 `analyze_cli --no-llm` 看快照定位） | 重启 |
| 查库工具限制（行数/超时/默认库） | `config.yaml` 的 tools 段 | 重启 |
| 历史上下文/召回条数、注入行为 | `orchestrator.build_analysis_context`（条数）+ `prompts/analyze_log.md` 的 extra_context 说明（措辞热更新） | 代码部分重启 |
| 回测行为/准确率口径 | `scripts/backtest.py` | 脚本无状态，改完即生效 |
| 凭据（钉钉/GLM/SR/Airflow） | `.env`（**只能手工改，不进代码不进git**） | 重启 |

**热更新边界**：只有 `prompts/` 下两个文件是每次分析现读的，其余一律要重启进程。
生产上调优 SOP：先改 prompts（零风险）→ 不够再动代码 → 重启前跑 `pytest tests/ -q`。

---

## 3. 排障手册（按症状查）

**前置**：进程输出必须落到文件（启动命令见 §4.1）。所有答案都在日志里。

### 症状1：群里@了完全没反应
```bash
pgrep -fl "python -m dq_agent"        # ① 进程死了？→ 正常情况 launchd 会10秒内自动拉起；
launchctl print gui/$(id -u)/com.dqagent.bot | grep -E "state|last exit"
                                       #    state≠running 且 last exit=78(EX_CONFIG) → 见 §4.1
                                       #    末尾的 provenance 坑：挪走 logs/bot.log 重装服务
grep "收到@消息" logs/bot.log          # ② 没收到 → Stream 断连：日志里搜 network exception
                                       #    （手动 start_bot.sh 启动的实例日志在 logs/bot-manual.log）
                                       #    （SDK 自动重连是正常现象，持续失败查网络/凭据）
```

补充：日志里出现 `ProxyError ... 127.0.0.1:xxxx Connection refused` = 请求被系统代理截走、
但代理程序没在运行。`run_bot.sh` 已用 `NO_PROXY` 让钉钉/GLM/本机地址直连（不走代理），
经 launchd/脚本启动不应再遇到；绕过脚本手动启动且系统代理开着时，可
`NO_PROXY=localhost,127.0.0.1,dingtalk.com,bigmodel.cn python -m dq_agent` 指定。

### 症状2：收到了但群里没回
```bash
grep "钉钉回复" logs/bot.log
# "回复成功 errcode=0"   → 回成功了，是客户端显示问题
# "业务失败 HTTP=200 body={errcode...}" → 按errcode查（常见：权限被回收/改配置未发版/内容风控）
# "回复异常"            → 网络到钉钉网关不通
```

### 症状3：回了但结论不对（三层二分）
```bash
# 层1 定位对不对：日志"收到@消息"后看解析出的 dag/task/run
#   错了 → @格式问题，改 orchestrator 正则
# 层2 快照对不对（不花钱）：
.venv/bin/python scripts/analyze_cli.py <dag> <task> --no-llm
#   SQL/错误段缺内容 → 改 log_extractor.py
# 层3 快照对但分析错：
.venv/bin/python scripts/analyze_cli.py <dag> <task> --force
#   看结论+工具调用轨迹；多数是提示词问题 → 改 prompts/（热更新，改完直接重跑）
```

### 症状4：查库工具报错
```bash
grep "核查工具" logs/bot.log    # 每次调用的SQL和返回/报错都在
# 守卫拦截（ToolGuardError）→ 模型会自己改写重试，一般不用管
# 连接失败 → 查 .env 的 SR_* 和 StarRocks 存活
# 超时 → 查库太慢，tools.query_timeout 可调
```

### 症状5：回复特别慢
先看是不是命中缓存（秒回才是常态）。现场分析速度构成 = GLM调用次数 × 思考档位 + 工具执行（毫秒级）。
提速旋钮：tools.max_rounds ↓、tools.thinking 调低档、limits.analysis_timeout（超时转后台的阈值）。

### 万能兜底
```bash
.venv/bin/python -m pytest tests/ -q          # 测试全过说明核心没坏
git diff && git checkout -- <file>            # 有git后，改坏了直接回滚
```

---

## 4. 运行与部署

### 4.1 本地运行（launchd 服务化）

```bash
cd <项目目录>
# 依赖：MySQL → StarRocks(colima容器) → Airflow 全起（顺序见各自环境手册）

# 机器人 = launchd 服务（开机自启 + 崩溃自动重启 + 单实例守卫），日常零操作：
#   状态：   launchctl print gui/$(id -u)/com.dqagent.bot | head -20
#   跟日志： tail -f logs/bot.log
#   重启：   launchctl kickstart -k gui/$(id -u)/com.dqagent.bot
#   停止：   launchctl bootout gui/$(id -u)/com.dqagent.bot
# 手动兜底（服务不可用时）：scripts/start_bot.sh

# 安装/重装服务（模板 scripts/com.dq-agent.plist.template）：
sed "s|___PROJECT_DIR___|$PWD|g" scripts/com.dq-agent.plist.template \
  > ~/Library/LaunchAgents/com.dqagent.bot.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.dqagent.bot.plist
# ⚠️ 坑（macOS 新版实测，2026-11-14 复发过一次）：若 logs/bot.log 由终端等其他进程创建
#    （带 com.apple.provenance / com.apple.macl 扩展属性），launchd 拉起会报
#    last exit code = 78 EX_CONFIG —— 把旧 bot.log 挪走归档再 kickstart 即可。
#    已根治：run_bot.sh 不再重定向任何日志（launchd 用 StandardOutPath 独占 bot.log，
#    手动启动走 start_bot.sh → logs/bot-manual.log），任何人手动启动都不会再污染 bot.log。
```

### 4.2 生产部署切分（安全域 架构，两条候选路线）

**共同点**：分析永远在域外（生产域通常禁出网，LLM/钉钉都够不着）；生产侧永远没有 LLM 调用。
两条路线的差别只有一件事——**失败信息怎么过域**。选哪条不看技术优劣，看哪条审批好过。

#### 路线一：零侵入拉取（REST）—— 生产侧几乎零改动

```
任务失败 ──► 生产现有告警链路 ──► 钉钉群（人看到告警，拿着任务名@agent）
                                        │
有人@排查 ──► agent 按需走 Airflow REST（按@触发、不轮询；一天几十次、单次几十KB）
              ① 列最近 failed 的任务实例，定位失败 run
              ② GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}
                （basic auth，Viewer 只读账号；官方 API，log_locator.py REST 分支已实现）
                                        │
              域外 agent 服务器解析（log_extractor 现成）→ LLM 分析 → 回群
```

- **生产侧改动**：只在生产 Airflow UI 建一个 **Viewer 只读账号**（不改代码、不发版、不装服务）
- **真正的关卡：网络审批**——给域外开放生产控制面 API 是安全上最敏感的请求类型。
  申请话术三件套：Viewer 只读 + 来源 IP 白名单 + HTTPS；按@按需拉取，非全量轮询
- **优点**：生产零代码侵入；解析与 agent 升级全在开发侧，迭代不碰生产
- **风险/局限**：控制面口子本身敏感（同一套 API 也能触发/清除任务，Viewer 可缓解但口子存在）；
  裸@时的"最近失败候选"需给 `recent_failure_candidates` 加 REST 分支（小改动）

#### 路线二：callback 域内采集落表 —— 数据面最干净

```
┌─ 生产（安全域 内）──────────────┐        ┌─ 开发（安全域 外）────────────────┐
│ Airflow+数仓+生产库            │        │ agent 主服务（常驻进程）           │
│                               │  DB    │  放：dq_agent/ 全部代码           │
│ 放（随DAG发版的采集件，非常驻）：│  通道  │      prompts/ scripts/ tests/    │
│  callback.py（需自建）          │ ────►  │      config.yaml .env            │
│  + log_locator.py             │ 只读   │  用：账号B（只读：agent_meta快照表  │
│  + log_extractor.py           │        │      + 业务库，供核查工具）         │
│  + store 的写快照部分          │        │  职责：轮询快照→钉钉播报；被@→     │
│  用：账号A（写快照表，凭据只在   │        │        LLM分析→回群                │
│  生产 Airflow Connection）     │        │  分析记录/缓存：开发侧自己的库      │
└───────────────────────────────┘        └─────────────────────────────────┘
```

- **生产上没有"agent 服务"**：只有随 DAG 走的 callback（失败瞬间被 Airflow 调一下，零 LLM、不出网）。分析永远在开发侧。
- **单向流铁律**：域内写、域外只读。账号B永远无写权限。
- 告警播报：保留生产现有链路（内网代理→钉钉群，格式不动），agent 只管@应答。

#### 两路线对比与决策

| | 路线一：REST 拉取 | 路线二：callback |
|---|---|---|
| 生产侧改动 | ≈0（建一个只读账号）| DAG 发版 + 部署 dq_agent 包 |
| 过域数据 | 原始日志（按需，几十次/天）| 脱敏快照表（单向流）|
| 审批焦点 | 网络路径 → 生产控制面 | 生产环境装包发版 |
| 代码就绪度 | **≈90%**（log_locator 双通道现成）| callback.py 需自建（§4.3 步骤 2）|
| 时效 | @时才拉（秒级，够用）| 失败即落表（@时秒出）|
| 数据暴露面 | 原始日志出域 | 仅脱敏截断后的快照 |

**决策规则**：哪条审批好过走哪条；两条都不批 → 人工兜底（把告警行/日志尾部粘贴给 agent，
今天就能用，orchestrator 已支持解析告警行）。

> **被否形态**（评估过、未采纳）：整个 agent 部署在生产域内 —— 生产域通常禁止出网调
> LLM/钉钉，且需在生产侧加常驻服务，审批与运维成本高。

### 4.3 上线步骤清单（顺序执行）

> 性质：**生产化 playbook**——在企业生产环境落地时按序执行。步骤 2 仅在采用路线二时需要
> （见 §4.2 路线对比与决策；采用路线一则跳过该步）。

1. **仓库就绪**：直接 clone 本仓库即可（提交历史与版本 tag 齐全）；若在企业内网重建仓库，
   先 git init + 首次提交 + tag——一切回滚能力的前提
2. **双端彩排**——本机模拟生产架构（生产采集端 + 域外分析端；对应路线二；若采用路线一 REST 拉取，
   本步跳过，改为完成网络开通与 Viewer 只读账号）：
   - 写 `dq_agent/callback.py`（薄壳：从 Airflow failure context 取 dag/task/run → 复用 locator+extractor → 账号A写快照表）
   - bot 加"仅DB模式"开关（禁读 ~/airflow/logs，快照只从表读）
   - **验收标准：关掉本地日志目录，全链路（@→查表→LLM→回群）仍通**
3. **回测验准**：`backtest.py` 同批历史失败在 glm-5.3 与私有化部署模型各跑一遍，人工对比准确率；不达标先调 prompts 再上线。评测集固化为**回归评测**：此后每次改 prompts/换模型自动重跑，准确率劣化即拦截（防止调好一个 case 调坏另一个）
4. **企业网关接入（纯配置）**：config.yaml 换 llm.base_url/model/thinking、tools.thinking（思考档位字段名以网关文档为准）；.env 换网关key+账号B；先跑 `analyze_cli --force` 验证再启机器人
5. **守护化**：systemd unit 或 Docker（Restart=always）+ 日志轮转 + **进程存活告警**（agent自己挂了要有人知道）
6. **安全收紧**：Airflow 账号降 Viewer；8765 预采集口加鉴权或关闭（两条生产路线都不需要它，那是本地形态的预采集口）；.env 进企业 secret 管理；**审计日志**（谁@的、LLM 执行了哪条 SQL、返回了什么，可回溯——LLM 持只读账号查库是安全审计重点）；requirements.txt 锁版本防环境漂移
7. **影子模式 1-2 周**：只分析落库不回群（或回到一个内部测试群），人工抽查结论 → 达标后放正式群
8. 单实例红线：**同一钉钉凭据只能跑一个进程**（Stream 互踢）；扩容=开放平台再建应用

### 4.4 日常运维动作速查

```bash
pgrep -fl "python -m dq_agent"                     # 存活
tail -f logs/bot.log                                # 跟日志
.venv/bin/python -m pytest tests/ -q                # 改代码后回归
.venv/bin/python scripts/analyze_cli.py <dag> <task> --force   # 复现单次分析
.venv/bin/python scripts/backtest.py --dry-run      # 看有多少历史失败可回测

# 制造新的失败样本（错误注入 DAG：7 类已知根因；用你部署 Airflow 的 venv 路径）
<airflow-venv>/bin/airflow dags trigger dq_error_demo
# 回测准确率（注入样本 ground truth 已知，实测 7/7；报告由 backtest.py 生成在 reports/，不入仓库）
.venv/bin/python scripts/backtest.py --limit 12
```

---

## 5. 配置速查

### config.yaml 关键项
| 项 | 默认 | 说明 |
|---|---|---|
| llm.model / base_url | glm-5.3 / 智谱paas v4 | 换私有化网关只改这两项+key |
| llm.max_tokens | 4096 | 推理模型须含思考预算，勿低于4096 |
| llm.thinking | `''`（不发） | 思考参数模板（JSON字符串），留空走模型默认 |
| tools.enabled | true | 关掉则纯静态分析（不查库） |
| tools.thinking | `{"type": "disabled"}` | 工具轮次思考档位（提速用），换网关按文档填 |
| tools.max_rounds | 3 | 工具循环上限 |
| tools.max_rows / query_timeout | 50 / 30 | 查库行数截断/超时 |
| tools.business_db | demo_dw | 核查工具默认库（.env 的 SR_BUSINESS_DB 可覆盖）|
| limits.concurrency | 2 | 同时分析数（全局，多群共享） |
| limits.analysis_timeout | 180s | 超时转后台阈值（不丢任务，完成后补推） |
| limits.cache_ttl_hours | 12 | 同 run 结论缓存时长 |
| storage.backend | starrocks | 降级 SQLite 自动、进程内sticky |

### .env（只手工改，永不进git/代码）
`GLM_API_KEY` / `DINGTALK_CLIENT_ID`+`SECRET` / `SR_HOST/PORT/USER/PASSWORD/DB`（agent_meta 读写）/ `SR_BUSINESS_DB`（核查工具默认业务库，须只读）/ `AIRFLOW_BASE_URL/USERNAME/PASSWORD`（REST用）

### StarRocks agent_meta 四表
`log_snapshot`（快照，签名召回的数据源）/ `analysis_record`（结论，含tools_used，缓存与任务历史的数据源）/ `alert_log`（告警，裸任务名匹配靠它）/ `knowledge_case`（知识沉淀：每次分析自动写入，source_run 幂等）
