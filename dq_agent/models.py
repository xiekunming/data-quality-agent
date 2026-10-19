"""核心数据模型"""

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class TaskRef:
    """一次排查要定位的任务"""

    dag_id: str
    task_id: str
    run_id: Optional[str] = None  # 缺省时取该任务最近一次 run

    def key(self) -> tuple:
        return (self.dag_id, self.task_id, self.run_id or "")


@dataclass
class AttemptInfo:
    attempt: int
    signature: str  # 错误段归一化后的哈希，用于判断重试间错误是否一致


@dataclass
class LogSnapshot:
    """规则代码从原始日志提取出的结构化快照（已脱敏、已截断）——喂给 LLM 的全部内容"""

    dag_id: str
    task_id: str
    run_id: str
    operator: Optional[str] = None          # 如 SQLExecuteQueryOperator
    dag_file: Optional[str] = None          # DAG 源文件（--subdir）
    try_count: int = 0
    retry_pattern: str = "unknown"          # all_same_error / varied / no_error
    sql_text: Optional[str] = None          # 仅保留一份（取最后一次 attempt）
    error_segment: str = ""                 # 最后一次 attempt 的错误段（含 traceback）
    attempts: list = field(default_factory=list)  # list[AttemptInfo]
    log_not_found: bool = False             # 本地没找到日志时的降级标记

    def to_prompt_dict(self) -> dict:
        return asdict(self)


@dataclass
class AnalysisResult:
    """LLM 输出的结构化结论"""

    category: str = "other"
    confidence: str = "low"
    summary: str = ""
    root_cause: str = ""
    evidence: list = field(default_factory=list)
    suggestion: str = ""
    need_human: bool = True
    model: str = ""
    created_at: str = ""
    tools_used: list = field(default_factory=list)  # 本轮分析实际调用的核查工具
    similar_cases: list = field(default_factory=list)  # 分析前按错误签名召回的历史案例

    @property
    def category_label(self) -> str:
        from .taxonomy import label_for  # 延迟导入避免循环依赖；读 taxonomy.yaml 热更新
        return label_for(self.category)
