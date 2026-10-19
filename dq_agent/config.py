"""配置加载：config.yaml + .env（凭据）"""

import os
from pathlib import Path

import yaml

try:  # python-dotenv 可选，没装则依赖真实环境变量
    from dotenv import load_dotenv

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

_CONFIG_PATH = os.environ.get(
    "DQ_AGENT_CONFIG", str(Path(__file__).resolve().parents[1] / "config.yaml")
)


class Settings:
    def __init__(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            self._d = yaml.safe_load(f) or {}

    def get(self, dotted: str, default=None):
        """get("llm.model", 默认值) 按点号路径取值"""
        node = self._d
        for key in dotted.split("."):
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node

    # 常用快捷属性 ---------------------------------------------------------
    @property
    def logs_dir(self) -> Path:
        return Path(self.get("airflow.logs_dir", "~/airflow/logs")).expanduser()

    @property
    def db_path(self) -> Path:
        p = Path(self.get("storage.db_path", "data/dq_agent.db"))
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[1] / p
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings(_CONFIG_PATH)
