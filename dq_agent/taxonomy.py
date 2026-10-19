"""分类法加载：prompts/taxonomy.yaml 是唯一事实源。

带 mtime 缓存——文件被修改后下一次调用即读到新内容（生产热更新，无需重启）。
"""

import threading
from pathlib import Path

import yaml

_PATH = Path(__file__).resolve().parents[1] / "prompts" / "taxonomy.yaml"
_lock = threading.Lock()
_cache = None  # (mtime, data)


def _load() -> dict:
    global _cache
    with _lock:
        mtime = _PATH.stat().st_mtime
        if _cache and _cache[0] == mtime:
            return _cache[1]
        with open(_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        _cache = (mtime, data)
        return data


def categories() -> list:
    return _load().get("categories", [])


def keys() -> list:
    return [c["key"] for c in categories()]


def label_for(key: str) -> str:
    for c in categories():
        if c["key"] == key:
            return c.get("label", key)
    return key


def prompt_block() -> str:
    """注入系统提示词的分类枚举块"""
    return "\n".join(
        f"- `{c['key']}`：{c.get('label', '')}。典型线索：{c.get('clues', '')}"
        for c in categories()
    )
