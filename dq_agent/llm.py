"""LLM API 客户端（OpenAI 兼容 chat/completions；支持 GLM 开放平台与私有化网关）"""

import json
import logging
import os
import time

import requests

from .config import settings

log = logging.getLogger(__name__)


class LLMNotConfigured(Exception):
    pass


def parse_thinking(v):
    """thinking 配置解析：dict 原样用；JSON 字符串解析；空/非法返回 None（不发该字段）。

    参数格式由网关决定：GLM 开放平台 '{"type": "disabled"}'；
    支持思考档位（低/中/高）的私有网关按其文档填，如 '{"effort": "low"}' 风格。
    """
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            d = json.loads(v)
            return d if isinstance(d, dict) else None
        except json.JSONDecodeError:
            log.warning("thinking 配置不是合法 JSON，忽略: %r", v)
    return None


def _payload_defaults(**override) -> dict:
    payload = {
        "model": override.get("model") or settings.get("llm.model", "glm-5.3"),
        "temperature": override.get("temperature", settings.get("llm.temperature", 0.1)),
        "max_tokens": override.get("max_tokens", settings.get("llm.max_tokens", 4096)),
        "stream": False,
    }
    # 思考参数：调用方显式给了（键存在）优先，否则用 config 的 llm.thinking 默认值
    raw = override["thinking"] if "thinking" in override else settings.get("llm.thinking")
    t = parse_thinking(raw)
    if t:
        payload["thinking"] = t
    return payload


def chat_raw(messages: list, tools: list = None, **override) -> dict:
    """完整响应 dict（含 tool_calls）；带 429/5xx 与网络异常重试"""
    key = os.getenv("GLM_API_KEY")
    if not key:
        raise LLMNotConfigured("未配置 GLM_API_KEY（.env）")

    base = settings.get("llm.base_url", "https://open.bigmodel.cn/api/paas/v4").rstrip("/")
    payload = _payload_defaults(**override)
    payload["messages"] = messages
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    timeout = settings.get("llm.timeout", 120)
    retries = settings.get("llm.retries", 2)
    last_err = None
    for i in range(retries + 1):
        try:
            r = requests.post(
                f"{base}/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json=payload,
                timeout=timeout,
            )
            if r.status_code in (429, 500, 502, 503):
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                log.warning("GLM 可重试错误(%s/%s): %s", i + 1, retries + 1, last_err)
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(2 ** i)
    raise RuntimeError(f"GLM 调用失败（已重试 {retries} 次）：{last_err}")


def chat(messages: list, **override) -> str:
    return chat_raw(messages, **override)["choices"][0]["message"]["content"]
