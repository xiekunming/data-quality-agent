"""分析器：快照 + 提示词（+ 只读核查工具循环）-> GLM -> 结构化结论"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

from . import llm
from . import tools as dq_tools
from .config import settings
from .models import AnalysisResult, LogSnapshot
from .taxonomy import keys as taxonomy_keys, prompt_block

log = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "analyze_log.md"


def _max_tool_rounds() -> int:
    return int(settings.get("tools.max_rounds", 3))


def _extract_json(text: str) -> dict | None:
    """模型输出里抠出 JSON 对象（容忍 ```json 包裹、前后杂文字）"""
    text = re.sub(r"```(?:json)?", "", text or "")
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None


def build_messages(snapshot: LogSnapshot, context: dict = None) -> list:
    payload = snapshot.to_prompt_dict()
    if context:
        payload["extra_context"] = context
    system = PROMPT_PATH.read_text(encoding="utf-8").replace("{{TAXONOMY}}", prompt_block())
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]


def _parse_tool_args(fn: dict) -> dict:
    raw = fn.get("arguments", fn.get("args"))
    try:
        return json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        return {}


def _tool_loop(messages: list) -> tuple:
    """function calling 循环：返回 (最终文本, tools_used)；失败不影响静态结论。

    中间轮次（决定调什么工具）关闭深度思考以提速（实测快约 40%，决策质量无损）；
    最终生成结论的那次回答若解析失败，由 analyze() 的修复调用兜底（默认开思考）。
    """
    tools_used = []
    # 工具轮次思考档位（低）：config 里 JSON 字符串，按网关格式填；留空则用 llm.thinking 默认
    _tt = llm.parse_thinking(settings.get("tools.thinking"))
    for _ in range(_max_tool_rounds()):
        resp = llm.chat_raw(messages, tools=dq_tools.TOOLS_SPEC,
                            **({"thinking": _tt} if _tt else {}))
        msg = resp["choices"][0]["message"]
        calls = msg.get("tool_calls") or []
        if not calls:
            return msg.get("content") or "", tools_used
        messages.append({"role": "assistant",
                         "content": msg.get("content") or "",
                         "tool_calls": calls})
        for tc in calls:
            fn = tc.get("function", {}) or {}
            args = _parse_tool_args(fn)
            try:
                out = dq_tools.dispatch(fn.get("name", ""), args)
            except Exception as e:
                out = f"工具执行失败: {e}"
            tools_used.append(f"{fn.get('name')}({json.dumps(args, ensure_ascii=False)[:120]})")
            log.info("核查工具 %s -> %s", tools_used[-1][:100],
                     out[:150].replace("\n", " "))
            messages.append({"role": "tool",
                             "tool_call_id": tc.get("id", ""),
                             "content": out[:4000]})
    log.warning("工具循环达到上限 %s 轮，转静态结论", _max_tool_rounds())
    return "", tools_used


def analyze(snapshot: LogSnapshot, context: dict = None) -> AnalysisResult:
    messages = build_messages(snapshot, context)

    tools_used = []
    if settings.get("tools.enabled", False):
        content, tools_used = _tool_loop(messages)
    else:
        content = llm.chat(messages)

    data = _extract_json(content)
    if data is None:  # 解析失败追一次，明确要求只输出 JSON（普通调用，不带工具）
        data = _extract_json(llm.chat(messages + [
            {"role": "assistant", "content": content},
            {"role": "user", "content": "你上一次输出无法解析为 JSON。重新输出，只输出一个 JSON 对象。"},
        ]))

    result = AnalysisResult(
        category=data.get("category", "other") if data else "other",
        confidence=data.get("confidence", "low") if data else "low",
        summary=(data or {}).get("summary", ""),
        root_cause=(data or {}).get("root_cause", "模型输出解析失败，请人工查看日志"),
        evidence=[str(e) for e in (data or {}).get("evidence", [])][:5],
        suggestion=(data or {}).get("suggestion", ""),
        need_human=bool((data or {}).get("need_human", True)),
        model=settings.get("llm.model", "glm-5.3"),
        created_at=datetime.now().isoformat(timespec="seconds"),
        tools_used=tools_used,
    )
    if result.category not in taxonomy_keys():
        result.category = "other"
    return result
