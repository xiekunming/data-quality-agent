"""钉钉机器人（Stream 模式，免公网 IP）

收到群内 @消息 -> 解析任务（含歧义候选/序号选择/最近失败任务兜底）->
缓存命中直接回；否则先回"正在排查"，异步分析完成后回推结论；
超时不丢弃（转后台继续，完成后自动补推）；同一任务重复@去重。
"""

import asyncio
import logging
import os
import re
import time

import requests

try:
    import dingtalk_stream
    from dingtalk_stream import AckMessage
    import dingtalk_stream.chatbot as chatbot
except ImportError as e:  # pragma: no cover
    raise ImportError("缺少依赖 dingtalk-stream：pip install dingtalk-stream") from e

from .. import orchestrator
from ..config import settings
from ..log_locator import LogNotFound
from ..llm import LLMNotConfigured
from . import render

log = logging.getLogger(__name__)

TOPIC = getattr(chatbot.ChatbotMessage, "TOPIC", "/v1.0/im/bot/messages/get")

_pending: dict = {}        # (conversationId, senderStaffId) -> (候选 TaskRef 列表, 过期时间)
_PENDING_TTL = 600


def _put_choices(key, refs):
    _pending[key] = (refs, time.time() + _PENDING_TTL)


def _take_choice(key, text):
    """上次给出候选列表后，用户回复纯数字 -> 取对应候选（优先用发出列表时的快照，防列表漂移）"""
    entry = _pending.get(key)
    if re.fullmatch(r"[0-9]{1,2}", text.strip()):
        idx = int(text.strip())
        if entry and time.time() <= entry[1]:
            refs = entry[0]
            if 1 <= idx <= len(refs):
                _pending.pop(key, None)
                return refs[idx - 1]
        try:  # 无暂存列表时兜底：按最近失败任务同序取
            return orchestrator.candidate_by_index(idx)
        except Exception:
            return None
    if entry and time.time() > entry[1]:
        _pending.pop(key, None)
    return None


def _reply_markdown(webhook: str, title: str, text: str, at_user: str | None = None) -> bool:
    payload = {
        "msgtype": "markdown",
        "markdown": {"title": title, "text": text},
    }
    if at_user:
        payload["at"] = {"atUserIds": [at_user]}
    try:
        r = requests.post(webhook, json=payload, timeout=15)
        try:
            body = r.json()
        except ValueError:
            body = r.text[:300]
        ok = r.status_code == 200 and not (isinstance(body, dict) and body.get("errcode"))
        if ok:
            log.info("钉钉回复成功: %r errcode=%s", title,
                     body.get("errcode") if isinstance(body, dict) else body)
        else:
            # HTTP 200 但业务失败（errcode!=0）是钉钉常态，必须打出来才能定位
            log.error("钉钉回复业务失败: HTTP=%s body=%s webhook=%s...",
                      r.status_code, body, webhook[:90])
        return ok
    except Exception:
        log.exception("钉钉回复异常 webhook=%s...", webhook[:90])
        return False


class AgentHandler(chatbot.ChatbotHandler):
    def __init__(self):
        super().__init__()
        self._sem = asyncio.Semaphore(settings.get("limits.concurrency", 2))
        self._inflight: dict = {}   # ref.key() -> True，重复@去重

    async def process(self, callback: dingtalk_stream.CallbackMessage):
        data = callback.data or {}
        asyncio.create_task(self._handle(data))
        return AckMessage.STATUS_OK, "OK"

    async def _handle(self, data: dict):
        text = ((data.get("text") or {}).get("content") or "").strip()
        webhook = data.get("sessionWebhook")
        sender = data.get("senderStaffId")
        conv = data.get("conversationId") or "default"
        if not webhook:
            return
        # 空 text（光@不带话）也放行：走"最近失败任务候选列表"兜底
        log.info("收到@消息: %r from %s", text, data.get("senderNick"))
        key = (conv, sender)

        if text.lower() in ("help", "帮助", "用法"):
            _reply_markdown(webhook, *render.render_help(), at_user=sender)
            return

        # 候选序号选择（上次列表后的数字回复）
        picked = _take_choice(key, text)
        if picked:
            await self._run_analysis(picked, webhook, sender)
            return

        ref, cands = orchestrator.resolve_task_ref(text)
        if not ref and not cands:
            recent = []
            try:
                recent = orchestrator.recent_failure_candidates(5)
            except Exception:
                log.exception("获取最近失败任务失败")
            if recent:
                _put_choices(key, recent)
                _reply_markdown(webhook, *render.render_candidates(
                    recent, "没有从消息里定位到任务。最近的失败任务如下，"
                            "回复**序号**选择，或带上任务名重新@我："), at_user=sender)
            else:
                title, md = render.render_help()
                _reply_markdown(webhook, title,
                                "⚠️ 没能定位到任务。请带上任务名，或直接粘贴告警行"
                                "（如 `<DAG: xx>-20261023-yy Failed`）。\n\n" + md,
                                at_user=sender)
            return
        if not ref and cands:
            _put_choices(key, cands)
            _reply_markdown(webhook, *render.render_candidates(
                cands, "该任务名匹配到多个任务，请回复**序号**选择："), at_user=sender)
            return
        await self._run_analysis(ref, webhook, sender)

    async def _run_analysis(self, ref, webhook, sender):
        # 第一步：定位 + 查缓存（不调 LLM）
        try:
            cached = await asyncio.to_thread(orchestrator.check_cached, ref)
        except LogNotFound:
            _reply_markdown(
                webhook, "未找到日志",
                f"❌ 没找到 `{ref.dag_id}.{ref.task_id}` 的日志"
                f"（run_id={ref.run_id or '未指定'}）。请确认任务已执行且日志存在。",
                sender,
            )
            return
        except Exception:
            log.exception("定位/查缓存失败 %s.%s", ref.dag_id, ref.task_id)
            _reply_markdown(webhook, "排查失败",
                            f"❌ 排查 `{ref.dag_id}.{ref.task_id}` 时程序出错（已记录日志）。",
                            sender)
            return
        if cached:
            _reply_markdown(webhook, *render.render_result(cached, ref, cached=True),
                            at_user=sender)
            return
        if ref.key() in self._inflight:
            _reply_markdown(webhook, "排查中",
                            f"⏳ `{ref.dag_id}.{ref.task_id}` 正在排查中，"
                            f"完成后会自动回推，无需重复@。", sender)
            return

        self._inflight[ref.key()] = True
        background = False
        try:
            _reply_markdown(
                webhook, "正在排查",
                f"🛠 收到，正在排查 `{ref.dag_id}.{ref.task_id}`"
                f"（run={ref.run_id}），预计 1-3 分钟出结果…", sender)
            timeout = settings.get("limits.analysis_timeout", 180)
            analysis = asyncio.create_task(
                asyncio.to_thread(orchestrator.analyze_task, ref, True))
            try:
                async with self._sem:
                    result, _ = await asyncio.wait_for(asyncio.shield(analysis), timeout)
            except asyncio.TimeoutError:
                background = True
                _reply_markdown(
                    webhook, "转后台继续",
                    f"⏱ `{ref.dag_id}.{ref.task_id}` 超过 {timeout}s 仍在排查"
                    f"（LLM/查询较慢），已转后台继续，完成后自动回推，无需重新@。", sender)
                asyncio.create_task(self._finish_later(analysis, ref, webhook, sender))
                return
            _reply_markdown(webhook, *render.render_result(result, ref), at_user=sender)
        except LLMNotConfigured as e:
            _reply_markdown(webhook, "配置缺失", f"❌ {e}", sender)
        except Exception:
            log.exception("分析失败 %s.%s", ref.dag_id, ref.task_id)
            _reply_markdown(webhook, "排查失败",
                            f"❌ 排查 `{ref.dag_id}.{ref.task_id}` 出错（已记录日志），"
                            f"请稍后重@。", sender)
        finally:
            if not background:
                self._inflight.pop(ref.key(), None)

    async def _finish_later(self, analysis, ref, webhook, sender):
        try:
            result, _ = await asyncio.wait_for(analysis, 900)
            _reply_markdown(webhook, *render.render_result(result, ref), at_user=sender)
        except Exception:
            log.exception("后台排查失败 %s.%s", ref.dag_id, ref.task_id)
            _reply_markdown(webhook, "排查失败",
                            f"❌ `{ref.dag_id}.{ref.task_id}` 后台排查失败（已记录日志）。",
                            sender)
        finally:
            self._inflight.pop(ref.key(), None)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    client_id = os.getenv("DINGTALK_CLIENT_ID")
    client_secret = os.getenv("DINGTALK_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise SystemExit("未配置 DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET（.env）")

    from .. import alert_server
    alert_server.start_background()

    client = dingtalk_stream.DingTalkStreamClient(
        dingtalk_stream.Credential(client_id, client_secret)
    )
    client.register_callback_handler(TOPIC, AgentHandler())
    log.info("钉钉机器人启动（Stream 模式），等待群内 @消息…")
    client.start_forever()
