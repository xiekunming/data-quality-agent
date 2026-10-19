"""本地预采集服务（可选）：Airflow 告警回调同时 POST 到这里，提前构建快照

POST /alert  body: {"dag_id": "...", "task_id": "...", "run_id": "..."}
             或纯文本（含 dag=xx | task=yy | run=zz 标识行）
不调 LLM，纯文件解析，毫秒级。
"""

import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import orchestrator
from .config import settings
from .models import TaskRef

log = logging.getLogger(__name__)


def _ref_from_body(body: str) -> TaskRef | None:
    try:
        d = json.loads(body)
        if d.get("dag_id") and d.get("task_id"):
            return TaskRef(d["dag_id"], d["task_id"], d.get("run_id"))
    except (json.JSONDecodeError, AttributeError):
        pass
    return orchestrator.parse_task_ref(body)


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/alert":
            self._reply(404, {"error": "not found"})
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode("utf-8", "replace")
        ref = _ref_from_body(body)
        if not ref:
            self._reply(400, {"error": "无法解析任务标识，body 示例: "
                                       '{"dag_id":"d","task_id":"t","run_id":"r"} 或含 dag=|task=|run= 的文本'})
            return
        ok = orchestrator.precollect(ref)
        self._reply(200, {"status": "collected" if ok else "log_not_found",
                          "dag_id": ref.dag_id, "task_id": ref.task_id, "run_id": ref.run_id})

    def do_GET(self):
        self._reply(200, {"status": "ok"})

    def _reply(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):  # 静默默认访问日志
        log.debug("alert_server: " + fmt % args)


def start_background() -> tuple | None:
    """在守护线程里启动预采集 HTTP 服务，返回 (host, port) 或 None"""
    if not settings.get("alert_server.enabled", True):
        return None
    host = settings.get("alert_server.host", "127.0.0.1")
    port = settings.get("alert_server.port", 8765)
    server = ThreadingHTTPServer((host, port), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True, name="alert-server").start()
    log.info("预采集服务已启动 http://%s:%s/alert", host, port)
    return host, port
