#!/bin/bash
# 手动后台启动机器人（仅救急用；launchd 服务在管时用 launchctl kickstart -k 重启即可）
# 日志写 logs/bot-manual.log，与 launchd 的 bot.log 相互独立 —— 手动启动后无需
# 归档日志即可把服务交还 launchd，两份日志永不冲突
cd "$(dirname "$0")/.." || exit 1
if pgrep -f "python -m dq_agent" >/dev/null 2>&1; then
    echo "✅ 机器人已在运行（pgrep 确认）"; exit 0
fi
mkdir -p logs
nohup bash scripts/run_bot.sh >> logs/bot-manual.log 2>&1 &
sleep 5
if pgrep -f "python -m dq_agent" >/dev/null 2>&1; then
    echo "✅ 已启动，日志: logs/bot-manual.log（tail -f 跟踪）"
else
    echo "❌ 启动失败，看日志: tail -20 logs/bot-manual.log"; exit 1
fi
