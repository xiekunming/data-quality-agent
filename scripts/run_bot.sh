#!/bin/bash
# 机器人单实例启动入口（launchd 与手动共用）：
#   - launchd 调用：输出进 plist 的 StandardOutPath（logs/bot.log，由 launchd 自己创建）
#   - 手动调用（经 start_bot.sh）：输出进 logs/bot-manual.log
# 本脚本自身不重定向任何日志文件 —— 避免终端侧进程创建 bot.log 后，
# launchd 拒绝写入（macOS provenance/macl 坑，last exit 78，见 operations.md §4.1）
cd "$(dirname "$0")/.." || exit 1
mkdir -p logs
# 单实例守卫：已有实例在跑就安静退出（launchd 配 SuccessfulExit=false 不会重启，避免互踢）
if pgrep -f "python -m dq_agent" >/dev/null 2>&1; then
    echo "$(date '+%F %T') [run_bot] 已有实例在运行，跳过"
    exit 0
fi
# 钉钉/GLM/本机地址不走系统代理（国内域名直连；否则代理程序一关，机器人跟着断连）。
# requests 会优先读 NO_PROXY 环境变量，优先于 macOS 系统代理设置，大小写都导出以兼容各 HTTP 库
export NO_PROXY="localhost,127.0.0.1,dingtalk.com,bigmodel.cn"
export no_proxy="$NO_PROXY"
exec .venv/bin/python -m dq_agent
