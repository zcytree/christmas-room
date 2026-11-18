#!/usr/bin/env bash
# 带守护的启动脚本：node 挂了自动拉起，端口默认 3000
cd "$(dirname "$0")"
: "${PORT:=3000}"
export PORT
while true; do
  node server.js >> server.log 2>&1
  echo "[watchdog] server exited, restarting in 2s" >> server.log
  sleep 2
done
