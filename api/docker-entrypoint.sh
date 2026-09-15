#!/bin/sh
set -e
# 宿主机 bind-mount 的 uploads/logs/.worker 常为 root 所属；切到 appuser 前修正权限
mkdir -p /app/uploads /app/logs /app/.worker
chown -R appuser:appuser /app/uploads /app/logs /app/.worker
exec runuser -u appuser -- "$@"
