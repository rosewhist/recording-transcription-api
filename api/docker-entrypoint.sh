#!/bin/sh
set -e
# 宿主机 bind-mount 的 uploads/logs 常为 root 所属；切到 appuser 前修正权限
mkdir -p /app/uploads /app/logs
chown -R appuser:appuser /app/uploads /app/logs
exec runuser -u appuser -- "$@"
