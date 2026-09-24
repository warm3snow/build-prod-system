#!/usr/bin/env bash
# EXP-18：恢复工具（宿主机执行）。Runbook 的每一步对应一个子命令，
# 受控顺序：deploy → import → [binlog] → 对账 → 恢复写入/事件链路（见 manual）。
#
# 用法：
#   restore.sh deploy                    部署 DR 栈（mysql 1 副本；api/relay/consumer 0）
#   restore.sh reset                     删除 order-dr 命名空间（PVC 一并删除=新卷重来）
#   restore.sh import <dump.sql>         导入全量备份（stdin 灌入 DR MySQL）
#   restore.sh binlog <binlogs.tar>      重放归档 binlog（PITR；已执行 GTID 自动跳过）
#   restore.sh status                    计数水位 + GTID
set -euo pipefail

NS=order-dr
ROOT_PW=flash-root
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

dr_mysql() { kubectl -n "$NS" exec -i deploy/mysql-dr -- mysql -uroot -p"$ROOT_PW" "$@"; }

case "${1:-}" in
deploy)
  kubectl apply -f "$REPO_ROOT/deploy/k8s/dr/mysql-dr.yaml"
  kubectl -n "$NS" rollout status deploy/mysql-dr --timeout=180s
  echo "DR MySQL 就绪（空卷、GTID ON）。下一步：restore.sh import <dump.sql>"
  ;;

reset)
  kubectl delete namespace "$NS" --ignore-not-found
  echo "order-dr 已删除（含 PVC——下一轮恢复必须新卷，不得复用原数据卷）。"
  ;;

import)
  DUMP=${2:?用法: restore.sh import <dump.sql>}
  [ -s "$DUMP" ] || { echo "dump 文件不存在或为空: $DUMP" >&2; exit 1; }
  echo "导入全量备份 ${DUMP}（大小 $(du -h "$DUMP" | cut -f1)）..."
  dr_mysql < "$DUMP"
  echo "导入完成。水位："
  dr_mysql -N -B -e "
    SELECT 'orders', COUNT(*) FROM flash.orders
    UNION ALL SELECT 'outbox_sent', COUNT(*) FROM flash.outbox_events WHERE status='SENT'
    UNION ALL SELECT 'outbox_pending', COUNT(*) FROM flash.outbox_events WHERE status='PENDING'
    UNION ALL SELECT 'inbox', COUNT(*) FROM flash.inbox_events
    UNION ALL SELECT 'notif', COUNT(*) FROM flash.order_notifications
    UNION ALL SELECT 'gtid_executed_len', CHAR_LENGTH(@@GLOBAL.gtid_executed)" 2>/dev/null || \
  dr_mysql -e "USE flash; SELECT (SELECT COUNT(*) FROM orders) orders,
    (SELECT COUNT(*) FROM outbox_events WHERE status='SENT') sent,
    (SELECT COUNT(*) FROM inbox_events) inbox,
    (SELECT COUNT(*) FROM order_notifications) notif;"
  dr_mysql -N -B -e "SELECT @@GLOBAL.gtid_executed" | head -c 300; echo
  ;;

binlog)
  TAR=${2:?用法: restore.sh binlog <binlogs.tar>}
  [ -s "$TAR" ] || { echo "binlog 归档不存在: $TAR" >&2; exit 1; }
  # mysql:8.0 镜像不含 mysqlbinlog（精简版）——用宿主机 mysqlbinlog 解析归档
  # （故障域外副本），SQL 流经 kubectl stdin 灌入 DR MySQL；已执行 GTID 自动跳过。
  MB=$(command -v mysqlbinlog || true)
  [ -n "$MB" ] || { echo "宿主机无 mysqlbinlog（brew install mysql-client）" >&2; exit 1; }
  TMP=$(mktemp -d /tmp/binlog-replay.XXXXXX)
  tar xf "$TAR" -C "$TMP"
  echo "解析归档：$(ls "$TMP" | wc -l | tr -d ' ') 个 binlog 文件（已执行 GTID 自动跳过，只补增量）..."
  date -u +"REPLAY-START=%H:%M:%S"
  # shellcheck disable=SC2046
  "$MB" $(ls "$TMP"/binlog.* | sort) 2>"$TMP/mb-stderr.log" | \
    kubectl -n "$NS" exec -i deploy/mysql-dr -- mysql -uroot -p"$ROOT_PW" 2>"$TMP/mysql-stderr.log" \
    || { echo "重放失败："; tail -5 "$TMP/mb-stderr.log" "$TMP/mysql-stderr.log"; exit 1; }
  date -u +"REPLAY-END=%H:%M:%S"
  echo "重放后水位与 GTID："
  kubectl -n "$NS" exec deploy/mysql-dr -- mysql -uroot -p"$ROOT_PW" -N -B -e "
    SELECT 'orders', COUNT(*) FROM flash.orders
    UNION ALL SELECT 'outbox_sent', COUNT(*) FROM flash.outbox_events WHERE status='SENT'
    UNION ALL SELECT 'inbox', COUNT(*) FROM flash.inbox_events
    UNION ALL SELECT 'notif', COUNT(*) FROM flash.order_notifications
    UNION ALL SELECT 'gtid', @@GLOBAL.gtid_executed" 2>/dev/null | cut -c1-220
  rm -rf "$TMP"
  ;;

status)
  dr_mysql -N -B -e "SELECT 'orders', COUNT(*) FROM flash.orders" 2>/dev/null || true
  kubectl -n "$NS" get deploy,pod 2>/dev/null || true
  ;;

*)
  echo "用法: $0 {deploy|reset|import <dump>|binlog <tar>|status}" >&2
  exit 1
  ;;
esac
