#!/usr/bin/env bash
# EXP-18：备份工具（宿主机执行；产物写宿主机目录 = K8s 数据卷故障域之外）。
#
# 用法：
#   backup.sh full    <outdir>   一致性全量 dump + binlog 归档 + 元数据（备份点 T_b）
#   backup.sh binlogs <outdir>   仅刷新 binlog 归档（故障注入前最后时刻 T_f）
#   backup.sh ledger  <pod内k6日志路径> <outfile>   从 k6 日志提取成功响应账本
#
# 主线备份方案（冻结）：
#   mysqldump --single-transaction --source-data=2 --set-gtid-purged=ON
#   —— InnoDB 一致性快照 + 记录 binlog 位点 + 导入时还原 gtid_purged；
#   目标 RPO < 备份间隔 ⇒ binlog 归档 + GTID 重放（PITR，已执行 GTID 自动跳过）。
#
# 恢复目标环境 gtid_executed 必须为空、gtid_mode=ON（见 deploy/k8s/dr/mysql-dr.yaml）。
set -euo pipefail

NS=order-lab
ROOT_PW=flash-root
MODE=${1:-}
OUT=${2:-backup}

primary_pod() {
  local host
  for p in mysql-ic-0 mysql-ic-1 mysql-ic-2; do
    host=$(kubectl -n "$NS" exec "$p" -- mysql -uroot -p"$ROOT_PW" -N -B -e \
      "SELECT member_host FROM performance_schema.replication_group_members WHERE member_role='PRIMARY'" 2>/dev/null || true)
    if [ -n "${host:-}" ]; then
      echo "${host%%.*}"
      return 0
    fi
  done
  echo "ERROR: PRIMARY 未找到（InnoDB Cluster 不可用？）" >&2
  return 1
}

mysql_exec() { kubectl -n "$NS" exec "$P" -- mysql -uroot -p"$ROOT_PW" "$@"; }

case "$MODE" in
full)
  mkdir -p "$OUT"
  P=$(primary_pod)
  TS=$(date +%Y%m%d-%H%M%S)
  echo "PRIMARY=$P  TS=$TS  OUT=$OUT"

  echo "[1/5] 一致性全量 dump（--single-transaction，负载持续写入中）..."
  if ! kubectl -n "$NS" exec "$P" -- mysqldump -uroot -p"$ROOT_PW" \
      --single-transaction --source-data=2 --set-gtid-purged=ON \
      --hex-blob --triggers --routines --events \
      --databases flash > "$OUT/flash-full-$TS.sql" 2> "$OUT/dump-stderr-$TS.log"; then
    echo "--source-data 不可用，回退 --master-data（旧选项名）"
    kubectl -n "$NS" exec "$P" -- mysqldump -uroot -p"$ROOT_PW" \
      --single-transaction --master-data=2 --set-gtid-purged=ON \
      --hex-blob --triggers --routines --events \
      --databases flash > "$OUT/flash-full-$TS.sql" 2>> "$OUT/dump-stderr-$TS.log"
  fi

  echo "[2/5] 备份可用性证明（dump 完成标记 + gtid_purged + 位点）..."
  grep -c "Dump completed on" "$OUT/flash-full-$TS.sql" | xargs echo "  dump 完成标记:"
  grep -im1 "SET @@GLOBAL.GTID_PURGED" "$OUT/flash-full-$TS.sql" | head -c 200; echo
  grep -im1 -E "CHANGE (MASTER|REPLICATION)" "$OUT/flash-full-$TS.sql" | head -c 200; echo

  echo "[3/5] 元数据（GTID 水位 + binlog 清单）..."
  mysql_exec -N -B -e "SELECT @@GLOBAL.gtid_executed" > "$OUT/gtid-$TS.txt"
  mysql_exec -N -B -e "SHOW MASTER STATUS" > "$OUT/master-status-$TS.txt"
  mysql_exec -N -B -e "SHOW BINARY LOGS" > "$OUT/binlog-list-$TS.txt"
  cat "$OUT/master-status-$TS.txt" | head -2

  echo "[4/5] binlog 归档（datadir → 宿主机）..."
  BFILES=$(awk '{print $1}' "$OUT/binlog-list-$TS.txt" | tr '\n' ' ')
  # shellcheck disable=SC2086
  kubectl -n "$NS" exec "$P" -- tar cf - -C /var/lib/mysql $BFILES > "$OUT/binlogs-$TS.tar"

  echo "[5/5] 校验和..."
  ( cd "$OUT" && shasum -a 256 "flash-full-$TS.sql" "binlogs-$TS.tar" > "SHA256SUMS-$TS.txt" && cat "SHA256SUMS-$TS.txt" )
  echo "完成：$OUT/flash-full-$TS.sql + binlogs-$TS.tar"
  ;;

binlogs)
  mkdir -p "$OUT"
  P=$(primary_pod)
  TS=$(date +%Y%m%d-%H%M%S)
  echo "PRIMARY=$P  TS=$TS （仅刷新 binlog 归档）"
  mysql_exec -N -B -e "SELECT @@GLOBAL.gtid_executed" > "$OUT/gtid-$TS.txt"
  mysql_exec -N -B -e "SHOW MASTER STATUS" > "$OUT/master-status-$TS.txt"
  mysql_exec -N -B -e "SHOW BINARY LOGS" > "$OUT/binlog-list-$TS.txt"
  cat "$OUT/master-status-$TS.txt" | head -2
  BFILES=$(awk '{print $1}' "$OUT/binlog-list-$TS.txt" | tr '\n' ' ')
  # shellcheck disable=SC2086
  kubectl -n "$NS" exec "$P" -- tar cf - -C /var/lib/mysql $BFILES > "$OUT/binlogs-$TS.tar"
  ( cd "$OUT" && shasum -a 256 "binlogs-$TS.tar" > "SHA256SUMS-$TS.txt" )
  echo "完成：$OUT/binlogs-$TS.tar"
  ;;

ledger)
  LOGPATH=${2:-/tmp/r1.log}
  OUTFILE=${3:-ledger.txt}
  # k6 console.log 落在 nohup 日志里（logfmt：msg="LEDGER|..." 或 msg=LEDGER|...），
  # 内容不含引号/空格，两种形态都能用同一模式截取。
  kubectl -n "$NS" exec k6-load -- grep -aoE "LEDGER\|[0-9]+\|[^\"']*" "$LOGPATH" > "$OUTFILE"
  echo "账本条目（含 200/201 重复）：$(wc -l < "$OUTFILE" | tr -d ' ')"
  ;;

*)
  echo "用法: $0 {full|binlogs} <outdir>   |   $0 ledger <pod内日志路径> <outfile>" >&2
  exit 1
  ;;
esac
