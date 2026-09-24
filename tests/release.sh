#!/usr/bin/env bash
# EXP-15 可重复发布流程：build → smoke → 幂等/库存回归 → 发布 → SLO 检查。
# 用法：
#   tests/release.sh <tag>            # 发布正常版本（构建 + 门禁 + 滚动 + SLO 检查）
#   tests/release.sh <tag> --no-build # 跳过构建（镜像已存在）
#   tests/release.sh --rollback       # 回滚到上一个版本（rollout undo）
#   tests/release.sh --slo-check      # 仅做发布后 SLO 检查
#
# 门禁失败即退出非零；SLO 检查失败自动 rollout undo 并退出非零。
# 访问通道：宿主机 port-forward（k6-load 镜像无 curl，不能用 exec curl）。
set -euo pipefail

NS=order-lab
DEP=order-api
CONTAINER=order-api
IMAGE_PREFIX=order-api
PF_PORT=18080
PROM_PF_PORT=19090
SLO_ERR_RATE_MAX=0.01   # 5xx / 全部 API 请求 > 1% 判定发布失败（对齐 OrderAPIErrorRateHigh 告警）

log()  { echo "[release] $*"; }
fail() { echo "[release] FAIL: $*" >&2; exit 1; }

API="http://localhost:$PF_PORT"
PF_PID=""
PROM_PF_PID=""

cleanup() {
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
  [[ -n "$PROM_PF_PID" ]] && kill "$PROM_PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

# 启动对 order-api 的 port-forward（幂等：已存在则复用）
ensure_pf() {
  if ! curl -s -m 2 "$API/healthz" >/dev/null 2>&1; then
    kubectl -n "$NS" port-forward svc/order-api "$PF_PORT:8080" >/tmp/release-pf.log 2>&1 &
    PF_PID=$!
    sleep 3
  fi
}

# smoke：健康检查 + 商品查询 + 一次下单
smoke() {
  log "smoke: healthz/readyz/product"
  local h r p
  h=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$API/healthz" || echo 000)
  r=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$API/readyz" || echo 000)
  p=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$API/api/products/P1" || echo 000)
  [[ "$h" == 200 && "$r" == 200 && "$p" == 200 ]] || fail "smoke health: h=$h r=$r p=$p"
  log "smoke OK"
}

# 幂等/库存回归：5 单（不同键）+ 同键重放 + 无重复（EXP-03 口径的发布版）
regression() {
  log "regression: 5 orders + replay"
  local i o replay key
  for i in $(seq 1 5); do
    key="rel-$(date +%s)-$i"
    o=$(curl -s -m 8 -X POST "$API/api/orders" \
      -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
      -d '{"user_id":"rel-u","sku":"P1","qty":1}')
    echo "$o" | grep -q '"id"' || fail "regression order $i: $o"
  done
  replay=$(curl -s -m 8 -X POST "$API/api/orders" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d '{"user_id":"rel-u","sku":"P1","qty":1}')
  echo "$replay" | grep -q '"replayed":true' || fail "regression replay: $replay"
  log "regression OK"
}

# SLO 检查：Prometheus 最近 2min 5xx 错误率（发布门禁核心）
slo_check() {
  log "slo-check: Prometheus 5xx rate (last 2m)"
  local err_rate
  if ! curl -s -m 2 "http://localhost:$PROM_PF_PORT/api/v1/query" >/dev/null 2>&1; then
    kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus \
      "$PROM_PF_PORT:9090" >/tmp/release-prom-pf.log 2>&1 &
    PROM_PF_PID=$!
    sleep 3
  fi
  # 用 python urlencode 避免 shell 对 PromQL 引号的二次转义问题
  err_rate=$(python3 - "$PROM_PF_PORT" <<'EOF'
import json, sys, urllib.request, urllib.parse
q = 'sum(rate(http_requests_total{namespace="order-lab",route=~"/api/.*",status=~"5.."}[2m])) / clamp_min(sum(rate(http_requests_total{namespace="order-lab",route=~"/api/.*"}[2m])),1)'
url = f'http://localhost:{sys.argv[1]}/api/v1/query?' + urllib.parse.urlencode({'query': q})
try:
    d = json.load(urllib.request.urlopen(url, timeout=8))
    r = d['data']['result']
    # 无 5xx 序列（sum 为空）→ 错误率按 0 处理；请求失败才按 1（失败）
    print(r[0]['value'][1] if r else '0')
except Exception:
    print('1')
EOF
)
  awk "BEGIN{exit !($err_rate < $SLO_ERR_RATE_MAX)}" || return 1
  log "slo-check OK (err_rate=$err_rate < $SLO_ERR_RATE_MAX)"
}

# ---------- 主流程 ----------
case "${1:-}" in
  --rollback)
    log "rollback: rollout undo $DEP"
    kubectl -n "$NS" rollout undo deploy/"$DEP"
    kubectl -n "$NS" rollout status deploy/"$DEP" --timeout=300s
    ensure_pf; smoke
    log "rollback OK"
    exit 0
    ;;
  --slo-check)
    slo_check || fail "slo-check failed"
    exit 0
    ;;
  "")
    fail "usage: $0 <tag> [--no-build] | --rollback | --slo-check"
    ;;
esac

TAG="$1"
NO_BUILD="${2:-}"

# 1. build（本地 docker，镜像对 k3s 可见；网络受限时用离线注入方式手工构建）
if [[ "$NO_BUILD" != "--no-build" ]]; then
  log "build: $IMAGE_PREFIX:$TAG"
  docker build -t "$IMAGE_PREFIX:$TAG" .
fi

# 2. 发布前：当前版本冒烟 + 回归（发布基线健康）
ensure_pf
log "pre-release smoke on current version"
smoke
regression

# 3. 发布（滚动，maxSurge=0/maxUnavailable=1，EXP-14 冻结）
log "release: set image $IMAGE_PREFIX:$TAG"
kubectl -n "$NS" set image deploy/"$DEP" "$CONTAINER=$IMAGE_PREFIX:$TAG"
kubectl -n "$NS" rollout status deploy/"$DEP" --timeout=300s

# 4. 发布后 smoke + 回归
ensure_pf
smoke
regression

# 5. 发布后 SLO 检查（失败自动回滚）
if ! slo_check; then
  log "SLO check failed → automatic rollback"
  kubectl -n "$NS" rollout undo deploy/"$DEP"
  kubectl -n "$NS" rollout status deploy/"$DEP" --timeout=300s
  fail "release $TAG failed SLO check, rolled back"
fi

log "release $TAG OK"
