# EXP-19 手工操作手册：全链路长稳与受控 Game Day

> 目标：D1 基线复测 → D2 过载复核 → D4 故障后剩余容量 → D5 受控组合故障
> （Game Day）→ D3 2 小时长稳 → D6 验收矩阵与复盘。
> 前置：EXP-01～18 必做验收全部完成；环境为 EXP-18 末态（原环境已恢复）。
> 预期总耗时：约 3.5 小时（含 2h 长稳后台窗口）。

## 0. 冻结口径（本实验不得中途调整）

```text
版本：order-api/outbox-relay/consumer = exp17a；配置 = all.yaml 冻结值
  （600rps 基线、CACHE_TTL=600s、BACKFILL=12、STALE_MAX=1024、限流 1500/副本）
SLO：读 P99≤200ms / 写 P99≤500ms / 系统错误≤0.1%（正常态）；
  故障态口径按各场景声明；不能用全程平均稀释故障窗口（roadmap §3.2）。
对照场景控制变量：D1/D2/D4/D5 每场景前跑 warmup（10000 SKU），
  确保测试窗口落在同一个 TTL 热缓存周期内（预热后 600s 内完成场景）。
长稳 D3 不做预热控制：完整暴露 ~12 个 TTL 过期周期（这正是要测的稳态现实）。
```

## 1. D1：基线复测（600 rps × 3min × 2 轮）

```bash
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=600 -e DURATION=180s /tmp/fixed-rate.js > /tmp/d1.log 2>&1 &'
# 第二轮（冷缓存对照，验证 TTL 过期假说）：空闲 >600s 后直接再跑（d1b）
# 判读：热缓存 ≈0.5% 失败（与 EXP-17 G1 的 0.57% 同源：开场冷页+边界噪声）；
#       TTL 过期后的冷缓存轮 ~5% 失败（回源风暴，见 EXP-19 报告 D1/D1b 对照）
```

## 2. D2：过载复核（900 rps × 2min，热缓存）

```bash
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=900 -e DURATION=120s /tmp/fixed-rate.js > /tmp/d2.log 2>&1 &'
# 验收：明确拒绝（429/503/504 分类）而非雪崩；恢复后正常
# Prometheus 错误分类：sum by (status)(increase(http_requests_total{status=~"[45].."}[2m]))
```

## 3. D4：故障后剩余容量（order-api 2→1 + router 2→1，600 rps × 3min）

```bash
kubectl -n order-lab scale deploy/order-api --replicas=1
kubectl -n order-lab scale deploy/mysql-router --replicas=1
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=600 -e DURATION=180s /tmp/fixed-rate.js > /tmp/d4.log 2>&1 &'
# 结束后恢复双副本
kubectl -n order-lab scale deploy/order-api deploy/mysql-router --replicas=2 --replicas=2 2>/dev/null
kubectl -n order-lab scale deploy/order-api --replicas=2
kubectl -n order-lab scale deploy/mysql-router --replicas=2
```

## 4. D5：受控 Game Day（600 rps × 8min，LEDGER=1）

```bash
# 组合两个已单故障验收过的场景：EXP-08 Redis 不可用 + EXP-14 Pod 硬中断
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=600 -e DURATION=480s -e LEDGER=1 /tmp/fixed-rate.js > /tmp/d5.log 2>&1 &'
kubectl -n order-lab port-forward svc/order-api 18080:8080 &   # 故障期抽查用

# T+60s：注入1 —— Redis 不可用（EXP-08 场景）
kubectl -n order-lab scale deploy/redis --replicas=0
curl -s -D - -o /dev/null localhost:18080/api/products/P1 | grep -iE '^HTTP|x-cache'
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp19-gameday-1' -d '{"user_id":"exp19","sku":"P1","qty":1}' \
  -o /dev/null -w '%{http_code}\n'      # 预期 201（写路径不依赖 Redis）

# T+150s：注入2 —— 强杀一个 order-api Pod（EXP-14 场景）
kubectl -n order-lab delete pod <一个 order-api pod> --force --grace-period=0

# T+240s：恢复 Redis（注意：--save "" 无持久化 → 重启即冷缓存）
kubectl -n order-lab scale deploy/redis --replicas=1
# 收敛观察：X-Cache miss→hit；错误率回落

# 结束后：恢复验证（600 rps × 2min 复测）+ 对账
deploy/backup/backup.sh ledger /tmp/d5.log backup-exp18/ledger-d5.txt
bin/reconcile -dsn '...' -mode check -baseline <exp19基线> \
  -ledger backup-exp18/ledger-d5.txt -strict-ledger
# 告警验证：ALERTS 时间线（OrderAPIErrorRateHigh/P99High/DeadlineHigh 触发与恢复）
```

## 5. D3：2 小时长稳（600 rps × 7200s，LEDGER=1）

```bash
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec mysql-ic-1 -- mysql -uroot -pflash-root -N -B -e \
  "SELECT COUNT(*) FROM flash.orders"   # 记录起始水位
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=600 -e DURATION=7200s -e LEDGER=1 /tmp/fixed-rate.js > /tmp/d3.log 2>&1 &'

# 每 20-30min 采样（长稳纪律：资源/队列不能无解释地持续增长）：
#   k6 进度行：grep 'running (' /tmp/d3.log | tail -1
#   内存：container_memory_working_set_bytes（order-api/relay/consumer/mysql-ic）
#   队列：outbox PENDING、consumer lag
#   错误率按 10min 窗口（识别 TTL 周期模式，不能用全程平均稀释）

# 结束后：
deploy/backup/backup.sh ledger /tmp/d3.log backup-exp18/ledger-d3.txt
bin/reconcile -dsn '...' -mode check -baseline <exp19基线> \
  -ledger backup-exp18/ledger-d3.txt -strict-ledger
```

## 6. D6：验收矩阵与复盘

- 按 roadmap §6.2 填最终验收矩阵（见 EXP-19.md 第 13 节）；
- 复盘落 `docs/postmortems/exp19-graduation-retrospective.md`；
- 未覆盖/失败项如实标记（结论边界），不宣称「已投产就绪」。

## 本实验要回答的问题

1. 冻结配置下 2 小时长稳：成功订单 TPS 多少？资源有没有无解释的增长？
   每 10 分钟的 TTL 过期周期在错误率/P99 上是什么形状？
2. 900 rps 过载时系统是「明确拒绝」还是雪崩？拒绝怎么分类？
3. 失去一个 order-api + 一个 Router 后，600 rps 还能满足 SLO 吗？
4. Redis 不可用叠加 Pod 强杀：读路径降级到什么程度？写路径还活着吗？
   Redis 恢复后为什么没有立即恢复（冷缓存重预热）？
5. 19 个实验的能力组合后，哪些验收条目通过、哪些如实标记为边界？
