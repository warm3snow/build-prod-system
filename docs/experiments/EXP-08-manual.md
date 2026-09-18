# EXP-08 手工操作手册：缓存失效与有界回源

> 目标：复现热点过期对照（合并 on/off）、Redis 故障与恢复、负缓存回源压缩。
> 前置：EXP-07 环境在 Rancher Desktop k3s 运行（order-lab 命名空间），最终镜像 `order-api:exp08e`。
> 预期总耗时：约 60 分钟（含 2 轮 3 分钟 + 1 轮 5 分钟压测）。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
# 预期：mysql、redis、order-api 均 Running（镜像 order-api:exp08e）
kubectl -n order-lab get deploy order-api -o jsonpath='{.spec.template.spec.containers[0].image}'; echo
# 预期：order-api:exp08e
```

快捷方式（沿用 EXP-07）：

```bash
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
alias redis-lab='kubectl -n order-lab exec deploy/redis -- redis-cli'
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
```

## 1. 构建说明（网络受限时）

本实验镜像 exp08a→exp08e 均通过离线方式构建（构建机无法访问 Docker Hub/gcr.io）：

```bash
# 仓库根目录：本地交叉编译 + 复用 exp07c 底座注入
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/order-api-exp08e ./cmd/order-api
CID=$(docker create --name tmp order-api:exp07c)
docker cp /tmp/order-api-exp08e $CID:/order-api
docker commit $CID order-api:exp08e && docker rm $CID
```

网络可用时直接用标准 `docker build -t order-api:exp08e .`（Dockerfile 未变）。

## 2. 冒烟（5 分钟）

```bash
# 命中与回源
curl -s localhost:18080/api/products/P1 -D - -o /dev/null | grep -i x-cache   # miss
curl -s localhost:18080/api/products/P1 -D - -o /dev/null | grep -i x-cache   # hit

# 负缓存语义：第一次 miss（回源确认不存在），第二次 neg
curl -s localhost:18080/api/products/NX-1 -D - -o /dev/null | grep -iE 'HTTP|x-cache'   # 404 miss
curl -s localhost:18080/api/products/NX-1 -D - -o /dev/null | grep -iE 'HTTP|x-cache'   # 404 neg

# stale 路径：预热 → 停 Redis → 读旧值
curl -s -o /dev/null localhost:18080/api/products/P1
kubectl -n order-lab scale deploy/redis --replicas=0 && sleep 5
curl -s localhost:18080/api/products/P1 -D - -o /dev/null | grep -iE 'HTTP|x-cache'   # 200 stale
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp08-smoke-1' -d '{"user_id":"u1","sku":"P1","qty":1}' -o /dev/null -w '%{http_code}\n'  # 201
kubectl -n order-lab scale deploy/redis --replicas=1
```

**冒烟完成标志**：hit/miss/neg/stale 四种头可观察，Redis 故障期间下单 201。

## 3. 场景 A：热点过期对照（约 25 分钟）

> 冻结口径：单热点 P1、`CACHE_TTL=5s`（放大过期频率）、纯读 4000 QPS × 3min × 2 组。

### Step A0：准备

```bash
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 2400
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=90s
# 仓库根目录：拷脚本（hotkey-expire.js / cache-warmup.js）
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/hotkey.js' < tests/load/hotkey-expire.js
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/warmup.js' < tests/load/cache-warmup.js
kubectl -n order-lab set env deployment/order-api CACHE_TTL=5s CACHE_COALESCE_ENABLED=true
kubectl -n order-lab rollout status deploy/order-api --timeout=60s
redis-lab FLUSHDB
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=1 /tmp/warmup.js
```

### Step A1：合并开启（当前配置）

```bash
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"      # 记录 A0
kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=4000 -e DURATION=3m -e HOT=1 /tmp/hotkey.js 2>&1 | tee /tmp/a1.log | tail -1'
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"      # 记录 A1
# Prometheus（10s 抓取间隔，压测结束后等一个周期再查）：
#   sum(cache_backfill_wanted_total) by (op)    → wanted（≈ k6 cache_misses + 少量）
#   sum(cache_backfill_executed_total) by (op)  → executed
# 合并率 = 1 - executed/wanted，预期 ≈ 98%
```

### Step A2：关闭合并

```bash
kubectl -n order-lab set env deployment/order-api CACHE_COALESCE_ENABLED=false
kubectl -n order-lab rollout status deploy/order-api --timeout=60s
redis-lab FLUSHDB
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=1 /tmp/warmup.js
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"      # B0
kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=4000 -e DURATION=3m -e HOT=1 /tmp/hotkey.js 2>&1 | tee /tmp/a2.log | tail -1'
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"      # B1
```

**场景 A 完成标志**：合并组 DB 回源 ≈ 1/13 于无合并组；无合并组 p95 明显恶化（~210ms），
且 stale 计数大增（回源风暴挤占 Redis 池），但两组 http_req_failed 均为 0。

## 4. 场景 B：Redis 故障与恢复（约 15 分钟）

> 故障与恢复属于同一个场景：同一条压测时间线内先停后恢复，恢复期观察即本场景的一部分。
> 冻结口径：8:1:1 混合 2000 QPS × 5min（cache-product.js），压测 60s 后停 Redis，90s 后恢复。

### Step B0：准备

```bash
kubectl -n order-lab set env deployment/order-api CACHE_TTL=60s CACHE_COALESCE_ENABLED=true
kubectl -n order-lab rollout status deploy/order-api --timeout=60s
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/cache.js' < tests/load/cache-product.js
mysql-lab -e "UPDATE inventory SET stock=200000 WHERE sku='P1';"   # 补库存并记录水位
mysql-lab -e "SELECT sku,stock FROM inventory WHERE sku IN ('P1','P2');
              SELECT sku,COUNT(*) o FROM orders WHERE sku IN ('P1','P2') GROUP BY sku;"
redis-lab FLUSHDB
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=100 /tmp/warmup.js
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"            # 记录基准
```

### Step B1：压测 + 故障 + 恢复（时间线操作）

```bash
# T+0s：后台启动压测
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2000 -e DURATION=5m -e SKU_COUNT=100 /tmp/cache.js > /tmp/b.log 2>&1 &'
# T+60s：停 Redis
kubectl -n order-lab scale deploy/redis --replicas=0
# T+90s：故障期抽查（预期：读 200 + X-Cache: stale；下单 201 可成功）
curl -s localhost:18080/api/products/P1 -D - -o /dev/null | grep -iE 'HTTP|x-cache'
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp08-fault-1' -d '{"user_id":"u1","sku":"P1","qty":1}' -o /dev/null -w '%{http_code}\n'
# T+150s：恢复 Redis
kubectl -n order-lab scale deploy/redis --replicas=1
# T+180s~：恢复期观察（预期：恢复后 ~70s 内仍 stale（go-redis 池重连窗口，本地旧值兜底）；
#   随后 cache_errors_total 归零、回源速率有界 ~8/s，P1 走 stale → miss → hit 收敛）
curl -s localhost:18080/api/products/P1 -D - -o /dev/null | grep -i x-cache
curl -s -o /dev/null localhost:18080/api/products/P1   # 间隔几十秒重复观察收敛
```

### Step B2：结果与对账

```bash
kubectl -n order-lab exec k6-load -- sh -c 'grep -E "http_req_failed|cache_stale|cache_reject|cache_hits|cache_misses" /tmp/b.log'
# 预期：http_req_failed < 0.5%；cache_stale 主导故障期读服务；cache_reject < 100
mysql-lab -e "SELECT sku,stock FROM inventory WHERE sku='P1';
              SELECT COUNT(*) new_orders FROM orders WHERE sku='P1' AND created_at >= '<b3 开始 UTC 时间>';"
# 守恒：stock 扣减 == 窗口内新订单数（跨轮幂等重放不扣库存）
```

**场景 B 完成标志**：故障期读以 stale 持续服务、下单保持成功、失败率 < 0.5%；
恢复初期 ~70s 内 stale 兜底（连接池重连窗口），此后 `cache_errors_total` 归零、
缓存经 stale → miss → hit 有界重建（回源 ~8/s），无 b2 式拒绝风暴；
库存守恒（stock 扣减 == 窗口内新订单数，重放不扣）。

## 5. 场景 C：负缓存回源（约 5 分钟）

```bash
# 仓库根目录：拷脚本（nx-backfill.js / nx-single-round.js）
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/nx.js' < tests/load/nx-backfill.js
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/nx1.js' < tests/load/nx-single-round.js
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # 记录
kubectl -n order-lab exec k6-load -- k6 run -e NX_COUNT=1000 /tmp/nx.js 2>&1 | grep -E 'cache_|failed'
mysql-lab -N -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # 记录
# 预期：Com_select 增量 << 请求数（每 Key 首批合并 + 负缓存吸收）
```

注意：grafana/k6 latest 的 `shared-iterations` 中 `__ITER` 为 per-VU，
脚本实际构成"40 个 Key × 50 并发"的穿透对照，符合实验意图。

**场景 C 完成标志**：不存在 Key 的 DB 回源 ≈ 每 Key 1~2 次（96%+ 压缩），
其余请求全部 neg 命中不打 DB。

## 6. 恢复与清理

```bash
kubectl -n order-lab set env deployment/order-api \
  CACHE_TTL=60s CACHE_COALESCE_ENABLED=true \
  CACHE_INVALIDATE_DELAY_MS=0 CACHE_FILL_DELAY_MS=0
kubectl -n order-lab rollout status deploy/order-api --timeout=60s
kubectl -n order-lab delete pod k6-load
```

库存保持当前水位即可；EXP-09 压测前再按需补充并重新记录基准。

## 7. 预期结果对照表（2026-09-17 实测）

### 场景 A（P1 单热点、TTL 5s、4000 QPS × 3min）

| 组 | wanted | executed | 合并率 | Com_select 增量 | stale | p95 |
|---|---:|---:|---:|---:|---:|---:|
| A1 合并 | 6696 | 140 | 97.9% | +141 | 3036 | ~18ms |
| A2 无合并 | 7796 | 1787 | 0% | +1829 | 50746 | 210ms |

### 场景 B（8:1:1、2000 QPS × 5min、故障 90s + 恢复）

| 实现迭代 | 失败率 | reject | stale |
|---|---:|---:|---:|
| 同步失效（exp08b） | 21.9% | — | — |
| 异步失效（exp08d） | 17.1% | 81559 | 154870 |
| 异步失效 + 滑动续期（exp08e） | 0.28% | 21 | 201760 |

恢复窗口（同一时间线）：恢复后 ~70s 内 stale 兜底（go-redis 池重连，不打 DB）；
此后 Redis 错误率归零，重建回源 ~8/s（合并 + 信号量 12 内有界），
P1 走 stale → miss → hit 收敛；reject 仅 21，无二次拒绝风暴。

### 场景 C（40 keys × 50 并发不存在 SKU）

2000 请求 → 66 次 DB 回源（96.7% 压缩），853 次负缓存命中。

## 本实验要回答的问题

1. 请求合并为什么能把回源压缩到"每过期事件一次"？合并范围为何仅限单进程？
2. 无合并时，为什么 hit 请求的延迟也恶化？回源风暴如何挤占共享资源？
3. 有界等待、独立超时、并发上限三个参数分别保护什么？缺一个会怎样？
4. Redis 故障时，stale 与 503 的分界是什么？滑动续期解决了什么问题？
5. 同步失效在 Redis 故障时如何放大为全链路事故？异步化的代价是什么？
6. 负缓存与请求合并各自对"不存在 Key 穿透"贡献多少压缩？
