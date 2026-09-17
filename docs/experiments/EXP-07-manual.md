# EXP-07 手工操作手册：Cache Aside 与缓存一致性边界

> 目标：复现「无缓存 / 冷缓存 / 热缓存」三轮对照、陈旧读窗口测量、负缓存与失效失败恢复。
> 前置：EXP-06 环境已在 Rancher Desktop k3s 运行（order-lab 命名空间），MySQL 内有 heavy-user-1 数据。
> 预期总耗时：约 70 分钟（含 3 轮各 3 分钟的压测）。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
# 预期：mysql 和 order-api 都 Running 且 ready
```

进入 MySQL 的快捷方式（沿用 EXP-06）：

```bash
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
mysql-lab -e "SELECT COUNT(*) AS orders FROM orders;"
```

进入 Redis 的快捷方式：

```bash
alias redis-lab='kubectl -n order-lab exec deploy/redis -- redis-cli'
redis-lab PING
# 预期：PONG
```

端口转发（本手册所有 curl 都走这个转发）：

```bash
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
```

---

## 第一部分：部署缓存版本（约 10 分钟）

### Step 1：构建镜像并部署

```bash
# 仓库根目录
docker build -t order-api:exp07a .
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab rollout status deploy/order-api
# 预期：Redis Deployment/Service 与 order-api 全部就绪
```

### Step 2：冒烟验证缓存路径

```bash
# 第一次请求：回源并回填缓存
curl -si localhost:18080/api/products/P1 | grep -i x-cache
# 预期：X-Cache: miss

# 第二次请求：命中缓存
curl -si localhost:18080/api/products/P1 | grep -i x-cache
# 预期：X-Cache: hit

curl -si localhost:18080/api/products/P1/stock
# 预期：X-Cache: miss（首次）/ hit（再次）

redis-lab KEYS 'cache:*'
# 预期：cache:product:P1 与 cache:stock:P1 存在，TTL ≈ 60s
```

下单后缓存应被失效：

```bash
curl -si -X POST localhost:18080/api/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: exp07-smoke-1' \
  -d '{"user_id":"u1","sku":"P1","qty":1}' | head -1
redis-lab KEYS 'cache:*'
# 预期：cache:stock:P1 已被删除；下一次 GET stock 为 miss 并回源到新值
```

**第一部分完成标志**：X-Cache 头三种状态（miss/hit/neg）可观察，下单后 stock 键被删除。

---

## 第二部分：三轮缓存对照压测（约 40 分钟）

> 冻结口径（与本实验报告一致）：恒定 2000 QPS × 3 分钟，SKU 热点集合 P1..P100，
> 8:1:1 读写混合。每组开始前记录 DB 读计数与库存水位。

### Step 3：准备压测 Pod 与脚本

```bash
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 900
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=60s

# 在仓库根目录执行：拷入对照脚本与预热脚本
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/cache.js' < tests/load/cache-product.js
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/warmup.js' < tests/load/cache-warmup.js
```

### Step 4：压测前库存准备与对账基准（重要！）

**为什么必须补库存**：压测脚本下单都打 P1（10% × 2000 QPS × 180s ≈ 3.6 万单/轮 × 3 轮 ≈ 11 万单）。
如果 P1 库存不足，写路径会变成纯 409 拒绝，三组对照失真，缓存收益也测不出来。
种子库存只有 1000，必须先补足。

**补多少**：建议 P1 ≥ 200000（3 轮共约 11 万单 + 余量）；P2 场景 B 只用几单，1000 足够，补不补都行。

**第一步：查看当前水位**

```bash
mysql-lab -e "SELECT sku, stock FROM inventory WHERE sku IN ('P1','P2');"
mysql-lab -e "SELECT sku, COUNT(*) AS orders FROM orders WHERE sku IN ('P1','P2') GROUP BY sku;"
```

**第二步：补库存（直接 UPDATE 到目标水位）**

```bash
# 注意：这是实验数据准备，属于人工库存调整；课程主线业务本身不补货。
# 对账基准必须用"补后水位"，不能用种子 1000。
mysql-lab -e "UPDATE inventory SET stock = 200000 WHERE sku = 'P1';"
mysql-lab -e "UPDATE inventory SET stock = 200000 WHERE sku = 'P2';"
```

**第三步：记录对账基准（写进实验报告）**

```bash
mysql-lab -e "SELECT sku, stock FROM inventory WHERE sku IN ('P1','P2');"
mysql-lab -e "SELECT sku, COUNT(*) AS orders FROM orders WHERE sku IN ('P1','P2') GROUP BY sku;"
```

例如：P1 基准 = stock 200000 + orders 20 = 200020，压测后两者之和必须仍是 200020（见 Step 9）。

**第四步：记录 DB 读计数基准**

```bash
mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"
# 记录压测前值 A0（Step 5/6/7 各记录一次，压测后相减 ÷ 180s = DB 读 QPS）
```

### Step 5：A 组——无缓存（对照）

```bash
kubectl -n order-lab set env deployment/order-api CACHE_ENABLED=false
sleep 15   # 等待滚动更新完成
curl -si localhost:18080/api/products/P1 | grep -i x-cache   # 预期：off

mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # 记录压测前值 A0
kubectl -n order-lab exec k6-load -- k6 run /tmp/cache.js
mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # 记录压测后值 A1
```

记录：送达 QPS、p95/p99、错误率、DB 读 QPS = (A1-A0)/180、k6 自定义指标 cache_hits/cache_misses。

### Step 6：B 组——冷缓存

```bash
kubectl -n order-lab set env deployment/order-api CACHE_ENABLED=true
sleep 15
redis-lab FLUSHDB    # 清空缓存，制造冷缓存

mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # B0
kubectl -n order-lab exec k6-load -- k6 run /tmp/cache.js
mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # B1
```

记录：同上（冷缓存下 miss 率应为 100%，DB 读 QPS ≈ 无缓存组）。

### Step 7：C 组——热缓存

```bash
# 先预热：对 P1..P100 商品与库存各请求一次，填充缓存
kubectl -n order-lab exec k6-load -- k6 run /tmp/warmup.js
redis-lab DBSIZE   # 预期 ≈ 200（100 商品 + 100 库存，另可能有少量负缓存键）

mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # C0
kubectl -n order-lab exec k6-load -- k6 run /tmp/cache.js
mysql-lab -e "SHOW GLOBAL STATUS LIKE 'Com_select';"   # C1
```

记录：同上。热缓存下 hit 率预期 > 95%，DB 读 QPS 应显著下降。

### Step 8：指标验证（可选，集群内 Prometheus 查询）

```bash
kubectl run curl-tmp -n monitoring --image=curlimages/curl:latest --restart=Never --command -- sleep 300
kubectl -n monitoring wait --for=condition=Ready pod/curl-tmp --timeout=60s
kubectl -n monitoring exec curl-tmp -- curl -s 'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/query?query=sum(rate(cache_hits_total[1m]))'
kubectl -n monitoring exec curl-tmp -- curl -s 'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/query?query=sum(rate(cache_misses_total[1m]))'
kubectl -n monitoring delete pod curl-tmp
```

### Step 9：压测后对账（缓存不破坏下单不变量）

三轮压测全部完成后，一次性核验 P1/P2 的库存守恒：

```bash
mysql-lab -e "
SELECT sku, stock FROM inventory WHERE sku IN ('P1','P2');
SELECT sku, COUNT(*) AS orders FROM orders WHERE sku IN ('P1','P2') GROUP BY sku;"
```

**守恒公式**：对每个 SKU，`压测后 stock + 压测后 orders = 压测前 stock + 压测前 orders`（Step 4 记录的基准）。
每成功一单扣 1 库存，订单数与库存消耗严格对应；缓存只影响读路径，不参与下单判断，
所以即使缓存陈旧或丢失，该公式也必须成立。若不等，立即停止后续实验并排查（见故障安全边界）。

**第二部分完成标志**：三组对照数据齐全，热缓存组 DB 读负载显著低于无缓存组，
库存对账通过。

---

## 第三部分：陈旧读窗口测量（约 15 分钟）

> 用两个实验开关放大竞态窗口（生产配置必须为 0）：
> `CACHE_INVALIDATE_DELAY_MS` 放大「提交→失效」窗口；`CACHE_FILL_DELAY_MS` 放大「旧值回填」竞态。

### Step 10：场景 A——提交后失效前的自然窗口（放大版）

```bash
# 放大窗口到 300ms
kubectl -n order-lab set env deployment/order-api CACHE_INVALIDATE_DELAY_MS=300
sleep 15

# 预热 P1 库存缓存，记录当前值 N
curl -s localhost:18080/api/products/P1/stock
# 预期：{"sku":"P1","stock":N}

# 后台高频读库存 + 中途下单，观察读到的值何时从 N 变为 N-1
cat > /tmp/stale-a.sh <<'EOF'
#!/bin/bash
BASE=localhost:18080
( for i in $(seq 1 150); do
    v=$(curl -s $BASE/api/products/P1/stock | python3 -c 'import sys,json;print(json.load(sys.stdin)["stock"])')
    echo "$(date +%s.%N) $v"
    sleep 0.01
  done ) > /tmp/stale-a.log &
READER=$!
sleep 0.5
curl -s -X POST $BASE/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp07-stale-a-1' -d '{"user_id":"u1","sku":"P1","qty":1}' > /dev/null
wait $READER
EOF
chmod +x /tmp/stale-a.sh && /tmp/stale-a.sh

python3 - <<'EOF'
# 统计：下单后仍然读到旧值 N 的持续时间（陈旧窗口）
rows = [l.split() for l in open('/tmp/stale-a.log')]
vals = [int(v) for _, v in rows]
n0 = max(vals)
changed = next((t for t, v in rows if int(v) != n0), None)
print(f"old={n0}, 首次读到新值时刻={changed}, 陈旧窗口≈{float(changed)-float(rows[0][0]):.3f}s")
EOF
```

预期：放大 300ms 后，陈旧窗口 ≈ 300~400ms（下单事务提交 + 300ms 延迟 + Redis RTT）。

### Step 11：场景 A 的自然窗口（关闭放大）

```bash
kubectl -n order-lab set env deployment/order-api CACHE_INVALIDATE_DELAY_MS=0
sleep 15
# 重复 Step 10 的脚本（换幂等键 exp07-stale-a-2）
```

预期：陈旧窗口收敛到毫秒级（事务提交到 DEL 完成的 RTT）。

### Step 12：场景 B——旧值回填竞态（Cache Aside 经典竞态）

```bash
# 放大回填窗口到 2s，便于人工操作
kubectl -n order-lab set env deployment/order-api CACHE_FILL_DELAY_MS=2000
sleep 15

# 1) 预热 P2 库存缓存
curl -s localhost:18080/api/products/P2/stock   # 记录值 N

# 2) 发起一个 GET：它读 DB 后 sleep 2s 才写回缓存（旧值在路上）
curl -s localhost:18080/api/products/P2/stock > /dev/null &
GET_PID=$!

# 3) 趁 2s 窗口内下单 P2：DB 扣到 N-1，并 DEL 缓存
sleep 0.3
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp07-stale-b-1' -d '{"user_id":"u1","sku":"P2","qty":1}' > /dev/null
wait $GET_PID

# 4) 此时旧值已回填：缓存被污染为 N（陈旧！）
curl -s localhost:18080/api/products/P2/stock
# 预期：stock 仍为旧值 N —— 陈旧读
mysql-lab -e "SELECT stock FROM inventory WHERE sku='P2';"
# 预期：MySQL 实际为 N-1 —— 下单不变量未被破坏

# 5) 等待 TTL（60s）到期后缓存自动恢复
sleep 65
curl -s localhost:18080/api/products/P2/stock
# 预期：回源到 N-1（陈旧有界，上界 = TTL）
```

**第三部分完成标志**：两个场景的陈旧窗口都测量到，且都不超过事前约定的上界
（正常模式毫秒级；人为放大模式下分别 ≈ 放大值、≈ TTL）。

---

## 第四部分：负缓存验证（约 5 分钟）

### Step 13：不存在对象不穿透数据库

```bash
# 恢复回填开关
kubectl -n order-lab set env deployment/order-api CACHE_FILL_DELAY_MS=0
sleep 15

# 第一次：回源 → 404 + 写负缓存
curl -si localhost:18080/api/products/NX-1 | grep -iE 'x-cache|HTTP'
# 预期：404，X-Cache: miss

# 第二次：负缓存命中 → 404，不打 DB
curl -si localhost:18080/api/products/NX-1 | grep -iE 'x-cache|HTTP'
# 预期：404，X-Cache: neg

redis-lab TTL cache:neg:NX-1
# 预期：≈ 5s
```

### Step 14：对象随后创建不被无限遮蔽

```bash
# 商品创建后（主线用 SQL 模拟），负缓存 TTL 内仍返回 404
mysql-lab -e "INSERT INTO products (sku,name,price_cents) VALUES ('NX-1','New Item',100);"
curl -si localhost:18080/api/products/NX-1 | grep -iE 'x-cache|HTTP'
# 预期：404 neg（负缓存 TTL 5s 内被遮蔽）

# TTL 过期后回源可见
sleep 6
curl -si localhost:18080/api/products/NX-1 | grep -iE 'x-cache|HTTP'
# 预期：200 miss（回源拿到新商品），再请求为 200 hit

# 清理实验数据
mysql-lab -e "DELETE FROM products WHERE sku='NX-1';"
```

**第四部分完成标志**：负缓存命中不打 DB；遮蔽窗口 ≤ negTTL（5s），不被无限遮蔽。

---

## 第五部分：失效失败恢复（约 5 分钟）

### Step 15：Redis 故障时的降级与恢复

```bash
# 预热 P1 库存缓存
curl -s localhost:18080/api/products/P1/stock

# 停掉 Redis
kubectl -n order-lab scale deploy/redis --replicas=0
sleep 5

# 下单仍成功（下单不依赖缓存）；GET 降级回源 DB，返回正确值
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp07-fail-1' -d '{"user_id":"u1","sku":"P1","qty":1}' | head -c 120; echo
curl -si localhost:18080/api/products/P1/stock | grep -iE 'x-cache|HTTP'
# 预期：200 且值正确（回源），X-Cache: miss；Redis GET 失败计数出现在 cache_errors_total{op="get"}

# 观察失效失败指标（Prometheus）
kubectl run curl-tmp -n monitoring --image=curlimages/curl:latest --restart=Never --command -- sleep 300
kubectl -n monitoring wait --for=condition=Ready pod/curl-tmp --timeout=60s
kubectl -n monitoring exec curl-tmp -- curl -s 'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/query?query=sum(rate(cache_errors_total[5m]))+by+(op)'
kubectl -n monitoring exec curl-tmp -- curl -s 'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/query?query=sum(cache_invalidate_total)+by+(result)'
kubectl -n monitoring delete pod curl-tmp
# 预期：op=del 错误计数 > 0，invalidate result=error 或 degraded 有值

# 恢复 Redis
kubectl -n order-lab scale deploy/redis --replicas=1
kubectl -n order-lab rollout status deploy/redis
sleep 5
curl -si localhost:18080/api/products/P1/stock | grep -iE 'x-cache|HTTP'
# 预期：恢复后为 miss（Redis 无持久化，缓存为空）→ 回源 → 再请求 hit，重建正常
```

**第五部分完成标志**：Redis 故障期间下单正确、读降级回源；失效失败有指标可观测；
恢复后缓存自动重建，没有永久陈旧。

---

## Step 16：恢复与清理

```bash
# 确认所有实验开关回到生产配置
kubectl -n order-lab set env deployment/order-api \
  CACHE_ENABLED=true \
  CACHE_INVALIDATE_DELAY_MS=0 \
  CACHE_FILL_DELAY_MS=0
kubectl -n order-lab delete pod k6-load
```

库存保持当前水位即可（EXP-08 压测前再按需补充）；不要随意调整库存——
每次人工调整都必须记录基准，否则后续对账失去依据。

---

## 预期结果对照表（2026-09-17 实测）

### 三轮对照（2000 QPS × 3min，P1..P100）

| 组 | 命中率 | DB 读 QPS | avg | p95 | 备注 |
|---|---:|---:|---:|---:|---|
| A 无缓存 | 0%（off） | 2035 | 25.6ms | 107.0ms | 全部读回源，P1 下单热点竞争 |
| B 冷缓存 | 98.8% | 476 | 8.4ms | 14.1ms | 冷窗口 <2s，冷启动回源风暴 |
| C 热缓存 | 98.8% | 492 | 1.9ms | 7.8ms | 与 EXP-06 基线一致 |

### 陈旧窗口

| 场景 | 窗口上界 | 实测 |
|---|---:|---:|
| A 自然模式 | 毫秒级（提交→DEL RTT） | 30.4ms（含事务耗时与轮询精度） |
| A 放大 300ms | ≈ 300ms + 事务耗时 | 337.9ms |
| B 旧值回填 | TTL = 60s | 污染复现（缓存旧值 199999 vs DB 199998），≤ TTL 自愈 |

## 本实验要回答的问题

1. 为什么只缓存商品/展示库存，不缓存"下单依据"？下单正确性如何保证？
2. 冷缓存组为什么没有收益？命中率从 0 到 >95% 的过程中 DB 负载如何变化？
3. Cache Aside 为什么"先更新 DB 后删缓存"？反过来会怎样？
4. 场景 B 的旧值回填竞态为什么无法用单次 DEL 完全消除？TTL 起什么作用？
5. 负缓存为什么必须短 TTL？TTL 设 1 小时会有什么问题？
6. Redis 故障时下单为什么仍然正确？失效失败靠什么兜底？
