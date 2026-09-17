# EXP-06 手工操作手册：MySQL 查询、索引与连接预算

> 目标：复现「深分页 vs 游标分页」与「连接池 10/25/50 对照」两组实验。
> 前置：EXP-02 环境已在 Rancher Desktop k3s 运行（order-lab 命名空间）。
> 预期总耗时：约 40 分钟（含 3 轮各 3 分钟的压测）。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
# 预期：mysql 和 order-api 都 Running 且 ready
```

进入 MySQL 的快捷方式（后续反复使用）：

```bash
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
mysql-lab -e "SELECT COUNT(*) AS orders FROM orders;"
```

---

## 第一部分：慢查询与索引实验（约 15 分钟）

### Step 1：造重度用户数据（20 万订单）

```bash
mysql-lab -e "
SET SESSION cte_max_recursion_depth=300000;
INSERT INTO orders (id, user_id, sku, qty, unit_price_cents, status, created_at)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<200000)
SELECT 1000000000+n, 'heavy-user-1', CONCAT('P', 1+(n%10000)), 1, 9900, 'CREATED',
       NOW(3) - INTERVAL n SECOND
FROM seq;
SELECT COUNT(*) AS heavy_orders FROM orders WHERE user_id='heavy-user-1';"
# 预期：heavy_orders = 200000
```

注意：ID 用 `1000000000+n` 段，避免与已有订单的自增 ID 冲突。

### Step 2：查看当前索引结构

```bash
mysql-lab -e "SHOW INDEX FROM orders;"
# 预期看到：idx_orders_user_created (user_id, created_at) 联合索引
```

### Step 3：有索引时的执行计划（深分页）

```bash
mysql-lab -e "
EXPLAIN SELECT id, sku, status, created_at
FROM orders WHERE user_id='heavy-user-1'
ORDER BY created_at DESC LIMIT 10 OFFSET 150000;"
```

**预期输出**：

```
type: ref
key:  idx_orders_user_created
rows: ~220000        ← 必须扫描并丢弃前 15 万行
Extra: Backward index scan
```

### Step 4：有索引时的实测耗时（profiling）

```bash
mysql-lab -e "
SET profiling=1;
SELECT id, sku, status, created_at FROM orders
WHERE user_id='heavy-user-1' ORDER BY created_at DESC LIMIT 10 OFFSET 150000;
SHOW PROFILES;"
```

**预期**：Duration ≈ **80ms**。

### Step 5：删除索引，观察全表扫描

```bash
mysql-lab -e "ALTER TABLE orders DROP INDEX idx_orders_user_created;"

mysql-lab -e "EXPLAIN SELECT id, sku, status, created_at FROM orders
WHERE user_id='heavy-user-1' ORDER BY created_at DESC LIMIT 10 OFFSET 150000\G"
# 预期：type: ALL、rows: ~450000、Extra: Using where; Using filesort

mysql-lab -e "
SET profiling=1;
SELECT id, sku, status, created_at FROM orders
WHERE user_id='heavy-user-1' ORDER BY created_at DESC LIMIT 10 OFFSET 150000;
SHOW PROFILES;"
```

**预期**：Duration ≈ **130ms**（全表扫描 + 内存排序）。

### Step 6：重建索引（记住必须恢复！）

```bash
mysql-lab -e "ALTER TABLE orders ADD INDEX idx_orders_user_created (user_id, created_at);"
```

### Step 7：游标分页——深分页的正确解法

先取第一页最后一条的 `(created_at, id)` 作为锚点，例如：

```bash
mysql-lab -e "
SELECT id, created_at FROM orders
WHERE user_id='heavy-user-1' ORDER BY created_at DESC, id DESC LIMIT 10;"
# 记下最后一行的 created_at 和 id，例如：2026-09-13 17:52:29.674 / 1000150010
```

用锚点查下一页：

```bash
mysql-lab -e "
SET profiling=1;
SELECT id, sku, status, created_at FROM orders
WHERE user_id='heavy-user-1'
  AND (created_at < '2026-09-13 17:52:29.674'
       OR (created_at = '2026-09-13 17:52:29.674' AND id < 1000150010))
ORDER BY created_at DESC, id DESC LIMIT 10;
SHOW PROFILES;"
```

**预期**：Duration ≈ **0.4ms** —— 比 OFFSET 深分页快约 200 倍。

### Step 8：API 层验证（游标分页接口）

```bash
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
curl -s 'localhost:18080/api/orders?user_id=heavy-user-1&limit=3' | python3 -m json.tool
# 记下响应里的 next_cursor_created_at 和 next_cursor_id

curl -s 'localhost:18080/api/orders?user_id=heavy-user-1&limit=3&cursor_created_at=<上一步的值>&cursor_id=<上一步的值>'
# 预期：返回下一页 3 条，has_more 正确
```

**第一部分的结论**：OFFSET 深分页慢的本质是「跳过前 N 行」，索引帮不上；
游标分页以 (created_at, id) 为锚点，索引直接定位。

---

## 第二部分：连接池对照实验（约 25 分钟）

### Step 9：准备压测 Pod 与脚本

```bash
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 900
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=60s

# 把仓库里的恒定负载脚本拷进 Pod（在仓库根目录执行）
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/load.js' < tests/load/constant-2000.js
```

脚本要点：constant-arrival-rate 2000 QPS，8:1:1 读写混合，3 分钟。

### Step 10：A 组——10 连接

```bash
kubectl -n order-lab set env deployment/order-api DB_MAX_OPEN_CONNS=10 DB_MAX_IDLE_CONNS=5
sleep 15   # 等待滚动更新完成

kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js
```

压测期间另开终端观察：

```bash
kubectl top pods -n order-lab
mysql-lab -e "SHOW STATUS LIKE 'Threads_connected';"
```

记录：送达 QPS、p90/p95、错误率、Threads_connected。

### Step 11：B 组——25 连接

```bash
kubectl -n order-lab set env deployment/order-api DB_MAX_OPEN_CONNS=25 DB_MAX_IDLE_CONNS=10
sleep 15
kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js
```

### Step 12：C 组——50 连接

```bash
kubectl -n order-lab set env deployment/order-api DB_MAX_OPEN_CONNS=50 DB_MAX_IDLE_CONNS=25
sleep 15
kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js
```

### Step 13：查看连接池等待指标（可选，验证池内排队）

在集群内用临时 curl Pod 查 Prometheus：

```bash
kubectl run curl-tmp -n monitoring --image=curlimages/curl:latest --restart=Never --command -- sleep 300
kubectl -n monitoring wait --for=condition=Ready pod/curl-tmp --timeout=60s
kubectl -n monitoring exec curl-tmp -- curl -s 'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/query?query=db_pool_wait_count_total'
kubectl -n monitoring delete pod curl-tmp
```

`db_pool_wait_count_total` > 0 说明发生过「连接池耗尽等待」。

### Step 14：恢复最终配置并清理

```bash
kubectl -n order-lab set env deployment/order-api DB_MAX_OPEN_CONNS=25 DB_MAX_IDLE_CONNS=10
kubectl -n order-lab delete pod k6-load
```

---

## 预期结果对照表

### 查询实验

| 方案 | 耗时 | 执行计划 |
|---|---:|---|
| 深分页 OFFSET 150000（有索引） | ~80ms | ref，扫 22 万行 |
| 深分页 OFFSET 150000（无索引） | ~130ms | ALL 全表扫 + filesort |
| 游标分页 | ~0.4ms | 索引定位 ≤10 行 |

### 连接池实验（2000 QPS）

| MaxOpenConns | 送达 QPS | 错误率 | p90 | p95 | 观察 |
|---|---:|---:|---:|---:|---|
| 10 | ~1999 | 0% | ~11ms | ~34ms | 写突发排队，p95 抬升 |
| 25 | ~2000 | 0% | ~2ms | ~8ms | 甜点位 |
| 50 | ~2000 | 0% | ~3ms | ~12ms | 无额外收益 |

## 本实验要回答的问题

1. OFFSET 150000 为什么慢？索引为什么帮不上忙？
2. 游标分页的锚点为什么是 (created_at, id) 双字段而不是单字段？
3. 为什么连接池 10 会排队、50 没有收益？
4. 如果 order-api 扩容到 4 副本，MySQL 连接预算怎么算？
   （答案：4 × 25 = 100，须低于 MySQL max_connections 的 60% 水位）
