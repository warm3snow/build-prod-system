# EXP-09 手工操作手册：Outbox、消费幂等与可靠事件

> 目标：复现正常投递链路、三个崩溃点测试、Kafka 不可用与恢复，完成逐事件对账。
> 前置：EXP-08 环境在 Rancher Desktop k3s 运行（order-lab 命名空间），镜像 exp09a。
> 预期总耗时：约 90 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
# 预期：mysql、redis、kafka、order-api、outbox-relay、consumer 均 Running
kubectl -n order-lab get deploy order-api outbox-relay consumer \
  -o jsonpath='{range .items[*]}{.metadata.name}={.spec.template.spec.containers[0].image}{"\n"}{end}'
# 预期：order-api=order-api:exp09a、outbox-relay=outbox-relay:exp09a、consumer=consumer:exp09a
```

快捷方式：

```bash
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
alias kafka-lab='kubectl -n order-lab exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh'
alias kfk='kubectl -n order-lab exec kafka-0 --'
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
```

Kafka 首次部署注意：apache/kafka:3.9.0 镜像若本地没有需要拉取；
镜像不可用时的替代见 EXP-08 的离线镜像处理思路（本实验代码镜像仍需离线构建）。

## 1. 构建说明（网络受限时）

三个 Go 二进制共用 distroless 底座，离线构建方式（沿用 EXP-08）：

```bash
# 仓库根目录：交叉编译三个二进制
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/order-api ./cmd/order-api
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/outbox-relay ./cmd/outbox-relay
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/consumer ./cmd/consumer

# 复用 exp08e 底座分别注入（注意 docker create 的名字不能冲突）
CID=$(docker create --name tmp1 order-api:exp08e)
docker cp /tmp/order-api $CID:/order-api
docker commit $CID order-api:exp09a && docker rm $CID

CID=$(docker create --name tmp2 order-api:exp08e)
docker cp /tmp/outbox-relay $CID:/order-api
docker commit $CID outbox-relay:exp09a && docker rm $CID

CID=$(docker create --name tmp3 order-api:exp08e)
docker cp /tmp/consumer $CID:/order-api
docker commit $CID consumer:exp09a && docker rm $CID
```

网络可用时：`docker build -t order-api:exp09a .` 后按同样方式生成另两个镜像。

部署：

```bash
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab rollout status deploy/order-api deploy/outbox-relay deploy/consumer --timeout=120s
kubectl -n order-lab get pods   # kafka 首次启动约 30-60s（format + 启动）
```

## 2. 冒烟（10 分钟）

```bash
# 1) 事件链路：下单 → 事件 → 通知
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp09-smoke-1' -H 'X-Request-Id: smoke-rid-1' \
  -d '{"user_id":"u1","sku":"P1","qty":1}' | tee /tmp/o.json   # 201，记下 id

mysql-lab -e "SELECT id,event_id,status,request_id FROM outbox_events ORDER BY id DESC LIMIT 1;"
# 预期：status=SENT（几秒内）、request_id=smoke-rid-1

mysql-lab -e "SELECT * FROM order_notifications ORDER BY id DESC LIMIT 1;"
# 预期：order_id 与下单一致，message='order accepted'

# 2) 重复投递吸收（模拟）：同一事件再走一遍消费事务不产生第二条通知
#    （由场景 B 的崩溃注入自然覆盖；此处可选直插 inbox 验证唯一键）
# 3) Kafka 侧核对
kfk /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 1 --timeout-ms 8000
```

**冒烟完成标志**：一次下单后 outbox SENT、inbox 1 行、notification 1 行，Kafka 有消息。

## 3. 场景 A：正常投递链路（约 15 分钟）

> 冻结口径：8:1:1 混合 2000 QPS × 5min（cache-product.js，下单 ~200 TPS）。

```bash
# Step A0：准备
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 2400
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=90s
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/cache.js' < tests/load/cache-product.js
mysql-lab -e "UPDATE inventory SET stock=200000 WHERE sku='P1';"
mysql-lab -N -e "SELECT COUNT(*) FROM orders;"          # 记录 O0
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events;"   # 记录 E0
mysql-lab -N -e "SELECT COUNT(*) FROM inbox_events;"    # 记录 I0
mysql-lab -N -e "SELECT COUNT(*) FROM order_notifications;"  # 记录 N0

# Step A1：压测 5min
kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=2000 -e DURATION=5m -e SKU_COUNT=100 /tmp/cache.js 2>&1 | tee /tmp/a.log | tail -1'

# Step A2：等积压排空（backlog 归零）
# Prometheus: outbox_backlog == 0；kafka_consumer_lag == 0
sleep 60

# Step A3：对账
mysql-lab -N -e "SELECT COUNT(*) FROM orders;"          # O1；下单增量 = O1-O0
mysql-lab -N -e "SELECT COUNT(*),status FROM outbox_events GROUP BY status;"  # PENDING=0，SENT=E1-E0
mysql-lab -N -e "SELECT COUNT(*) FROM inbox_events;"    # I1；I1-I0 == SENT 增量
mysql-lab -N -e "SELECT COUNT(*) FROM order_notifications;"  # N1；N1-N0 == I 增量
# 唯一性断言（必须全部返回空）
mysql-lab -e "SELECT order_id,COUNT(*) c FROM outbox_events GROUP BY order_id HAVING c!=1;
              SELECT order_id,COUNT(*) c FROM order_notifications GROUP BY order_id HAVING c!=1;
              SELECT event_id,COUNT(*) c FROM inbox_events GROUP BY event_id HAVING c!=1;"
```

**场景 A 完成标志**：对账 1:1:1（订单增量==事件增量==通知增量），唯一性断言为空；
完成延迟 P99（`order_post_process_latency_seconds`）≤ 5s；http_req_failed < 0.5%。

## 4. 场景 B：三个崩溃点（约 35 分钟）

> 统一口径：下单流量持续（沿用 cache-product.js，2000 QPS），每次注入后恢复并核对。
> 崩溃注入注意：`kill -9`（kubectl exec 到容器内 kill）模拟突然中断；
> `kubectl delete pod` 走 SIGTERM 优雅退出，两者结论分开记录。

### Step B0：准备

```bash
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events;"    # 基线 E0
mysql-lab -N -e "SELECT COUNT(*) FROM inbox_events;"     # 基线 I0
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2000 -e DURATION=20m -e SKU_COUNT=100 /tmp/cache.js > /tmp/b.log 2>&1 &'
```

### Step B1：DB 提交后 Relay 崩溃（积压 → 重启 → 排空，无遗漏）

```bash
# 压测第 1 分钟：停 relay（模拟崩溃，outbox 继续积累）
kubectl -n order-lab scale deploy/outbox-relay --replicas=0
sleep 60
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events WHERE status='PENDING';"   # 积压增长
# 重启 relay，观察排空
kubectl -n order-lab scale deploy/outbox-relay --replicas=1
sleep 90
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events WHERE status='PENDING';"   # 预期 0
# 无遗漏断言：重启后 SENT 增量 == 停 relay 期间订单增量
```

**B1 完成标志**：积压排空，SENT 增量 == 期间新订单数（0 遗漏）。

### Step B2：消息已发送但未标记（SIGKILL relay 制造重复投递）

```bash
# 连续 3 次突然中断（随机时刻 kill，覆盖「确认后、标记前」窗口）
for i in 1 2 3; do
  kubectl -n order-lab exec deploy/outbox-relay -- sh -c 'kill -9 1' || true
  sleep 10   # 等 pod 自动重建就绪
done
sleep 60
# 重复投递证据：consumer dup 计数
# Prometheus: sum(consumer_processed_total{result="dup"}) > 0
# 副作用唯一性断言
mysql-lab -e "SELECT order_id,COUNT(*) c FROM order_notifications GROUP BY order_id HAVING c!=1;"   # 空
```

**B2 完成标志**：dup 命中 > 0（重复投递确实发生），通知仍每订单恰一条。

### Step B3：消费事务已提交但位点未提交（SIGKILL consumer）

```bash
for i in 1 2 3; do
  kubectl -n order-lab exec deploy/consumer -- sh -c 'kill -9 1' || true
  sleep 10
done
sleep 60
# Prometheus: dup 继续增长；lag 收敛为 0
# 断言同上：order_notifications 每 order 唯一；inbox 每 event 唯一
```

**B3 完成标志**：位点回退重读产生 dup，副作用仍唯一。

### Step B4：收尾对账

```bash
kubectl -n order-lab exec k6-load -- sh -c 'grep -E "http_req_failed" /tmp/b.log'
mysql-lab -N -e "SELECT COUNT(*),status FROM outbox_events GROUP BY status;"   # PENDING=0
mysql-lab -e "SELECT order_id,COUNT(*) c FROM outbox_events GROUP BY order_id HAVING c!=1;
              SELECT order_id,COUNT(*) c FROM order_notifications GROUP BY order_id HAVING c!=1;"
```

## 5. 场景 C：Kafka 不可用及恢复（约 20 分钟）

> 冻结口径：压测 60s 后 scale kafka→0，故障 90s 后恢复；故障期间下单必须持续成功。

```bash
mysql-lab -N -e "SELECT COUNT(*) FROM orders;"               # O0
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2000 -e DURATION=5m -e SKU_COUNT=100 /tmp/cache.js > /tmp/c.log 2>&1 &'
sleep 60
# T+60s：停 Kafka（模拟 Broker 故障）
kubectl -n order-lab scale statefulset/kafka --replicas=0
# T+90s：故障期抽查——下单必须仍成功，事件积压增长
curl -s -X POST localhost:18080/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp09-c-fault-1' -d '{"user_id":"u1","sku":"P1","qty":1}' -o /dev/null -w '%{http_code}\n'   # 201
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events WHERE status='PENDING';"   # 持续增长
# 观察告警：OutboxRelayNotDelivering（relay 5m 零投递且积压 > 0）
# T+150s：恢复 Kafka
kubectl -n order-lab scale statefulset/kafka --replicas=1
kubectl -n order-lab rollout status statefulset/kafka --timeout=120s
# 恢复期：观察 outbox_backlog 单调下降至 0、relay_sent 速率、kafka lag 收敛
```

**场景 C 完成标志**：故障期下单错误率不恶化（k6 http_req_failed 保持 <0.5%，
且与故障前一致）；积压排空；恢复后对账 1:1:1 成立。

## 6. 恢复与清理

```bash
kubectl -n order-lab delete pod k6-load
# 压测后库存水位保持，EXP-10 前再补
# DEAD 事件重放（如需）：
mysql-lab -e "UPDATE outbox_events SET status='PENDING', attempts=0, last_error='' WHERE status='DEAD';"
```

## 7. 对账模板（贯穿本实验）

```sql
-- 1. 订单 → 事件：每订单恰一个事件
SELECT COUNT(*) orders FROM orders;
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
-- 2. 事件 → 消费：SENT 事件全部被消费（去重后等数）
SELECT COUNT(*) FROM inbox_events;
-- 3. 消费 → 副作用：每事件恰一条通知、每订单恰一条通知
SELECT COUNT(*) FROM order_notifications;
-- 4. 唯一性断言（结果必须为空）
SELECT order_id, COUNT(*) c FROM outbox_events GROUP BY order_id HAVING c != 1;
SELECT event_id, COUNT(*) c FROM inbox_events GROUP BY event_id HAVING c != 1;
SELECT order_id, COUNT(*) c FROM order_notifications GROUP BY order_id HAVING c != 1;
-- 5. 客户端账本 vs DB（k6 201/200 计数 ≈ orders 增量，跨轮重放除外）
```

## 本实验要回答的问题

1. Outbox 为什么必须与订单同事务？异步写事件的丢失窗口在哪里？
2. Relay at-least-once 的重复投递在哪些时刻产生？Inbox 如何把副作用收敛为恰好一次？
3. 位点提交为什么在业务事务之后？反过来的崩溃窗口会丢什么？
4. Kafka 不可用时下单为什么不受影响？积压如何观测、如何防止无限增长？
5. 保序消费的代价是什么？失败消息为什么能阻塞后续消息？
6. DEAD 事件是什么语义？如何重放而不产生重复副作用？
