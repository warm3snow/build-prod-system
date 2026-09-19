# EXP-10 手工操作手册：后置任务削峰、消费并行与反压

> 目标：突发吸收与排空模型、消费并行对照、反压验证、失败可追踪。
> 前置：EXP-09 环境运行中（order-lab），镜像升级为 exp10a。
> 预期总耗时：约 120 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
# 预期：mysql、redis、kafka、order-api、outbox-relay、consumer 均 Running
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
alias kfk='kubectl -n order-lab exec kafka-0 --'
```

## 1. 构建与部署

```bash
# 仓库根目录：三个二进制交叉编译（网络受限时沿用 EXP-09 的 docker cp 注入方式）
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/order-api-exp10a ./cmd/order-api
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/outbox-relay-exp10a ./cmd/outbox-relay
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/consumer-exp10a ./cmd/consumer
# 注入镜像（底座 exp09a）→ order-api:exp10a / outbox-relay:exp10a / consumer:exp10a
# 修改 all.yaml 中三个 image tag 后：
kubectl apply -f deploy/k8s/base/all.yaml
```

## 2. 重建 orders 主题为 3 分区（一次性）

分区只增不减，EXP-09 的 1 分区主题需删除重建。**事件真相在 Outbox，可重放补齐**：

```bash
# 1) 停 consumer 与 relay（防止重建期间位点混乱）
kubectl -n order-lab scale deploy/consumer --replicas=0
kubectl -n order-lab scale deploy/outbox-relay --replicas=0
# 2) 删除主题（若提示 marked for deletion 需等 60s）
kfk /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orders
sleep 10
# 3) 把已消费但 Kafka 侧已无记录的事件重置为 PENDING（重放补齐）
mysql-lab -e "UPDATE outbox_events SET status='PENDING' WHERE status='SENT' AND event_id NOT IN (SELECT event_id FROM inbox_events);"
# 4) 恢复组件（relay 启动时按 KAFKA_PARTITIONS=3 幂等建主题）
kubectl -n order-lab scale deploy/outbox-relay --replicas=1
kubectl -n order-lab scale deploy/consumer --replicas=1
# 5) 等待 1:1:1 恢复（inbox == notif == outbox SENT）
```

**恢复完成标志**：`outbox PENDING=0`、`inbox == notif == SENT 总数`、Kafka lag=0。

## 3. 场景 A：突发吸收与排空模型（约 30 分钟）

> 冻结口径：水位 200000；突发 2000 TPS 下单 × 60s ≈ 12 万事件；消费 ≥ 500 msg/s。

```bash
# Step A0：准备（纯下单脚本 order-only.js 拷入 k6-load；补库存 P1=200000）
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2000 -e DURATION=60s /tmp/orderonly.js > /tmp/a.log 2>&1 &'
# 压测中每 30s 采样：
mysql-lab -N -e "SELECT COUNT(*) FROM outbox_events WHERE status='PENDING';"
# 输入速率（k6 201 计数增量）、消费速率（consumer_processed_total rate）、积压峰值
# T+60s 输入停止 → 记录积压峰值 → 每秒采样直到 PENDING=0，计时排空
```

**完成标志**：峰值积压 ≈ 模型预测（输入 12 万 - 消费×60s）；排空时间可解释；
排空后 1:1:1 对账成立。

## 4. 场景 B：消费并行对照（约 40 分钟）

> 冻结口径：3 分区固定；制造 6 万积压（2000 TPS × 30s）后分别以 1/2/3 实例排空。

```bash
# Step B0：制造积压（先停 consumer，制造后再启动）
kubectl -n order-lab scale deploy/consumer --replicas=0
kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=2000 -e DURATION=30s /tmp/orderonly.js 2>&1 | tail -1'
# Step B1：1 实例 × concurrency 8 → 记录排空吞吐（PENDING 下降斜率）
kubectl -n order-lab scale deploy/consumer --replicas=1
# Step B2：重新制造同量积压 → 2 实例
kubectl -n order-lab scale deploy/consumer --replicas=2
# 观察重平衡（consumer 日志 group join/leave）、吞吐、MySQL CPU
# Step B3：同法 3 实例
```

**完成标志**：三组排空吞吐、MySQL CPU、重平衡耗时齐全；解释「分区分配限制
有效并行度」——3 分区下 3 实例不再有扩展收益（每个实例 ≤ 1 分区）。

## 5. 场景 C：反压验证（约 25 分钟）

> 冻结口径：水位调低至 30000；突发 2000 TPS 持续直至积压触顶。

```bash
kubectl -n order-lab set env deploy/order-api OUTBOX_BACKLOG_LIMIT=30000
kubectl -n order-lab rollout status deploy/order-api --timeout=60s
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2000 -e DURATION=3m /tmp/orderonly.js > /tmp/c.log 2>&1 &'
# 观察：orders_rejected_backlog_total 开始增长的时刻 ≈ PENDING 达 30000 的时刻
# PENDING 封顶（≈ 30000 + 采样滞后），201 计数停滞，503 backlog_limited 出现
# 输入停止后：PENDING 排空 → 水位回落 → 恢复受理（新下单 201）
kubectl -n order-lab set env deploy/order-api OUTBOX_BACKLOG_LIMIT=200000  # 恢复
```

**完成标志**：积压封顶在预算附近；拒绝可观测（503 + 分类计数）；恢复后自动受理；
对账：拒绝数 == 输入 - 受理数，拒绝不产生订单/事件。

## 6. 场景 D：失败可追踪（约 15 分钟）

```bash
# D1：处理中断（force delete consumer）→ 恢复后从位点继续，Inbox 去重
kubectl -n order-lab delete pod -l app=consumer --force --grace-period=0
# D2：毒丸构造 → DEAD → 重放
mysql-lab -e "INSERT INTO outbox_events (event_id,order_id,user_id,sku,payload,status,created_at)
  VALUES (UUID(), 0, 'poison', 'P1', '{bad json', 'PENDING', NOW());"
# 观察 relay_dead_total 增长（attempts 耗尽 → DEAD + 告警）
mysql-lab -e "UPDATE outbox_events SET status='PENDING', attempts=0 WHERE status='DEAD';"  # 重放
# 毒丸再次 DEAD（内容错误不可修复 → 人工处理），不静默跳过
```

**完成标志**：中断恢复 0 副作用重复；毒丸进入 DEAD 且告警可见；重放路径可执行。

## 7. 对账模板

```sql
-- 1:1:1 全链（沿用 EXP-09）
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
SELECT (SELECT COUNT(*) FROM inbox_events) inbox,
       (SELECT COUNT(*) FROM order_notifications) notif;
SELECT order_id, COUNT(*) c FROM order_notifications GROUP BY order_id HAVING c != 1;
-- 反压窗口对账：拒绝数 == 输入 - 受理数（k6 计数 vs 201 计数 vs backlog_limited 计数）
```

## 本实验要回答的问题

1. 突发积压的峰值与排空时间如何用「输入-消费」模型解释？实测偏差来自哪？
2. watermark 顺序提交如何保证并发下「位点不跳过未完成任务」？
3. 有界在途队列为什么是反压阀？下游变慢时链路如何收缩？
4. 分区分配如何限制有效并行度？3 分区下 3 实例是否还有收益？
5. 高水位反压为什么发生在下单入口而不是 Relay？拒绝的语义与恢复路径是什么？
6. 磁盘保留期预算与 Outbox 重放如何配合保证「积压不能无限增长且不丢真相」？
