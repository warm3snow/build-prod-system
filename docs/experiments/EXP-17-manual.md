# EXP-17 手工操作手册：Kafka 高可用与事件完整性

> 目标：KRaft 3 集群部署（G1 基线）、Leader Broker 强杀（G2）、双杀/多数派
> 失守（G2b+G3）、事件 ID 三段对账（G4）。
> 前置：EXP-16 环境（InnoDB Cluster + Router 运行中）；镜像 exp17a 已构建。
> 预期总耗时：约 100 分钟。

## 0. 关键引导约束（先读，都是实测踩坑）

```text
1. podManagementPolicy: Parallel——OrderedReady 下首成员等待 quorum 无法
   Ready，STS 永不创建后续成员（死锁）。
2. headless Service publishNotReadyAddresses: true——quorum 成员在就绪前
   必须互相解析（DNS 默认只发布 Ready 端点，鸡生蛋）。
3. 同名 Service 禁止多清单定义：base all.yaml 的旧 kafka-hs/kafka 与
   ha 清单同名，apply 相互覆写 selector → 新集群 DNS 全 NXDOMAIN。
   旧定义已从 all.yaml 移除。
```

## 1. 构建与部署

```bash
# 1.1 构建三镜像（kafka.go/config 有变更）：
for c in order-api outbox-relay consumer; do
  GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/$c-exp17a ./cmd/$c
  CID=$(docker create --name t-$c order-api:exp12b) && docker cp /tmp/$c-exp17a $CID:/order-api \
    && docker commit $CID "${c}:exp17a" >/dev/null && docker rm $CID >/dev/null
done

# 1.2 部署 KRaft 集群（脏数据需先清 PVC + local-path 残留）：
kubectl -n order-lab delete sts kafka-i 2>/dev/null
kubectl -n order-lab delete pvc -l app=kafka-i 2>/dev/null
docker run --rm -v /:/host alpine sh -c 'for d in /host/var/lib/rancher/k3s/storage/*kafka-i*; do [ -d "$d" ] && rm -rf "$d"; done; true'
kubectl apply -f deploy/k8s/ha/kafka-kraft.yaml
# 等 3/3 Running（~90s；首次 format）
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status | grep -E 'LeaderId|CurrentVoters'

# 1.3 切换链路（all.yaml 已含：KAFKA_BROKERS=三成员、KAFKA_RF=3、
#     KAFKA_MIN_ISR=2、镜像 exp17a；旧 kafka STS replicas=0）：
kubectl -n order-lab scale deploy/outbox-relay deploy/consumer --replicas=0
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab rollout status deploy/outbox-relay deploy/consumer --timeout=180s
# 1.4 验证 topic（relay 启动时显式创建）：
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders
# 预期：PartitionCount 3 / RF 3 / min.insync.replicas=2 / Isr 1,2,3
# 1.5 预热缓存（TTL 600s 过期后冷读会拖垮 HTTP 层，属 EXP-16 已知边界）：
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e SKU_COUNT=10000 /tmp/warmup.js > /tmp/warm.log 2>&1 &'
```

## 2. 场景 G1：事件链路基线（600 rps × 3min）

```bash
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=600 -e DURATION=180s /tmp/fixed-rate.js > /tmp/g1.log 2>&1 &'
# 结束后测延迟分布（投递=orders→outbox.sent_at；完成=orders→notif.created_at）：
#   见第 6 节 SQL；预期完成 p99 ≤ 5s（实测 1.34s）；1:1:1；lag=0
```

## 3. 场景 G2：Leader Broker 强杀（约 8 分钟）

```bash
# 找 orders-0 Leader（Partition: 0 行的 Leader: N → kafka-i-(N-1)）：
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders | grep 'Partition: 0'
B0=<orders 计数>
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=600 -e DURATION=300s /tmp/fixed-rate.js > /tmp/g2.log 2>&1 &'
sleep 60
kubectl -n order-lab delete pod kafka-i-<leader> --force --grace-period=0
# 每 10-12s 采样：分区 Leader/ISR、PENDING、orders 增速、quorum LeaderId
# 预期：T+20s 内 Leader 切换 + 被杀成员回 ISR；PENDING ≤15；对账 1:1:1
```

## 4. 场景 G2b+G3：双杀（多数派失守，约 8 分钟）

```bash
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=600 -e DURATION=300s /tmp/fixed-rate.js > /tmp/g2b.log 2>&1 &'
sleep 45
kubectl -n order-lab delete pod kafka-i-1 kafka-i-2 --force --grace-period=0
# 观察明确失败证据（核心验收点）：
kubectl -n order-lab logs deploy/outbox-relay --since=5m | grep 'Not Enough Replicas' | head -2
kubectl -n order-lab logs deploy/consumer --since=5m | grep -E 'Coordinator' | head -2
# 采样：PENDING 瞬时积压（~数百）→ STS 重建后排空；orders 增速不变
# 预期：明确拒写（非假成功）+ 恢复自动排空 + 终局 1:1:1
```

## 5. 场景 G4：事件 ID 三段抽查

```sql
-- Outbox payload 里的 event_id == Inbox 主键
SELECT ob.event_id, ob.payload->>'$.event_id' AS payload_id, ib.event_id AS inbox_id
FROM outbox_events ob JOIN inbox_events ib ON ib.event_id = ob.event_id
ORDER BY ob.id DESC LIMIT 3;
```

## 6. 延迟分布 SQL（G1/G2 复用；MySQL 无 PERCENTILE_CONT，用 offset 法）

```sql
-- n / avg / p99 / max：投递延迟（Outbox→Kafka 确认）
SELECT COUNT(*), ROUND(AVG(TIMESTAMPDIFF(MICROSECOND, o.created_at, ob.sent_at)/1e6),2)
FROM outbox_events ob JOIN orders o ON o.id=ob.order_id
WHERE ob.created_at > NOW() - INTERVAL 12 MINUTE;
-- p99：先取 COUNT(N)，OFFSET = FLOOR(N/100)，子查询 ORDER BY d DESC LIMIT 1 OFFSET n
-- 完成延迟同法（orders→order_notifications.created_at）
```

## 7. 对账模板

```sql
SELECT (SELECT COUNT(*) FROM orders) orders,
       (SELECT COUNT(*) FROM outbox_events WHERE status='SENT') sent,
       (SELECT COUNT(*) FROM outbox_events WHERE status='PENDING') pending,
       (SELECT COUNT(*) FROM inbox_events) inbox,
       (SELECT COUNT(*) FROM order_notifications) notif,
       (SELECT COUNT(*) FROM idempotency WHERE order_id=0) orphan;
```

```bash
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-consumer   # lag=0
```

## 本实验要回答的问题

1. acks=all + min.isr=2 的「保证」与「不保证」各是什么？双杀时的
   NotEnoughReplicas 证据链是什么？
2. KRaft on K8s 引导为什么必须 Parallel + publishNotReadyAddresses？
3. 同名 Service 覆写如何让一个健康集群的 DNS 全部失效？
4. controller 多数派失守时哪些能力停摆？下单为什么完全不受影响？
5. 「事件真相在 Outbox」的架构决定在集群替换场景兑现了什么价值？
