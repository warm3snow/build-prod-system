# EXP-13 手工操作手册：固定副本容量与 HPA 动态行为

> 目标：固定副本容量曲线（E1）、HPA 动态时序（E2）、缩容稳定性（E4）、Pending（E5）。
> 前置：EXP-12 环境运行中（order-lab），order-api 镜像 exp12b。
> 预期总耗时：约 90 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
# 关键预算事实（冻结）：
mysql-lab -N -e "SELECT @@max_connections;"   # 151 → maxReplicas ≤ 5（25N+20 ≤ 151）
kubectl get nodes -o jsonpath='{.items[0].status.allocatable.cpu}'  # 6 核
```

## 1. 部署（HPA + PDB + requests 校准）

`deploy/k8s/base/all.yaml` 已包含：HPA（CPU 70%，min 1 / max 5，缩容稳定 60s +
50%/60s）、PDB（minAvailable 1）、order-api requests=1 核。

```bash
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab delete hpa order-api   # E1 固定副本阶段先删除 HPA
kubectl -n order-lab scale deploy/order-api --replicas=1
```

## 2. 场景 E1：固定副本容量曲线（约 50 分钟）

> 冻结口径：`tests/load/fixed-rate.js`，constant-arrival-rate，60s/档。

```bash
# 1 副本混合（8:1:1）
for R in 1000 1500 2000; do
  kubectl -n order-lab exec k6-load -- k6 run -e RATE=$R -e DURATION=60s /tmp/fixed-rate.js | grep -E 'p\(99\)|failed'
done
# 1 副本纯读
kubectl -n order-lab exec k6-load -- k6 run -e RATE=1500 -e DURATION=60s -e READ_ONLY=1 /tmp/fixed-rate.js
# 2 副本混合 + 纯读
kubectl -n order-lab scale deploy/order-api --replicas=2
for R in 2000 3000; do k6 run ...; done        # 预期：反而劣于 1 副本（P1 行锁）
kubectl -n order-lab exec k6-load -- k6 run -e RATE=3000 -e DURATION=60s -e READ_ONLY=1 /tmp/fixed-rate.js
# 压测中抓锁队列深度（关键证据）
mysql-lab -N -e "SHOW STATUS LIKE 'Innodb_row_lock_current_waits';"
```

**完成标志**：混合/纯读两条曲线 + 行锁队列证据（预期 2 副本 2000 rps 时
current_waits ≈ 40）；「读线性、写负收益」结论成立。

## 3. 场景 E2：HPA 动态行为（约 20 分钟）

```bash
kubectl apply -f deploy/k8s/base/all.yaml     # 重建 HPA
kubectl -n order-lab scale deploy/order-api --replicas=1
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=2400 -e DURATION=5m /tmp/fixed-rate.js > /tmp/e2.log 2>&1 &'
# 每 30s 记录：kubectl -n order-lab get hpa order-api；events 抓 SuccessfulRescale 时间戳
kubectl -n order-lab get events -w | grep -E 'SuccessfulRescale|ScalingReplicaSet'
# 停载后记录缩容时序（60s 稳定窗口 + 50%/60s 策略）
```

**完成标志**：扩容触发时间（指标可用 ~15s、rescale 一步 1→5）、Pod 全部 Ready 耗时、
缩容 5→2→1 时间戳齐全；5 副本下业务吞吐与 1 副本对照（预期更差——CPU 误扩证据）。

## 4. 场景 E4：缩容稳定性（约 10 分钟）

```bash
kubectl -n order-lab delete hpa order-api
kubectl -n order-lab scale deploy/order-api --replicas=3
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=90s /tmp/fixed-rate.js > /tmp/e4.log 2>&1 &'
sleep 45
kubectl -n order-lab scale deploy/order-api --replicas=1    # 负载中缩容
# 预期：~97% 通过，失败集中在端点摘除窗口（~2.5s）；对账确认 0 订单丢失
mysql-lab -N -e "SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL 10 MINUTE;"
```

## 5. 场景 E5：Pending（约 10 分钟）

```bash
kubectl -n order-lab scale deploy/order-api --replicas=8    # requests=1 核 × 8 > 节点余量
kubectl -n order-lab get pods -l app=order-api              # 预期若干 Pending
kubectl -n order-lab get events | grep FailedScheduling     # Insufficient cpu
kubectl -n order-lab scale deploy/order-api --replicas=1    # 恢复
```

## 6. 对账模板

```sql
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
SELECT (SELECT COUNT(*) FROM inbox_events), (SELECT COUNT(*) FROM order_notifications);
SELECT COUNT(*) FROM idempotency WHERE order_id = 0;
```

## 本实验要回答的问题

1. 读路径为什么线性扩展？写路径为什么扩容无效甚至有害？
2. 限流预算随副本放大后，过载去了哪里？（锁队列深度证据）
3. CPU HPA 在 DB 瓶颈下为什么误扩？什么指标更合适？
4. maxReplicas=5 的连接预算推导？requests 从 100m 校准到 1 核改变了什么？
5. 缩容的失败窗口从哪来？为什么「零失败缩容」不是目标？
6. Pending 的调度语义（Insufficient cpu、无抢占）说明什么？
