# EXP-14 手工操作手册：应用 Pod/Node 故障与维护中断

> 目标：优雅退出 vs 突然中断对照（A1/A2/A3）、Eviction API + PDB（B）、
> 节点维护/硬故障模拟（C）、全程逐单对账。
> 前置：EXP-13 环境运行中（order-lab），order-api 镜像 exp12b。
> 预期总耗时：约 90 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
kubectl -n order-lab get hpa            # 实验期间删除 HPA（固定副本，避免误扩干扰）
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
```

## 1. 部署（3 副本 + PDB minAvailable 2 + 拓扑分布）

`deploy/k8s/base/all.yaml` 已包含：order-api replicas=3、PDB minAvailable=2、
topologySpreadConstraints（单节点下声明存在、无实际分散效果）。

```bash
kubectl -n order-lab delete hpa order-api
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab rollout status deploy/order-api --timeout=120s
kubectl -n order-lab get pods -l app=order-api -o wide    # 预期 3/3 Running
```

## 2. 场景 A1：优雅退出（约 15 分钟）

> 冻结口径：800 rps × 2min，T+30s 注入。

```bash
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=120s /tmp/fixed-rate.js > /tmp/a1.log 2>&1 &'
sleep 30
kubectl -n order-lab delete pod $(kubectl -n order-lab get pods -l app=order-api -o name | shuf -n1 | cut -d/ -f2)
# 观察（并行）：k6 失败时间窗、Pod 状态迁移、新 Pod 就绪
kubectl -n order-lab get pods -w
# 结束后看结果与对账
kubectl -n order-lab exec k6-load -- tail -20 /tmp/a1.log
```

**完成标志**：失败窗口 ≈ 端点摘除竞态（~2.5s）；0 已提交业务丢失（逐单核验）；
新 Pod Ready 时间戳记录。

## 3. 场景 A2：突然中断（约 15 分钟）

```bash
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=120s /tmp/fixed-rate.js > /tmp/a2.log 2>&1 &'
sleep 30
kubectl -n order-lab delete pod $(kubectl -n order-lab get pods -l app=order-api -o name | shuf -n1 | cut -d/ -f2) --force --grace-period=0
```

**完成标志**：服务级 5xx 率被剩余副本吸收；Pod 恢复时间（删除→Ready）≤ 90s；
对账 1:1:1 成立。

## 4. 场景 A3：容器内崩溃（约 15 分钟）

> distroless 镜像无 shell/kill，`kubectl exec kill -9` 不可用；
> 经宿主机 docker（rancher-desktop context）直杀容器主进程，
> 模拟容器内崩溃（Pod 保留、restartPolicy 重启，绕过 kubelet）。

```bash
C=$(docker ps --format '{{.Names}}' | grep -E '^k8s_order-api_order-api.*_[0-9]$' | head -1)
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=120s /tmp/fixed-rate.js > /tmp/a3b.log 2>&1 &'
sleep 30
docker kill -s KILL $C
kubectl -n order-lab get pods -l app=order-api -w    # RESTARTS 0→1、IP 不变（无重调度）
```

**完成标志**：RESTARTS+1 且 IP 不变（restartPolicy 路径 vs A2 删除重建）；
失败窗口大于 A2（探针摘流延迟）；对账 1:1:1。

## 5. 场景 B：Eviction API + PDB（约 15 分钟）

```bash
# PDB minAvailable=2、3 副本 → 最多允许驱逐 1 个
POD1=$(kubectl -n order-lab get pods -l app=order-api -o name | head -1)
POD2=$(kubectl -n order-lab get pods -l app=order-api -o name | sed -n 2p)
kubectl proxy --port=8001 &
sleep 2
# 第 1 次驱逐：应成功（3→2 ≥ minAvailable 2）
curl -s -X POST localhost:8001/api/v1/namespaces/order-lab/pods/${POD1#pod/}/eviction \
  -H 'Content-Type: application/json' -d '{}' | jq .
# 第 2 次驱逐：应 429 TooManyRequests（2→1 < 2）
curl -s -i -X POST localhost:8001/api/v1/namespaces/order-lab/pods/${POD2#pod/}/eviction \
  -H 'Content-Type: application/json' -d '{}' | head -5
kubectl -n order-lab get pdb order-api -o yaml | grep -A3 status   # disruptionsAllowed
```

**完成标志**：第 1 次驱逐放行（Pod 优雅终止后重建）、第 2 次 429 拒绝；
`status.disruptionsAllowed == 0`（3-1=2 存活 = minAvailable）。

## 6. 场景 C：节点维护与硬故障模拟（约 20 分钟）

> 单节点环境：cordon/drain 后 Pod 无第二调度目标 → 全部 Pending。
> 结果标注「模拟档」，不声称节点级 HA。

```bash
NODE=$(kubectl get nodes -o name | head -1 | cut -d/ -f2)
# C1 节点维护模拟：cordon → drain（--ignore-daemonsets）
kubectl cordon $NODE
kubectl drain $NODE --ignore-daemonsets --delete-emptydir-data --timeout=60s 2>&1 | tail -5
kubectl -n order-lab get pods -l app=order-api          # 预期驱逐后 Pending
# 观察 PDB 是否阻止 drain（disruptionsAllowed 已为 0）
kubectl uncordon $NODE
kubectl -n order-lab rollout status deploy/order-api --timeout=120s

# C2 硬故障模拟：cordon + force delete 全部 order-api Pod
kubectl cordon $NODE
kubectl -n order-lab delete pod -l app=order-api --force --grace-period=0
kubectl -n order-lab get pods -l app=order-api          # 预期全部 Pending（无节点可调度）
kubectl uncordon $NODE
kubectl -n order-lab rollout status deploy/order-api --timeout=120s
```

**完成标志**：C1 记录 drain 行为（驱逐被 PDB 约束、Pending 恢复）；C2 记录
「单节点硬故障 = 应用全停」的事实并标注模拟档。

## 7. 对账模板

```sql
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
SELECT (SELECT COUNT(*) FROM inbox_events), (SELECT COUNT(*) FROM order_notifications);
SELECT COUNT(*) FROM idempotency WHERE order_id = 0;
-- 成功响应账本：k6 201/200 计数 == orders 表计数（窗口内逐单）
SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL 30 MINUTE;
```

## 本实验要回答的问题

1. 优雅退出与突然中断的失败窗口分别由什么决定？恢复时间瓶颈在哪一环？
2. PDB 保护什么、不保护什么？为什么 `--force --grace-period=0` 绕过它？
3. 单节点环境的节点故障实验为什么只能算模拟？多节点档需要什么条件？
4. 故障窗口内客户端收到失败但服务端已提交的订单如何收敛？（幂等键重试）
5. 容器内崩溃（restartPolicy）与 Pod 删除重建的恢复路径差异是什么？
