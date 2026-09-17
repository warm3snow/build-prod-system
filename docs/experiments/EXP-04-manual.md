# EXP-04 手工操作手册：最小可观测性与容量 Baseline

> 目标：接入 Prometheus 指标与告警，用 k6 建立容量基线并定位首个瓶颈。
> 前置：EXP-03 完成；已有 kube-prometheus-stack（monitoring 命名空间）；安装 k6。
> 预期耗时：约 40 分钟（含两轮压测各约 6 分钟）。

## Step 0：准备

```bash
brew install k6
k6 version
kubectl get pods -n monitoring
# 预期：prometheus / grafana / alertmanager 等组件 Running（复用已有监控栈）
```

## Step 1：确认指标代码与部署

指标已在代码中（`internal/api/metrics.go` 的 RED + 业务指标），
部署清单含 metrics 端口、ServiceMonitor 与告警规则：

```bash
kubectl get servicemonitor -n order-lab
# 预期：order-api（labels 带 release=kube-prometheus-stack）
kubectl get prometheusrule -n order-lab
# 预期：order-api-alerts
```

## Step 2：验证 Prometheus 抓取

用临时 curl Pod 在集群内查询（宿主机 port-forward 在本实验容易卡）：

```bash
kubectl run curl-tmp -n monitoring --image=curlimages/curl:latest --restart=Never --command -- sleep 300
kubectl -n monitoring wait --for=condition=Ready pod/curl-tmp --timeout=60s
kubectl -n monitoring exec curl-tmp -- curl -s \
  'http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090/api/v1/targets?state=active'
# 预期：targets 里有 order-api，health=up
kubectl -n monitoring delete pod curl-tmp
```

## Step 3：准备压测数据（关键！口径必须一致）

压测脚本生成 `P1..P10000` 的 SKU，而种子数据只有 P1/P2，
**不补齐会导致 80% 读请求 404**（这是本实验最大的坑）：

```bash
kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash -e "
SET SESSION cte_max_recursion_depth=20000;
INSERT IGNORE INTO products (sku, name, price_cents)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10000)
SELECT CONCAT('P', n), CONCAT('Product ', n), 1000+(n%500) FROM seq;
INSERT IGNORE INTO inventory (sku, stock)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10000)
SELECT CONCAT('P', n), 1000 FROM seq;
SELECT COUNT(*) FROM products;  -- 预期 10000
SELECT COUNT(*) FROM inventory; -- 预期 10000
"

kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash \
  -e "UPDATE inventory SET stock=1000000 WHERE sku='P1';"   # 写压测热点 SKU
```

## Step 4：阶梯基线压测（宿主机 k6，经端口转发）

```bash
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
cd build-prod-system
k6 run tests/load/baseline.js
```

预期（6 分钟，50→1000 QPS 六档）：

```text
http_req_failed: 0.00%
✓ 'p(99)<200' p(99)≈16ms（读）
✓ 'p(99)<500' p(99)≈39ms（写）
```

## Step 5：容量上限探测（集群内 k6，绕过端口转发）

宿主机 port-forward 在 1000+ QPS 会先成为瓶颈（实测教训），
改用集群内 k6 Pod 直连 Service：

```bash
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 900
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=60s
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/probe.js' < tests/load/capacity-probe.js
kubectl -n order-lab exec k6-load -- sh -c \
  "sed 's|http://localhost:18080|http://order-api.order-lab.svc.cluster.local:8080|' /tmp/probe.js > /tmp/p.js && k6 run /tmp/p.js"
```

压测期间另开终端观察：

```bash
kubectl top pods -n order-lab
# 关键观察：order-api CPU 撞 limit（节流）时延迟/错误率开始恶化
```

## Step 6：结论记录

- 满足 SLO 的稳定容量（1000 QPS 档）：0 失败，读 P99 ~16ms / 写 P99 ~39ms。
- 容量上限：CPU limit 撞线（EXP-04 时 500m；EXP-05 后调整）。
- 首个瓶颈 = order-api CPU，不是 MySQL、不是代码热点。
- 施压端 `dropped_iterations` 是容量判断先行指标，比 QPS 先暴露问题。

## Step 7：Grafana 面板（可选）

导入 `deploy/monitoring/grafana-dashboard-order-api.json`：
Grafana → Dashboards → Import → 粘贴 JSON 内容。

## 常见坑

| 现象 | 原因 | 处理 |
|---|---|---|
| 读请求 80% 404 | SKU 无 P 前缀 / 库存表只有 2 行 | Step 3 补数据 |
| 1000+ QPS 时延迟爆炸 | 宿主机 port-forward 先成瓶颈 | 集群内 k6 Pod 直连 |
| 施压压不上去 | k6 dropped_iterations | 加大 maxVUs / 查服务端 CPU 节流 |

## 验收清单

- [ ] Prometheus target `order-api` health=up
- [ ] 规则组 `order-api.rules` 已加载
- [ ] 1000 QPS 档 0 失败且 P99 达标
- [ ] 能回答：CPU 节流如何影响容量？dropped_iterations 是什么？
