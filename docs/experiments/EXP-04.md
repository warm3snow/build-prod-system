# EXP-04：最小可观测性与容量 Baseline

## 1. 实验目标

- 建立最小可观测性：RED 指标 + Prometheus 抓取 + Grafana 面板 + 告警规则。
- 建立容量基线：满足 SLO 的最大稳定 QPS，并定位首个瓶颈。

## 2. 背景问题

“感觉慢”和“扛不住”都没有证据。基线必须回答：当前系统在什么负载下满足 SLO、
哪个资源先饱和、继续加压会怎样。

## 3. 初始架构

复用已有 kube-prometheus-stack（Prometheus/Grafana/Alertmanager/Loki/Tempo）。
order-api 新增独立 metrics 端口 2112，ServiceMonitor + PrometheusRule 纳入监控栈。

## 4. SLA / SLO（EXP-01 冻结值）

| Metric | Target |
|---|---:|
| 读 P99 | ≤ 200ms |
| 写 P99 | ≤ 500ms |
| 系统错误率 | ≤ 0.1%（压测允许阈值 1%） |
| 容量目标 | 1,000 QPS（缩尺档） |

## 5. Baseline（改造前）

- 无指标暴露、无告警、无容量数据；压测只能看 k6 总览，无法定位瓶颈。

## 6. Hypothesis

RED 指标（按 route 分类）＋业务订单指标，配合阶梯到达率压测与 Pod 资源观测，
可以定位“哪个资源先饱和”。

## 7. 实验方案

1. `internal/api/metrics.go`：http_requests_total（route/method/status）、
   http_request_duration_seconds（histogram）、orders_created/rejected/replayed。
2. 独立 2112 端口暴露 /metrics，与业务流量分离。
3. ServiceMonitor（release=kube-prometheus-stack 标签匹配）+ PrometheusRule（错误率/P99/CPU 节流）。
4. k6 阶梯压测（50→1000 QPS × 6 分钟，8:1:1 混合）+ 容量探测（1000→5000 QPS）。
5. Grafana 面板 JSON：deploy/monitoring/grafana-dashboard-order-api.json。

## 8. Code Change

- `internal/api/metrics.go`（新增）、`internal/observability/metrics.go`（新增）。
- `internal/api/handler.go`：接入 MetricsMiddleware，下单路径记录业务指标。
- `cmd/order-api/main.go`：metrics 独立端口 2112 + 优雅退出。
- `tests/load/baseline.js`、`tests/load/capacity-probe.js`。

## 9. Kubernetes Change

- Service 增加 metrics 端口；ServiceMonitor、PrometheusRule（order-lab 命名空间）。
- 镜像升级 order-api:exp04。

## 10. Load Test

- 阶梯基线（6 分钟，124,495 请求）：**0 失败**，读 P99 16.4ms、写 P99 39.5ms @ 1000 QPS 档。
- 容量探测（1000→5000 QPS）：集群内直连 1,415 req/s 时错误率 0.77%、
  P95 6.6s——**order-api CPU 撞 500m limit**（压测期 top 500m/500m，MySQL 496m）。

## 11. Failure Injection

不适用（EXP-14 起做故障注入）。

## 12. Observability

- Prometheus target `order-api up`（抓取正常，11 条时间序列）。
- 规则组 `order-api.rules` 已加载（36 组之一），当前 inactive（系统健康）。
- 告警触发链路验证（制造 5xx）留到 EXP-19 Game Day 统一执行。

## 13. Results

| Metric | Before | After | Change |
|---|---:|---:|---|
| 指标暴露 | 无 | RED+业务 5 个指标 | 新增 |
| 告警规则 | 无 | 3 条（错误率/P99/节流） | 新增 |
| 1000 QPS 稳定性 | 未知 | 0 失败，读 P99 16ms/写 P99 39ms | 达标 |
| 有效容量（SLO 内） | 未知 | ≈1,400 req/s（CPU limit 撞线前） | 实测 |
| 首个瓶颈 | 未知 | order-api CPU 500m limit | 定位 |

## 14. Root Cause

- 数据问题曾导致 80% 读失败：SKU 缺 `P` 前缀、库存表只有 2 行——压测数据必须与参数表一致。
- 容量瓶颈：500m CPU limit 下，1000 QPS 时 CPU 仅 188m（余量 62%）；
  探测到 1400+ req/s 时 CPU 打满并节流，延迟与错误率恶化。

## 15. Trade-offs

- 经 kubectl port-forward 施压会先撞转发瓶颈（本地探测 1000 req/s 即恶化）；
  最终采用集群内 k6 Pod 直连 Service，数据可信。
- 指标独立端口避免抓取流量污染 RED；代价是多一个端口约定。
- 告警阈值暂用 1%（压测可接受），生产级 0.1% 由 sla.md 定义。

## 16. Architecture Decision

- ADR-007：RED 指标按 route 归一化（FullPath），业务指标（订单数）单独计数。
- ADR-008：压测基线采用集群内 k6 Pod，不用本机 port-forward 施压。

## 17. Lessons Learned

- 压测数据与业务数据口径必须冻结一致（SKU 命名、库存行数），否则 80% 失败是数据问题不是性能问题。
- 看延迟必须同时看施压端 dropped_iterations 与服务端 CPU 节流，单看平均值会漏掉容量边界。
- 基线结论：SLO 内稳定容量 ≈1,400 req/s，当前 CPU limit 是硬顶——EXP-05 用 pprof 找热点，并评估提高 limit 与优化的关系。

## 18. Interview Questions

- 为什么压测用到达率模型而不是固定 VU？
  → 固定 VU 下服务变慢会降低实际施压（coordinated omission），测不到真实容量；到达率模型保持固定速率，真实暴露容量边界。
- dropped_iterations 是什么？为什么它是容量判断的先行指标？
  → VU 池耗尽时被丢弃的迭代；服务饱和时它先于错误率上升（实测送达 1219 req/s、dropped 704/s、错误率 0%）。
- CPU 节流（throttled_seconds）如何反映 limit 设置问题？
  → usage 顶到 limit 且 throttled 持续增长 = limit 是硬顶，瓶颈是资源限制而非代码慢。
- 为什么 metrics 要独立端口？
  → 抓取请求会污染业务 RED 指标的分母（拉低错误率），独立端口还利于故障隔离与 pprof 访问控制。
- 如何证明“1000 QPS 达标”而不只报一个平均延迟？
  → 到达率闭环（送达+dropped）+ 分位延迟（读 16.4ms/写 39.5ms）+ 错误分类 + 资源余量（CPU 188m/500m）组合证据，而非一个均值。
