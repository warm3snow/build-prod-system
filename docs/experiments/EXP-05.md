# EXP-05：Go 热点与容器资源约束

## 1. 实验目标

用 pprof 定位 2000 QPS 负载下的 CPU 热点，回答两个问题：
- 瓶颈是代码热点还是容器资源限制？
- 「优化代码」与「调整资源配置」哪个更有效？

## 2. 背景问题

EXP-04 发现 500m CPU limit 是硬顶（2000 QPS 目标只送达 1219 req/s）。
需要证据判断：到底是代码慢，还是资源给少了。

## 3. 初始架构

order-api（Gin+GORM）单副本，metrics 端口 2112 挂载 pprof（/debug/pprof/）。
压测：集群内 k6 Pod 恒定 2000 QPS，8:1:1 混合。

## 4. SLA / SLO

| Metric | Target |
|---|---:|
| 读 P99 | ≤ 200ms（1000 QPS 档） |
| 写 P99 | ≤ 500ms（1000 QPS 档） |
| 系统错误率 | ≤ 0.1% |

## 5. Baseline（EXP-04 结论）

| Metric | Before |
|---|---:|
| 2000 目标 QPS 实际送达 | 1219 req/s |
| 掉迭代 | 704/s |
| CPU | 451m/500m（节流） |
| 错误率 | 0% |

## 6. Hypothesis

profile 会显示：热点不在业务代码，而在网络 syscall 与调度（futex）；
真正约束是 CPU limit 500m 与 GOMAXPROCS（默认 6，VM 核数）不匹配。

## 7. 实验方案

1. pprof 抓 30s CPU profile（2000 QPS 负载中）。
2. 分析 top 函数分布。
3. A/B 对照（同负载脚本、同 k6 配置，只改部署配置）：
   - A：GOMAXPROCS=1，limit 500m。
   - B：GOMAXPROCS=2，limit 1000m。
4. 对比送达 QPS、掉迭代、延迟、错误率。

## 8. Code Change

- `internal/observability/metrics.go`：metrics 端口挂载 pprof 端点。
- 无业务代码修改（实验目标即验证“无需改代码”）。

## 9. Kubernetes Change

- 镜像 order-api:exp05（含 pprof）。
- 最终配置：GOMAXPROCS=2，CPU limits 1000m（requests 100m 不变）。

## 10. Load Test

恒定 2000 QPS × 3 分钟，三组对照（数据见 Results）。

## 11. Failure Injection

不适用。

## 12. Observability

- CPU profile：52.3% `runtime.Syscall6`（网络 I/O）、17.2% `runtime.futex`（调度），
  无业务函数热点；堆 61Mi，无 GC 压力。
- 该画像的结论：I/O 密集 + 调度开销，不是可优化的代码热点。

## 13. Results

| 配置 | 送达 QPS | 掉迭代 | p90 | p95 | 错误率 | 结论 |
|---|---:|---:|---:|---:|---:|---|
| Baseline：500m，GOMAXPROCS 默认 | 1219 | 704/s | — | — | 0% | CPU 节流，k6 压不进去 |
| A：500m，GOMAXPROCS=1 | 1780 | 208/s | 2.42s | 3.17s | 0% | 吞吐 +46% 但延迟爆炸，SLO 不达标 |
| B：1000m，GOMAXPROCS=2 | 1993 | 5.8/s | 235ms | 479ms | 0% | 吞吐 +64%，延迟可接受 |

## 14. Root Cause

- 不是代码热点：profile 中无业务函数，52% 时间在网络 syscall。
- 是资源配置：500m limit 下 GOMAXPROCS=6（VM 核数）导致频繁节流 + futex 调度开销。
- GOMAXPROCS=1 消除节流但单线程排队 → 延迟雪崩。

## 15. Trade-offs

- GOMAXPROCS=2 + 1000m：吞吐接近目标（1993/2000），p95 479ms 接近写 SLO 边缘
  （2000 QPS 已超缩尺档 1000 QPS 目标 2 倍，正常态 SLO 在 1000 QPS 档验证）。
- 继续提高 limit 的边际收益与节点资源成本，留给 EXP-13（HPA）评估。

## 16. Architecture Decision

- ADR-009：I/O 密集服务 GOMAXPROCS 对齐 CPU limit（1 核 2 线程），
  不采用默认核数，不盲目降到 1。
- ADR-010：性能结论必须区分「代码热点」与「资源限制」，profile 证据先行。

## 17. Lessons Learned

- “感觉慢”时先抓 profile：本次完全不需要改业务代码，改两行部署配置 +64% 吞吐。
- 单看送达 QPS 会被 k6 掉迭代掩盖：1219 不是服务上限，是压测没压进去。
- GOMAXPROCS 对齐是 I/O 密集 + cgroup limit 场景的常见陷阱。

## 18. Interview Questions

- CPU profile 52% 在 syscall 说明什么？这时候该改代码还是加资源？
  → I/O 密集画像、无业务热点可优化，该调资源配置而非改代码（实测零代码改动 +64% 吞吐）。
- 为什么 GOMAXPROCS=1 延迟爆炸而 GOMAXPROCS=6 节流？
  → =6 时按 VM 核数并发但 cgroup 只有 0.5 核配额 → 节流+futex 开销；=1 时单线程排队 → 延迟雪崩（p95 3.17s）；2 对齐 1000m 才平衡。
- k6 dropped_iterations 是什么？为什么它比 QPS 先暴露问题？
  → VU 池耗尽被丢弃的迭代；服务饱和时先 dropped 后报错，只看送达 QPS 会误判容量。
- 如何判断一个“慢”是代码热点、锁竞争还是资源限制？
  → profile 业务函数占比高=代码热点，syscall/futex 多=资源限制，锁函数高=锁竞争；再用单变量对照确认归因。
