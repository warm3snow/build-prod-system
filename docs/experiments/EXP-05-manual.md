# EXP-05 手工操作手册：Go 热点与容器资源约束

> 目标：用 pprof 证明「瓶颈是资源限制而不是代码热点」，并做 GOMAXPROCS/limit 的 A/B 对照。
> 前置：EXP-04 完成，镜像已包含 pprof（metrics 端口 2112 上挂 /debug/pprof/）。
> 预期耗时：约 30 分钟（含两轮各 3 分钟压测）。

## Step 0：确认 pprof 已挂载

```bash
kubectl -n order-lab get pods
# 确认 order-api 镜像为 exp05 或更新版本（含 pprof）
```

## Step 1：集群内起恒定负载

```bash
kubectl run k6-load -n order-lab --image=grafana/k6:latest --restart=Never --command -- sleep 900
kubectl -n order-lab wait --for=condition=Ready pod/k6-load --timeout=60s
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/load.js' < tests/load/constant-2000.js
kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js &
# 恒定 2000 QPS × 3 分钟，压测期间做 Step 2-3
```

## Step 2：抓取 30 秒 CPU profile（用 kubectl proxy，不要用 port-forward）

```bash
kubectl proxy --port=18001 &
sleep 4
# 注意：必须走 Service 代理 + 命名端口 metrics（Pod 代理带端口号的方式不可用）
curl -s --max-time 70 \
  'http://127.0.0.1:18001/api/v1/namespaces/order-lab/services/order-api:metrics/proxy/debug/pprof/profile?seconds=30' \
  -o /tmp/cpu-before.prof
ls -la /tmp/cpu-before.prof    # 预期 20KB+（206 字节=失败）
pkill -f 'kubectl proxy'
```

## Step 3：分析 profile

```bash
cd build-prod-system
go tool pprof -top -nodecount=15 /tmp/cpu-before.prof
```

**预期画像**：

```text
52% internal/runtime/syscall.Syscall6   ← 网络 I/O 系统调用（I/O 密集服务的正常画像）
17% runtime.futex                       ← goroutine 调度/锁
~0% 业务函数                            ← 没有可优化的代码热点！
```

结论：热点不在代码，而在「CPU limit 与 GOMAXPROCS 不匹配」导致的节流与调度开销。

## Step 4：A/B 对照实验

压测脚本、k6 配置不变，只改部署环境变量。每轮改完等 15 秒（滚动更新完成）。

### A 组：GOMAXPROCS=1，limit 500m

```bash
kubectl -n order-lab set env deployment/order-api GOMAXPROCS=1
kubectl -n order-lab patch deployment order-api -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"order-api","resources":{"limits":{"cpu":"500m"}}}]}}}}'
sleep 15
kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js
# 预期：送达 ~1780 req/s，但 p90 飙到 2.4s —— 单线程排队，SLO 不达标
```

### B 组：GOMAXPROCS=2，limit 1000m

```bash
kubectl -n order-lab set env deployment/order-api GOMAXPROCS=2
kubectl -n order-lab patch deployment order-api -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"order-api","resources":{"limits":{"cpu":"1000m"}}}]}}}}'
sleep 15
kubectl -n order-lab exec k6-load -- k6 run /tmp/load.js
# 预期：送达 ~1993 req/s，0 失败，p90 ~235ms —— 明显最优
```

## Step 5：结果对照（课程实测）

| 配置 | 送达 QPS | 掉迭代 | p90 | 结论 |
|---|---:|---:|---:|---|
| 500m + GOMAXPROCS 默认(6) | 1219 | 704/s | — | 频繁节流 |
| 500m + GOMAXPROCS=1 | 1780 | 208/s | 2.42s | 吞吐升但延迟雪崩 |
| 1000m + GOMAXPROCS=2 | 1993 | 5.8/s | 235ms | ✅ 冻结配置 |

## Step 6：固化配置与清理

最终配置写回部署清单（GOMAXPROCS=2、limit 1000m），并清理：

```bash
kubectl -n order-lab delete pod k6-load
```

## 本实验要回答的问题

1. CPU profile 52% 在 syscall 说明什么？该改代码还是加资源？
2. 为什么 GOMAXPROCS=6 会节流、GOMAXPROCS=1 会延迟雪崩？
3. dropped_iterations 为什么是容量判断的先行指标？
4. 如何区分「代码热点 / 锁竞争 / 资源限制」三种慢？
