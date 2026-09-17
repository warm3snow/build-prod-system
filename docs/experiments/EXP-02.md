# EXP-02：Kubernetes 最小订单运行底座

## 1. 实验目标

在 Rancher Desktop（k3s v1.33.5）上搭建最小订单系统：order-api（Go）＋ MySQL（PVC 持久化），
验证部署可重建、探针生效、优雅退出、数据持久化与幂等写路径。

## 2. 背景问题

性能与可用性实验需要可重复的部署底座。底座要求：
- 声明式部署，干净 Namespace 可重建；
- 应用就绪前不接流量（readiness）；
- 进程重启不丢数据（PVC）；
- SIGTERM 优雅退出，在途请求不被硬中断。

## 3. 初始架构

```text
kubectl port-forward (本实验)
        │
order-api × 1  ──── mysql × 1 (local-path PVC 2Gi)
```

## 4. SLA / SLO

本实验不压测；功能验收标准：所有接口行为与 EXP-01 契约一致。

## 5. Baseline

不适用（EXP-04 建立性能基线）。

## 6. Hypothesis

使用声明式配置＋持久化＋探针＋优雅退出，可以重复运行最小系统，不依赖人工补环境。

## 7. 实验方案

1. Go 服务：`/healthz`（存活）、`/readyz`（就绪，查 DB）、商品/库存查询、幂等下单、最新订单查询。
2. MySQL：单实例 Deployment＋PVC，建表与种子数据由应用启动时逐条执行。
3. K8s：Namespace `order-lab`、Secret、Deployment×2、Service×2。
4. 镜像：多阶段构建（distroless nonroot），本地 build 后 k8s 直接使用 VM 内 docker 镜像。

## 8. Code Change

- `cmd/order-api/main.go`：连接池、启动建表、优雅退出、结构化日志。
- `internal/api/handler.go`：HTTP 层，错误码映射（404/409/500）。
- `internal/store/mysql/store.go`：事务下单（幂等→扣库存→建订单）、逐条建表。
- `internal/config/config.go`：环境变量配置。
- `Dockerfile`：多阶段非 root 镜像。

## 9. Kubernetes Change

- `deploy/k8s/base/all.yaml`：Namespace、Secret、PVC、MySQL、order-api、Service。
- 探针：startupProbe/readinessProbe（/readyz）、livenessProbe（/healthz）。
- 资源：order-api 100m/64Mi req，500m/256Mi limit；MySQL 250m/512Mi req，1C/1Gi limit。
- MySQL readiness：`mysqladmin ping`。

## 10. Load Test

不适用。

## 11. Failure Injection

`kubectl delete pod -l app=order-api`：自动重建，库存数据不变（999）。

## 12. Observability

结构化 JSON 日志（请求 ID、耗时）；指标在 EXP-04 接入。

## 13. Results

| 检查项 | 结果 |
|---|---|
| 部署重建 | `kubectl apply` 幂等通过 |
| readiness 就绪前不接流量 | startupProbe 生效（DB 未就绪时应用不 Ready） |
| 下单 | 201 Created，stock 1000→999 |
| 幂等重放 | 同一 key 返回同一订单 ID，replayed=true |
| 同键不同参数 | 409 idempotency_conflict |
| 数据持久化 | 删 Pod 重建后 stock 仍为 999，订单可查 |
| 优雅退出 | 15s terminationGracePeriod，无请求中断 |
| 非 root 镜像 | distroless nonroot 运行 |

## 14. Root Cause（排障记录）

- **MySQL 启动慢**：初期 0/1 Pending 是 PVC 绑定与镜像拉取，等待后正常。
- **schema 1064 错误**：MySQL 驱动默认不支持 multiStatements，多语句合并执行失败；改为逐条拆分执行后修复。
- **同 tag 镜像缓存**：k8s 复用旧镜像，改 tag（exp02-v2/v3）强制更新。

## 15. Trade-offs

- 单实例 MySQL：本实验只验证持久化，不验证高可用（EXP-16 处理）。
- 应用启动建表：简单，但多人/多副本并发建表有风险；EXP-15 引入独立迁移。
- 内联 Secret：实验环境可接受；真实环境需外部密钥管理。

## 16. Architecture Decision

- ADR-003：采用 distroless nonroot 镜像＋local-path PVC。
- ADR-004：schema 由应用启动时逐条执行（本阶段），迁移工具后置。

## 17. Lessons Learned

- Rancher Desktop 的 docker 镜像对 k3s 直接可见，无需额外 import。
- MySQL 官方镜像需 root 启动，非 root 强化留到安全实验。
- 同 tag 重建镜像会被 k8s 缓存，本地迭代必须换 tag。

## 18. Interview Questions

- readiness 和 liveness 的区别？为什么 DB 故障时 readiness 失败但 liveness 不重启？
  → readiness 失败摘流不重启（依赖未就绪），liveness 失败才重启（进程死锁）；DB 故障时进程本身健康，重启风暴无益反而加重故障。
- 为什么 graceful shutdown 需要 terminationGracePeriodSeconds？
  → 它是优雅退出的预算上限：SIGTERM 后等在途请求完成，超时未退出才 SIGKILL，防止请求被硬中断。
- 为什么建表语句要逐条执行？
  → 驱动默认不支持 multiStatements（实测 1064）；逐条执行能精确定位失败语句，且避免开启多语句带来的注入面。
- PVC 在 Pod 删除后还存在吗？数据什么时候会丢？
  → 存在：PVC 是独立资源，删 Pod 重建后数据仍在（实测 stock 999）；丢数据的是删 PVC/删 Namespace/底层存储故障。
