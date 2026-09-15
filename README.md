# Production System Lab

基于 Kubernetes 从 0→1 构建高并发、高可用业务系统的实验仓库。

- **课程主线**：19 个必做实验，详见 [docs/roadmap.md](docs/roadmap.md)。
- **业务**：Flash Order 简版订单系统（商品/库存查询、下单、订单结果查询、异步后置任务）。
- **技术栈**：Go、MySQL、Redis、Kafka、Kubernetes、Prometheus/Grafana、k6。

## 目录

```text
cmd/                     可执行入口：order-api / outbox-relay / consumer / dependency-simulator
internal/                业务与基础设施内部包
deploy/k8s/              部署清单，base + overlays（ha / chaos）
deploy/helm/             Helm 模板
tests/load/              k6 压测脚本
tests/chaos/             故障注入脚本
docs/                    设计、实验报告、ADR 与复盘
docs/roadmap.md          19 个核心实验主线文档
```

## 约定

- 实验推进顺序与验收标准以 `docs/roadmap.md` 为准，不跳过前置实验。
- 只维护一个业务；Kafka 用于可靠的后置任务，下单成功语义见路线图第 2 节。
- 未经过对应故障演练与压测，不得标注“高可用”或“性能提升”。
