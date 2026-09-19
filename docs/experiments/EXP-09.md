# EXP-09：Outbox、消费幂等与可靠事件

## 1. 实验目标

- 以「下单事务 + Outbox 事件」消除应用双写缺口：订单提交 == 事件提交；
- 验证 Outbox Relay 的 at-least-once 投递：仅在 Kafka 确认后标记 SENT，未知结果允许重试；
- 验证 Consumer 的 Inbox 去重：去重与后置副作用同一 MySQL 事务，业务提交后才提交位点；
- 在三个崩溃点（Relay 崩溃、已发送未标记、消费后位点未提交）验证副作用唯一；
- 验证 Kafka 不可用不影响下单，事件积压有界可观测，恢复后自动排空。

## 2. 背景问题

EXP-08 之前，下单响应即事务提交，但没有任何异步后置能力：后续的「订单受理通知、
异步投影」若在 HTTP 路径内同步做会放大下单延迟与故障半径；若提交后再发消息，则
「DB 已提交 + 消息未发」的崩溃窗口造成事件遗漏。经典 Outbox 模式把事件写入与业务
放在同一事务，投递交给独立 Relay，消费侧用 Inbox 去重，是异步可靠性的最小正确解。

## 3. 可靠事件链路（本实验冻结）

```text
写路径：下单事务（库存+订单+幂等+Outbox，同一事务，EXP-03 不变量不变）
  └─ 新订单写 outbox_events（event_id UUID 唯一，PENDING）；重放不产生事件

Relay（outbox-relay 独立进程）：
  PENDING → 轮询（500ms，批量 100，按 id 升序）
  ├─ Produce（acks=all，key=event_id，2s 超时）→ 确认 → 标记 SENT
  ├─ 确认失败/超时 → 保持 PENDING 重试（attempts+1）
  └─ attempts ≥ 100 → DEAD（显式失败，告警 + SQL 可重放）

Kafka：orders 主题，单分区单副本（KRaft 单 Broker，故障边界见 §15）

Consumer（consumer 独立进程，单副本保序）：
  Fetch → 事务{INSERT inbox（event_id 主键）+ INSERT order_notifications}
  ├─ 事务成功（新/重复）→ CommitMessages（不越过未完成消息）
  └─ 事务失败（DB 不可用）→ 不提交位点，1s 后重试同一消息

对账锚点：event_id 贯穿 outbox_events → Kafka key → inbox_events，逐事件可对账。
关联 ID：request_id（HTTP 中间件）→ Outbox → Kafka header → Consumer 日志；
traceparent 头原样透传（预留 OTel）。
```

- **Outbox 与订单同事务**：不存在「DB 已提交、事件未写」的窗口；
- **Relay at-least-once**：已发送未标记、确认超时都是合法重试路径，重复由 Inbox 吸收；
- **Inbox 与副作用同事务**：重复投递只命中唯一键，副作用不重复；位点提交在事务之后；
- **保序主线**：单分区 + 单 goroutine 逐条处理；失败消息不提交位点，后续消息不越过它。

## 4. SLA / SLO（冻结）

| Metric | 正常态 | 故障态 |
| --- | ---: | ---: |
| 下单 P99 | ≤ 500ms（EXP-01 冻结，不因 Kafka 变差） | Kafka 不可用期间保持不变 |
| 投递延迟（下单提交→Kafka 确认）P99 | ≤ 2s | Kafka 不可用时事件保持 PENDING（不丢） |
| 业务完成延迟（下单→后置副作用）P99 | ≤ 5s | 恢复后 5min 内排空并收敛 |
| 副作用唯一性 | 每订单恰 1 条通知 | **任何单进程崩溃点**下仍成立 |
| 事件遗漏 | 0 | 0（Outbox 与订单同事务） |
| 系统错误率 | ≤ 0.1% | Kafka 故障期间下单路径不受影响 |

- 崩溃点范围：relay/consumer 进程崩溃（Pod 删除/SIGKILL），持久化存储可用；
- 通过标准：恢复后 Outbox 排空且副作用唯一；未投递事件保留并告警；
  业务成功不依赖 Kafka 当时立即可达。

## 5. Baseline（来自 EXP-03/04/08）

- 下单事务（幂等占位→扣库存→建订单→回填）P99 39.5ms @1000 QPS（EXP-04）；
- EXP-08 后写路径：事务 + 异步缓存失效，Redis 故障时下单 P99 1.3s（P1 热点行锁）；
- 无 Outbox：下单成功后无任何异步事件，客户端只能轮询订单接口。

## 6. Hypothesis

1. Outbox 写入与订单同事务，事务 P99 增加可忽略（单行 INSERT）；
2. Relay 确认前崩溃/超时会导致重复投递，但 Inbox 去重后副作用恰好一次；
3. Consumer 事务提交后位点未提交（崩溃）→ 重启从旧位点重读 → dup 命中，无重复副作用；
4. Kafka 不可用时下单可用性与延迟不受影响，事件 PENDING 积压（不丢）；
5. Kafka 恢复后 Relay 自动排空积压，无需人工干预；
6. 完成延迟 P99 ≤ 5s（Relay 轮询 500ms + 消费事务毫秒级，正常态远低于上限）。

## 7. 实验方案

1. **场景 A（正常投递链路）**：8:1:1 混合 2000 QPS × 5min（cache-product.js，下单 ~200 TPS）。
   观察 outbox 积压趋零、relay_sent 速率、consumer_processed、完成延迟分位、Kafka lag；
   压测后逐事件对账。
2. **场景 B（三个崩溃点）**：下单流量持续（200 TPS 档），分别注入：
   - B1 Relay 崩溃：停 relay 60s（积压增长）→ 重启 → 排空，验证 sent 数 == 积压数；
   - B2 已发送未标记：压测中 SIGKILL relay 3 次（随机时刻，覆盖「确认后、标记前」窗口）
     → 重启 → consumer dup 命中 > 0 且通知无重复；
   - B3 消费后位点未提交：压测中 SIGKILL consumer 3 次 → 重启 → dup 命中，副作用唯一。
3. **场景 C（Kafka 不可用及恢复）**：压测 60s 后 scale kafka→0，故障 90s 后恢复。
   故障期观察下单成功/延迟不变、outbox 积压增长速率、告警触发；
   恢复期观察积压排空、relay 投递速率、consumer 追赶、lag 收敛。
4. **对账**（所有场景，压测后执行）：
   - `orders` 新单数 == `outbox_events` 数（每 order 恰 1 事件）；
   - `inbox_events` 去重后数 == `outbox_events` SENT 数；
   - `order_notifications` 数 == `inbox_events` 数，且每 order_id 唯一；
   - 客户端成功响应账本（k6 成功下单计数）与 DB 订单数一致。

## 8. Code Change

- `internal/event/event.go`（新增）：`OrderCreated` 事件、`TraceContext`（request_id/traceparent）、UUID v4 生成。
- `internal/store/mysql/outbox.go`（新增）：
  - `OutboxEvent`（event_id 唯一键，PENDING/SENT/DEAD 状态机，attempts/last_error）；
  - `InboxEvent`（event_id 主键）、`OrderNotification`（order_id 唯一，后置副作用）；
  - `FetchPendingOutbox`（id 升序）/`MarkOutboxSent`/`MarkOutboxFailed`/`RequeueDead`/`OutboxStats`；
  - `ProcessInboxEvent`：事务 {INSERT inbox（1062→dup）+ INSERT notification}。
- `internal/store/mysql/store.go`：`CreateOrder` 事务内写 Outbox（新订单，重放不写）；
  签名增加 `event.TraceContext`；AutoMigrate 增加三张表。
- `internal/mq/kafka/kafka.go`（新增）：Producer（acks=all、key=event_id、2s 超时、幂等建主题）
  与 Consumer（手动位点，新消费组从 FirstOffset，保序）。
- `internal/relay/`（新增）：轮询投递循环、积压/DEAD 指标；`cmd/outbox-relay`。
- `internal/consumer/`（新增）：单 goroutine 保序处理、dup/new 指标、完成延迟直方图；`cmd/consumer`。
- `internal/api/handler.go`：withLogging 把 request_id/traceparent 放入 gin context；
  createOrder 传入事件关联上下文。
- `internal/config/config.go`：Kafka/Relay/Consumer 全部 env 配置。
- 依赖：`github.com/segmentio/kafka-go v0.4.51`。

## 9. Kubernetes Change

- 镜像：`order-api:exp09a`、`outbox-relay:exp09a`、`consumer:exp09a`。
- 新增 Kafka（apache/kafka:3.9.0，KRaft 单节点 StatefulSet + headless Service，
  `KAFKA_HEAP_OPTS=-Xmx512m`、PVC 2Gi、cluster.id 固定）——单实例，**不声称高可用**（EXP-17 验证）。
- 新增 outbox-relay / consumer Deployment（各独立小连接池 4/2，EXP-06 预算表预留）。
- ServiceMonitor 增加 relay/consumer；PrometheusRule 增加 Outbox 积压/停滞/DEAD 三条告警。
- 构建说明：网络受限时复用「exp08e 底座 + docker cp 注入二进制 + docker commit」
  离线构建三个镜像（relay/consumer 与 order-api 同为 distroless 底座），见 manual。

## 10. Load Test

- 复用 `tests/load/cache-product.js`（8:1:1、2000 QPS、SKU_COUNT=100）——下单 ~200 TPS；
- `tests/load/smoke.js` 冒烟不变；k6 成功下单计数（status 201/200）作为客户端账本。

## 11. Failure Injection

- Kafka 停服（scale→0）90s 与恢复；故障期间持续下单与读。
- Relay/Consumer 进程崩溃：`kubectl delete pod`（SIGTERM 优雅）与
  `kubectl exec -- kill -9`（模拟突然中断）各执行若干轮，覆盖三个崩溃点。

## 12. Observability

- Relay：`relay_sent_total`、`relay_failed_total`、`relay_dead_total`、
  `relay_errors_total{op}`、`outbox_backlog`、`outbox_oldest_pending_age_seconds`、`outbox_dead`。
- Consumer：`consumer_processed_total{result=new|dup}`、`consumer_process_errors_total{op}`、
  `order_post_process_latency_seconds`（业务完成延迟）、`kafka_consumer_lag`。
- 关联：`request_id` 贯穿 HTTP 日志 → Outbox → Kafka header → Consumer 日志；
  `trace_parent` 透传预留 OTel 上下文（traceparent 头原样保存）。
- 告警：`OutboxBacklogHigh`（>10000 条 5m）、`OutboxRelayNotDelivering`（有积压且 5m 零投递）、
  `OutboxDeadEvents`（出现 DEAD）。

## 13. Results

实测环境：Rancher Desktop k3s（单机），镜像 `order-api:exp09a`、`outbox-relay:exp09a`、
`consumer:exp09a`、`apache/kafka:3.9.0`（KRaft 单节点）。
**迭代**：exp09a 历经 9 处修复（见第 14 节），最终配置下的数据如下。

### 场景 A：正常投递链路（8:1:1 2000 QPS，最终配置）

| 指标 | 实测 | SLO | 判定 |
| --- | ---: | ---: | --- |
| 系统失败率（3min 复测） | 0%（0/357068） | ≤0.1% | 通过 |
| 投递延迟 P50 / P99 | 336ms / 1969ms | P99 ≤ 2s | 通过 |
| 完成延迟 P50 / P99 | 1.96s / **29.2s** | P99 ≤ 5s | **不通过** |
| 对账（订单增量 : 事件 : 消费 : 副作用） | 1:1:1:1 | 唯一 | 通过 |

- **完成延迟 P99 不通过**：consumer 保序串行消费（每消息一个事务，~200-250 msg/s）
  与下单 200 TPS 速率平衡，队列堆积导致完成延迟随压测时长增长。
  这是保序主线的固有代价，EXP-10（消费并行与反压）正面解决。
- 压测中 Kafka 重复投递（relay 重试）累计 4.6 万+ 条全部被 Inbox 吸收，副作用唯一。

### 场景 B：三个崩溃点

| 崩溃点 | 注入方式 | 结果 |
| --- | --- | --- |
| B1 DB 提交后 Relay 崩溃 | 停 relay 60s（积压 10344+） | 恢复后排空，SENT 增量 == 订单增量，0 遗漏 |
| B2 已发送未标记 | 确定性重放（SENT→PENDING） | consumer `dup=true`，通知数不变 |
| B3 事务提交后位点未提交 | 位点回退 5 条重读 | `dup=true` ×5，通知数不变 |
| 随机 SIGKILL（relay×7 + consumer×8，两轮） | `delete pod --force` | 12 万下单仅 2 失败（0.003%），1:1:1 对账成立 |

随机 SIGKILL 难以命中「已发送未标记」（毫秒级）窗口，故 B2/B3 以确定性构造
精确复现；随机注入验证的是恢复能力与下单可用性。

### 场景 C：Kafka 不可用及恢复（scale→0，90s）

- 故障期下单 **48000 单 0 失败**（业务不依赖 Kafka 立即可达）；
- 事件 PENDING 积压，Kafka 恢复后自动排空（PENDING→0），1:1:1 对账成立。

### 意外发现：Kafka 单节点数据丢失事故（OOM + 挂载错误）

实验期间发生两次真实事故，暴露并修复了两个部署缺陷：

1. **OOM（Exit 137）**：`-Xmx512m` + 1Gi limit 在 200 TPS 投递 + 消费下被 OOM kill
   3 次；KRaft 单节点未 flush 的日志随崩溃丢失，29 万条已确认消息消失。
2. **PVC 挂载路径错误**：apache/kafka:3.9.0 的 KRaft 日志目录是 `/tmp/kafka-logs`，
   首版挂到了 `/tmp/kraft-combined-logs`——数据一直在容器可写层，Pod 重建即丢。
   受控 scale→0 也复现了数据丢失。

两次事故中，**已 SENT 未消费的事件通过 Outbox 重放（5208 条）完整修复**，
1:1:1 对账恢复。修复（2Gi limit + 768m heap、PVC 挂载路径）后，
Kafka 重启数据保留（重启前后 offset 不变）。

**边界结论**：单 Broker + acks=all 只保证「leader 内存确认」，不保证崩溃后
磁盘持久（默认 flush 策略 + 无副本）；本实验因此不声称 Kafka 高可用——EXP-17
以 3 副本 + ISR 正面解决。Outbox 重放是本场景的恢复手段。

## 14. Root Cause

**实验期间修复的 9 个问题**（按发现顺序）：

1. **kafka-go Writer 默认 `BatchTimeout=1s`**：串行逐条 `WriteMessages` 每条触发
   1s 批量超时，relay 实际吞吐 ~1 msg/s（首批测试 sent=10/批 完全吻合）。
   修复：整批一次 `WriteMessages`（BatchSize=100 + BatchTimeout=50ms）。
2. **relay 逐条 autocommit UPDATE**：每条一次 fsync，与下单事务竞争 redo log。
   修复：批量标记单事务（fsync 归组）。
3. **MySQL 1 核 CPU 饱和**：事件链路（outbox 同事务 INSERT + relay 轮询/标记 +
   consumer 消费事务）使 1 核 limit 打满（实测 1.0 核）→ 连接池排队 + readyz 超时。
   修复：MySQL limit 1→2 核（预算重估）。
4. **order-api 1 核 CPU 饱和**：下单路径新增固定开销（uuid+json.Marshal+outbox
   INSERT），2000 QPS 混合负载下 1 核打满（实测 0.999 核）→ 全链路排队
   （下单 P50 3.4s、连接池等待 44 万秒/6min）。修复：limit 1→2 核。
5. **下单事务热点锁窗口过长**：库存 UPDATE 原在事务第 2 步（持锁到提交），
   价格查询带 FOR UPDATE 锁商品行；与 outbox INSERT 叠加后 P1 锁队列爆炸。
   修复：库存 UPDATE 移至事务末尾（锁窗口 = 1 UPDATE + COMMIT）、价格普通读
   （价格不可变）。集成测试（200 并发不超卖/幂等/回滚）全部通过。
6. **批量标记 UPDATE 的 next-key 锁冲突**：`WHERE id IN (...) AND status='PENDING'`
   走 status 二级索引大范围扫描加锁，与下单 INSERT 的新行冲突，UPDATE 堆积等锁
   2s+ 超时。修复：纯主键点更新，去掉 status 条件（标记幂等）。
7. **Kafka OOM**：见场景 C 意外发现。修复：2Gi limit + 768m heap。
8. **Kafka PVC 挂载路径错误**：见场景 C 意外发现。修复：挂载 `/tmp/kafka-logs`。
9. **完成延迟直方图缺 `+Inf` bucket**：超 60s 观测丢失。修复：补 `math.Inf(1)`。

## 15. Trade-offs

- **at-least-once + 幂等消费**：不追求 Kafka exactly-once（不自动覆盖外部 MySQL 修改）；
  重复投递的成本由 Inbox 唯一键吸收，语义简单且可证明。
- **单分区保序**：牺牲消费并行（EXP-10 再评估分区与并发），换取「位点提交不跳过未完成任务」
  的可证明性；本实验下单 ~200 TPS，单消费者事务吞吐充足。
- **Relay 轮询**：投递延迟下界 = 轮询间隔（500ms），换取实现简单与有界 DB 负载；
  正常态 P99 投递延迟远低于 2s SLO。
- **Kafka 单 Broker**：仅验证「持久化存储可用 + 应用进程崩溃」范围的可靠性；
  Broker 故障/数据丢失不在本实验保障范围（EXP-17）。
- **DEAD 事件**：attempts≥100 标记 DEAD 并告警，不静默跳过；重放为显式 SQL 操作
  （manual 提供），坏消息始终可追踪。
- **消费失败不提交位点**：DB 不可用时消费停滞（保序代价），EXP-10 讨论反压与超限策略。

## 16. Architecture Decision

- **ADR-018**：订单事件采用事务内 Outbox + 独立 Relay（at-least-once）+ Consumer Inbox
  去重（副作用同事务、位点后提交）。事件与业务数据同库，作为同一一致性恢复集
  （EXP-18 恢复范围）。
- **ADR-019**：后置任务用「订单受理通知」投影模拟（不接真实外部系统）；
  每个已提交订单恰一行通知，终态语义为 INSERT-only（重复消息不产生状态倒退）。

## 17. Lessons Learned

- **消息中间件客户端的默认参数会暗中限速**：kafka-go 的 `BatchTimeout=1s` 默认值
  针对高吞吐批量场景，串行逐条写入时每条付 1s。接入新组件先测端到端吞吐，
  不要假设「发一条就立刻发出去」。
- **单行热点锁的边际成本是非线性的**：200 TPS × 5ms 在临界点附近，事务 +1 条
  INSERT 就从「可承受排队」跨到「队列爆炸」。锁持有窗口的最小化
  （热点 UPDATE 放事务末尾）是本实验最有效的事务级优化。
- **事件链路的 CPU/连接成本必须计入预算**：outbox 写入、relay 轮询、consumer
  事务都是真实负载，EXP-06 的预算表需按组件重估（本实验 MySQL/order-api 各 +1 核）。
- **故障注入会先打在你的基础设施上，再打在业务上**：本实验的「Kafka 崩溃点测试」
  最终演变成 Kafka 自身的 OOM 与 PVC 挂载错误——先确认基础设施的持久化与资源
  配置正确，否则业务级结论无法归因。
- **确认默认路径，不要靠猜**：`/tmp/kraft-combined-logs` 是旧版镜像的默认目录，
  3.9 已改为 `/tmp/kafka-logs`；挂错路径的后果延迟到 Pod 重建才爆发。
- **Outbox 重放是最后的恢复手段**：Kafka 数据丢失后，5208 条已确认事件靠
  「SENT 且未被消费 → 重置 PENDING → 重投」完整修复。课程要求「坏消息可重放」
  在此得到真实检验。
- **随机 kill 打不中窄窗口**：毫秒级的「已发送未标记」窗口用随机注入难以命中，
  用确定性构造（状态重置/位点回退）才能精确复现并验证去重语义。

## 18. Interview Questions

- 为什么 Outbox 必须与订单同事务？异步写事件会丢什么？
  → 同事务：订单提交 == 事件提交，崩溃不可能造成「订单存在、事件不存在」；
  先提交订单再异步发消息，进程在两步之间崩溃即永久遗漏。
- Relay 为什么 at-least-once 而不是 exactly-once？重复投递如何被吸收？
  → Kafka 确认语义下「发送成功但确认丢失」必然产生重复；Relay 只保证不丢。
  重复由 Consumer Inbox 唯一键（event_id 主键）去重，副作用与去重同事务，恰好一次。
- 消费位点为什么在事务提交之后？反过来会怎样？
  → 先提交位点后提交事务，位点提交后进程崩溃 → 消息被跳过且副作用丢失，无法恢复；
  先事务后位点，最坏重读 → Inbox 去重，副作用不重复。
- 保序处理下一条消息失败会阻塞后续吗？为什么可以接受？
  → 会：失败消息不提交位点，后续消息不越过它。EXP-09 主线以正确性优先；
  EXP-10 在并发与反压之间重新权衡。
- Kafka 不可用期间下单为什么不受影响？积压会无限增长吗？
  → 下单只写 MySQL（含 Outbox），不碰 Kafka；积压增长由指标+告警观测，
  EXP-10 落实超限反压策略；DEAD 机制兜底投递失败。
- 每订单一个事件、每事件一条通知的「1:1:1」对账链如何构造？
  → orders ⊆ outbox（事件 ID 唯一键）→ SENT ⊆ inbox（去重后等数）→
  notifications（order_id 唯一键）；三条链上的计数差即定位遗漏/重复的位置。
