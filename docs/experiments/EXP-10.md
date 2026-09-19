# EXP-10：后置任务削峰、消费并行与反压

## 1. 实验目标

- 验证事件链路对受控突发的吸收能力：积压模型（输入-消费=积压）与排空时间实测可解释；
- 固定 3 分区，对照不同消费者配置（并发 worker 数、实例数）的吞吐、DB 负载与重平衡影响；
- 实现有限在途（有界队列）与 watermark 顺序位点提交：并发处理不越过未完成任务；
- 建立积压高水位反压：下单侧明确拒绝（503 backlog_limited），Kafka 磁盘/保留期双重预算，
  积压不能无限增长；
- 失败任务可追踪：DB 抖动重试、毒丸跳过记录、DEAD 重放，恢复后正确排空。

## 2. 背景问题

EXP-09 建立可靠事件链路，但两个量化问题未解决：

1. **消费吞吐瓶颈**：保序串行消费（每消息一个事务）实测 ~200-250 msg/s，
   与下单 200 TPS 恰好平衡——完成延迟 P99 29.2s（SLO 5s 未达标），
   任何输入超过消费能力即无限积压。
2. **积压无预算**：Kafka 停服 90s 积压增长、下单不受限——积压只受「Relay 投递 +
   Kafka 磁盘」的自然上限约束，没有事前预算与主动反压。

## 3. 消费与反压链路（本实验冻结）

```text
输入：下单事务写 Outbox（EXP-09 不变）
  └─ order-api 反压检查：PENDING ≥ 高水位（OUTBOX_BACKLOG_LIMIT=200000）
     → 503 backlog_limited（明确拒绝，幂等键可重试；水位回落后自动恢复受理）

投递：relay 批量投递（batch 200 × 200ms，EXP-09 结论保持）

Kafka：orders 主题 3 分区（固定）、副本 1、key=event_id hash
  磁盘预算：保留期 24h 或 1.5GiB（< PVC 2Gi），先到先删最老段

消费：consumer（1 实例起步，实验对照 1/2/3 实例）
  reader（单）→ 有界队列（MaxInflight=16，满则暂停 fetch = 反压）
  → 8 workers 并发处理（Inbox 去重 + 通知副作用同事务，无业务顺序依赖）
  → watermark 按分区顺序提交位点：不越过未完成/失败消息
  失败：DB 不可用循环重试（阻塞该分区水位）；毒丸跳过记录；DEAD 可重放
```

- **并发化边界**：副作用 INSERT-only + Inbox 全局去重，事件间无顺序依赖；
  位点提交仍保序（watermark），「不跳过未完成任务」的保证不因并发削弱。
- **有界在途**：MaxInflight 是下游（DB）变慢时的自动反压阀——worker 阻塞 → 队列满 →
  暂停 fetch，消费压力随下游收缩。
- **分区固定 3**：分区数不可逆（只增不减）；分区分配限制有效并行度，实例数对照在此约束内。

## 4. SLA / SLO（冻结）

| Metric | 正常态（下单 200 TPS） | 突发态 | 超预算态 |
| --- | ---: | ---: | ---: |
| 完成延迟 P99 | ≤ 5s（EXP-09 未达标，本实验达成） | 积压期允许排队 | 拒绝而非拖延 |
| 投递延迟 P99 | ≤ 2s（EXP-09 沿用） | 同上 | 同上 |
| 消费吞吐 | ≥ 500 msg/s（单实例） | 决定排空速率 | — |
| 积压预算 | 0（无积压） | 突发 12 万全额吸收 | ≥ 20 万 → 503 拒绝 |
| 排空时间 | — | ≤ 10min（模型可解释） | 恢复后自动排空 |
| 副作用唯一性 | 每订单恰 1 通知 | 同左 | 同左 |
| 失败率 | ≤ 0.1% | 拒绝不计入系统错误 | 拒绝分类可见 |

- 突发口径：2000 TPS 下单 × 60s ≈ 12 万事件（无预热、从零积压起）。
- 反压口径：水位 200000；拒绝为明确 503 `backlog_limited`，幂等重试可恢复。

## 5. Baseline（来自 EXP-09）

- 消费吞吐：串行保序 ~200-250 msg/s；完成延迟 P99 29.2s；
- 投递延迟 P99 1969ms；失败率 0%（最终配置）；
- 连接池：consumer 4 连接（本实验 16）；MySQL 2 核、order-api 2 核、Kafka 2Gi/768m。

## 6. Hypothesis

1. 并发 8 worker + 16 在途时，消费吞吐从 ~200 提升到 500+ msg/s（MySQL group commit
   吸收并发事务的 fsync），完成延迟 P99 降至 5s 内；
2. watermark 顺序提交在并发下仍保证「不跳过未完成任务」：任一消息失败，其后
   已完成消息的位点被拦住，副作用已产生、位点未提交（重启重读由 Inbox 吸收）；
3. 有界在途让 DB 变慢时消费自动收缩（fetch 暂停），不会压垮 MySQL；
4. 3 分区下，1 实例吞吐 = 实例内并发上限；2/3 实例按分区分配并行扩展
   （单实例分区数为 3 时吞吐受分区分配限制）；重平衡窗口内消费短暂中断；
5. 积压模型线性：突发 12 万 - 消费 500/s × 60s = 峰值积压 ≈ 9 万 < 水位 20 万，
   全额吸收；排空 ≈ 峰值/（消费-输入）；
6. 水位 20 万 + 突发 12 万不触发反压；水位调低后同突发触发 503，积压封顶，
   水位回落后自动恢复受理。

## 7. 实验方案

1. **场景 A（突发吸收与排空模型）**：水位 200000。无积压起，突发 2000 TPS 下单 × 60s
   （纯下单脚本），之后回落到 0。记录：输入曲线（k6 到达率与 201 计数）、
   积压峰值（outbox_backlog）、消费曲线（consumer_processed rate）、排空时间。
   与模型（输入-消费=积压）对照解释偏差。
2. **场景 B（消费并行对照）**：固定 3 分区，回放 6 万积压（下单 2000 TPS × 30s 制造）：
   - B1：1 实例 × concurrency 8；
   - B2：2 实例 × concurrency 8；
   - B3：3 实例 × concurrency 8。
   记录排空吞吐（msg/s）、MySQL CPU、重平衡事件（consumer 日志/消费组状态）。
3. **场景 C（反压验证）**：水位调低至 30000。突发 2000 TPS 下单 → 积压达水位后
   `orders_rejected_backlog_total` 增长、201 计数停滞 → 积压封顶 ≈ 水位 →
   停止输入 → 排空 → 恢复受理（水位回落，503 消失）。
4. **场景 D（失败可追踪）**：消费中注入 DB 短暂不可用（停 consumer DB？实验以
   `kill consumer` 模拟处理中断）+ 毒丸构造（直插坏 payload 事件）+ DEAD 重放验证。
5. **对账**：1:1:1 全链 + 拒绝窗口对账（拒绝数 == 输入 - 受理数，不产生订单/事件）。

## 8. Code Change

- `internal/consumer/processor.go`（重构）：
  - reader（单）→ 有界队列（`MaxInflight`）→ N workers 并发 → committer；
  - `committer` 按分区 watermark 顺序提交（`partitionState{nextToCommit, done}`），
    连续完成段才提交，失败消息拦住水位；
  - 毒丸跳过提交；DB 失败循环重试；
  - 指标：`consumer_inflight`（在途 gauge）。
- `internal/api/backpressure.go`（新增）：`Backpressure` 采样器——后台每 500ms
  COUNT PENDING（索引计数）存 atomic，下单前检查水位。
- `internal/api/handler.go`：`createOrder` 反压检查（503 `backlog_limited`）；
  `Store` 接口加 `CountPendingOutbox`；`NewServer` 加 bp 参数。
- `internal/api/metrics.go`：`orders_rejected_backlog_total`。
- `internal/store/mysql/outbox.go`：`CountPendingOutbox`。
- `internal/mq/kafka/kafka.go`：`ensureTopic` 分区数参数化 + 分区数不足报错
  （分区只增不减的显式约束）。
- `internal/config/config.go`：`KAFKA_PARTITIONS=3`、`CONSUMER_CONCURRENCY=8`、
  `CONSUMER_MAX_INFLIGHT=16`、`OUTBOX_BACKLOG_LIMIT=200000`、
  consumer DB 池 16/8。
- `cmd/consumer`、`cmd/outbox-relay`、`cmd/order-api`：配置接线。

## 9. Kubernetes Change

- orders 主题重建为 3 分区（删除重建，事件从 Outbox 重放补齐，见 manual）；
- Kafka：`log.retention.hours=24`、`log.retention.bytes=1.5GiB`（磁盘预算 < PVC 2Gi）；
- consumer：concurrency/inflight env、DB 池 16；实例数对照用 `kubectl scale`；
- 告警：`KafkaDiskUsageHigh`（PVC > 80%）、`ConsumerStalledWithBacklog`。

## 10. Load Test

- 复用 EXP-09 纯下单脚本（200/2000 TPS 可配）；
- 突发模型：constant-arrival-rate 2000 TPS × 60s（输入 12 万）；
- 回放吞吐不计为真实下单 TPS（课程口径：回放是后置任务负载，下单能力由 EXP-04/13 界定）。

## 11. Failure Injection

- consumer 实例 SIGKILL / force delete（处理中断与位点回退）；
- 毒丸消息构造（直插不可解析 payload 的 outbox 行 → DEAD 路径）；
- Kafka 停服（EXP-09 已验证，本实验侧重积压预算与恢复排空）。

## 12. Observability

- 新增：`consumer_inflight`、`orders_rejected_backlog_total`；
- 沿用：`outbox_backlog`、`outbox_oldest_pending_age`、`relay_sent_total`、
  `consumer_processed_total{new,dup}`、`order_post_process_latency_seconds`、
  `kafka_consumer_lag`、`relay_dead_total`；
- 告警：OutboxBacklogHigh（沿用，阈值对齐水位）、KafkaDiskUsageHigh、
  ConsumerStalledWithBacklog。

## 13. Results

实测环境：Rancher Desktop k3s（单机），镜像 exp10a，orders 主题 3 分区。
迭代：exp10a 历经 5 处修复（见第 14 节），最终配置数据如下。

### 场景 A：突发吸收与排空模型（回放 12 万事件）

| 指标 | 实测 | 模型预测 | 判定 |
| --- | ---: | ---: | --- |
| 注入 | 12 万 / 8.8s（13669/s） | — | — |
| 峰值积压 | 107600 | 120000 - 1058×8.8 ≈ 110700 | 偏差 2.8% |
| 排空时间 | ~105s | 107600/1058 ≈ 102s | 偏差 3% |
| 消费速率 | 550-650 msg/s（批处理后） | ≥500 | 通过 |
| 对账 | 回放域 12 万 = 12 万 = 12 万 | 1:1:1 | 通过 |

**积压模型线性可解释**：峰值 ≈ 输入 - 消费×注入时长，排空 ≈ 峰值/消费。

### 场景 B：消费并行对照（3 分区固定，回放积压排空）

| 配置 | 消费吞吐 | 说明 |
| --- | ---: | --- |
| EXP-09 串行（对照） | ~200-250 msg/s | 每事务 fsync 串行墙 |
| 1 实例 × 8 并发 × 批 50 | **550-650 msg/s** | 合并提交 + 批量事务 |
| 2 实例 × 8 并发 | 510 msg/s | 无收益 |
| 3 实例 × 8 并发 | 457 msg/s | 无收益（rebalance 抖动） |

**结论**：分区/实例扩展无效——消费瓶颈在共享 MySQL（事务 fsync），
不在消费者并行度。印证课程预期「不盲目增加实例」；
且比预期更深刻：瓶颈随优化迁移到数据库。

### 场景 C：反压验证（水位 30000 + Kafka 停 90s + 真实下单 200 TPS）

| 指标 | 实测 |
| --- | ---: |
| 积压封顶 | 30227（≈ 水位 30000 + 采样滞后） |
| 拒绝数 | 27933（503 backlog_limited，与 orders_rejected_backlog_total 精确一致） |
| 输入-受理对账 | 72001 = 受理 + 27933 拒绝（拒绝不产生订单/事件） |
| 恢复 | Kafka 恢复后 PENDING 30201→0 排空；水位回落自动恢复受理（201） |

### 场景 D：失败可追踪

- 毒丸（Kafka 注入非 JSON 消息）：显式 `parse event, skipping` 日志 +
  `consumer_process_errors_total{op=parse}` 计数，位点照常提交不阻塞链路；
- Outbox 毒丸进不去（payload 列 JSON 类型约束，DB 层防护）——毒丸只能来自
  Kafka 侧，parse 跳过路径是正确归宿；
- 中断重试：位点回退重读 5.7 万条（dup 风暴）后自动收敛 lag=0，副作用零重复。

### 完成延迟（正常态 200 TPS 下单，17459 样本）

- **P50 203ms / P99 538ms**（SLO ≤ 5s，EXP-09 实测 29.2s → 54 倍改善）。

### 最终对账

outbox SENT 770637 = inbox 770637 = notif 770637；dup 0/0；Kafka lag=0。

## 14. Root Cause

**实验期间修复的 5 个问题**（按发现顺序）：

1. **真实下单无法制造突发**：2000 TPS 下单被 P1 单行锁物理限制（实际到达
   ~163 req/s，15% 失败）。按课程口径改用回放工具（`cmd/event-replay`）——
   独立数据集直接注入 Outbox，负数订单域与 replay- 用户前缀与真实业务隔离。
2. **Kafka liveness 探针高负载误杀**：回放 12 万事件时 broker-api-versions
   响应 >5s，kubelet SIGTERM（Exit 143）反复重启。放宽探针（60s/15s/3 次失败）。
3. **逐条位点提交是串行点**：3 分区交错完成使连续段常仅 1-2 条，每段一次
   Kafka 往返（实测提交 3310 次/4616 条、每条 74ms）。改为 100ms 攒批合并提交
   （平均 19.7 条/批，提交延迟 ≤100ms 由 Inbox 兜底）。
4. **逐条事务的 fsync 串行墙**：8 并发下消费仅 217 msg/s（与串行相当）——
   每事务 1 次 fsync（VM 磁盘 ~2-5ms）封顶 ~250/s。改为批量事务
   （批 50 一个事务），吞吐 550-650 msg/s。
5. **Kafka auto.create.topics 默认开启**：consumer 先于 relay 连接时 broker
   按默认 1 分区自动建主题。禁用自动建主题，主题只能由 relay 按
   KAFKA_PARTITIONS 显式创建；ensureTopic 增加分区数不足报错（分区只增不减）。

## 15. Trade-offs

- **并发换吞吐**：放弃 EXP-09 的「单 goroutine 全序」，保留「位点提交不越过未完成
  消息」的 watermark。副作用 INSERT-only + 全局 Inbox 去重使并发安全；
  若未来后置任务有顺序依赖（如状态机迁移），必须回退到分区内保序消费。
- **水位是软预算**：采样滞后 500ms 允许少量超调；水位值需覆盖正常突发峰值
  （本实验 20 万 ≈ 突发 12 万 + 安全边际），否则正常突发误伤。
- **分区只增不减**：3 分区为固定实验值；扩容分区会破坏「分区内按 key 有序」的
  弱保证（本实验不依赖跨分区顺序）。分区重分配（再平衡）期间消费短暂中断。
- **保留期删段 vs 事件完整**：磁盘预算满时 Kafka 删最老段——已消费事件可删；
  未消费事件的真相在 Outbox（可重放），Kafka 是「可重建的投递缓存」。

## 16. Architecture Decision

- **ADR-020**：消费者并发模型定为「有界在途 + watermark 顺序位点提交」。
  分区内消息无业务顺序依赖（INSERT-only 副作用 + 全局 Inbox 去重），
  并发 worker 只影响处理延迟不影响正确性；位点提交保持保序语义。
- **ADR-021**：积压治理双预算——下单侧高水位反压（503 明确拒绝）+
  Kafka 保留期/磁盘预算（先到先删）。积压水位是业务预算，磁盘保留是物理预算，
  两者独立成立：任何一侧触顶系统仍可解释、可恢复。

## 17. Lessons Learned

- **吞吐瓶颈会迁移**：逐条 fsync 墙（~250/s）→ 批量事务后 Kafka 提交往返成为
  串行点 → 合并提交后 MySQL 成为最终墙（550-650/s）。每一层优化后都要重测，
  不能沿用上一次的瓶颈结论。
- **扩展性实验必须找对瓶颈**：2/3 实例无收益的根因是共享 MySQL，不是分区分配。
  课程预期「分区限制并行度」在本实验表现为更根本的「DB 事务吞吐限制」——
  加实例前先确认瓶颈在哪一层。
- **真实流量造不出突发，回放才是正解**：下单接口被 P1 行锁限制在 ~200 TPS，
  用真实下单测后置任务突发永远测不到上限。课程「回放不计入真实 TPS」的口径
  是实验可行性的关键；回放用独立数据域（负数 order_id）保持对账干净。
- **k8s 探针不能在高负载下误杀**：liveness 的目的是检测进程死亡，不是负载诊断；
  重负载组件（Kafka）的探针必须足够宽松，否则探针本身成为故障源。
- **幂等键跨轮复用会污染压测**：k6 `__ITER` 每轮从 0 开始，同脚本多轮运行
  大量重放（200），造成「请求全成功但零新订单」的假象。压测轮次之间必须
  更换幂等键前缀或数据域。
- **批量 + 幂等是安全的组合**：批量事务（INSERT IGNORE 批）与 Inbox 去重天然
  兼容——批内部分重复不影响新事件计数（RowsAffected），整批失败重试幂等。

## 18. Interview Questions

- 并发消费下「位点提交不跳过未完成任务」如何保证？
  → 每分区 watermark：只有从待提交 offset 起的连续完成段才提交。消息乱序完成时，
  完成的后续消息进入 done 集合等待；任一消息失败重试会拦住整个分区水位，
  重启后从旧位点重读，Inbox 去重保证副作用唯一。
- 为什么用「有界队列」而不是「无限缓冲」？
  → 无限缓冲等于把 Kafka 的积压搬进进程内存，DB 变慢时内存先爆。
  有界队列满 → 暂停 fetch → 消费压力随下游自动收缩，积压留在 Kafka/Outbox
  （有磁盘预算），进程内存恒定。
- 为什么 3 分区而不是 12 分区？分区多了有什么代价？
  → 分区是并行度上限：单实例多分区增加 fetch 复杂度与位点管理面；
  分区数必须 ≥ 实例数才有横向扩展意义（本实验 3 实例对照）。
  更多分区提升极端并行度，但增加 Broker 文件句柄与再平衡成本。
- 积压高水位为什么拒绝「新下单」而不是「暂停 Relay」？
  → 暂停 Relay 后 Outbox 继续增长，只是把积压从 Kafka 搬到 MySQL——没有减少总量。
  拒绝新输入才是真正的反压终点：预算触顶时停止增长，存量排空后恢复。
- 突发 12 万事件的积压峰值如何估算？实测偏差从哪来？
  → 峰值 ≈ 输入总量 - 消费速率 × 突发时长（若消费与输入并行）=
  12 万 - 500/s × 60s = 9 万。偏差来源：消费速率非恒定（group commit 波动）、
  采样粒度、重试放大。实测与模型对照是场景 A 的核心证据。
- Kafka 磁盘保留期与 Outbox 重放的关系？
  → Kafka 删段只影响「未消费消息」，其真相在 Outbox（SENT 记录可重放）。
  已消费消息删段无损。这要求 Outbox 保留期 ≥ Kafka 保留期（课程：Outbox 保留
  至 DR 验收结束）。
