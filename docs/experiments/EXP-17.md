# EXP-17：Kafka 高可用与事件完整性

## 1. 实验目标

- 把单 Broker Kafka 升级为 **3 Broker / 3 KRaft Controller**（combined 模式）实验拓扑，
  主线 Topic `orders` 复制因子 3、`min.insync.replicas=2`，生产者 `acks=all`；
- 持续订单事件流中**强杀承载分区 Leader 的 Broker**：记录 ISR 收缩、Leader 切换、
  生产失败/重试、消费 Lag 与恢复，验证不丢事件、不产生重复副作用；
- 单独验证 **Controller 多数派条件**（1 个 Controller 中断仍可用、2 个失守则
  集群元数据操作停摆——明确不可用而非假成功）；
- 事件 ID 对账 Outbox → Kafka 消费 → 副作用，全程 1:1:1。

## 2. 背景问题

EXP-09 建立的单 Broker Kafka 一直以「事件真相在 Outbox、Kafka 可丢可重放」为
前提运行（EXP-09/10 验证过 Kafka 不可用时下单不受影响）。但后置任务完成延迟
（5s SLO）依赖 Kafka 可用，单 Broker 是后置链路的单点：

1. **Broker 故障的行为从未实测**：分区 Leader 死亡时 `acks=all + min.isr=2` 的
   生产语义（阻塞？重试？失败？）、ISR 收缩与恢复、消费组重平衡耗时——
   只有单点拓扑时这些都无从谈起；
2. **KRaft Controller 是新单点**：单节点 Controller（当前 kafka-0 兼任）死亡
   = 集群无元数据大脑，任何 Leader 选举都无法进行；
3. **EXP-16 的教训直接适用**：Router 的「启动路径单点」在 Kafka 侧对应
   「Controller quorum 单点」；多副本拓扑必须验证多数派语义。

## 3. HA 拓扑（本实验冻结，HA 模拟档）

```text
outbox-relay（acks=all）──┐
                          ├── kafka-hs（headless）── kafka-i × 3
consumer（group）─────────┘   （KRaft combined：每进程 broker+controller）
                                 NODE_ID = i+1（1/2/3）
                                 quorum voters = 3 成员 9093
                                 client listener 9092（advertise: kafka-i.kafka-hs...）
                                 每实例独立 PVC 2Gi
order-api ── 不直连 Kafka（Outbox 同事务，无变化）

Topic orders：partitions=3（EXP-10 冻结）、RF=3、min.insync.replicas=2
故障域：单机（HA 模拟档，同 EXP-16 声明）
```

### 资源预算（在 EXP-16 HA 档之上）

| 组件 | 变更 | 预算 |
| --- | --- | ---: |
| kafka-0（单实例） | 退役（PVC 保留存档） | -250m/-512Mi |
| kafka-i × 3 | 新增 | 3 ×（250m req / 750m limit；512Mi req / 1.5Gi limit；heap 512m；PVC 2Gi） |
| 节点合计 | | ~5.1/6 核 request（85%），内存 limit 超额但实际占用 ~60% |

### 关键配置（冻结）

```text
KAFKA_PROCESS_ROLES: broker,controller（combined，资源受限档；分离部署为选做）
KAFKA_CONTROLLER_QUORUM_VOTERS: 3 成员 @9093
KAFKA_DEFAULT_REPLICATION_FACTOR=3 / KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3
KAFKA_MIN_INSYNC_REPLICAS=2（broker 默认；topic 创建时显式置 3/2）
producer（relay，kafka-go 已有）：RequiredAcks=RequireAll ✓
rebalance: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0（沿用）
retention: 24h / 1.5GiB per broker（EXP-10 预算沿用）
```

## 4. SLA / SLO（冻结）

| 指标 | 目标 |
| --- | ---: |
| Leader Broker 强杀：下单可用性 | 不受影响（Outbox 同事务，EXP-09 语义） |
| Leader 强杀：事件投递 | 阻塞 ≤ 30s（acks=all 等待 ISR 选举），恢复后自动排空 |
| Leader 强杀：事件完整性 | 0 无法解释的遗漏 / 0 重复副作用（Inbox 去重） |
| 消费恢复 | Broker 选举 + 重平衡 ≤ 60s，Lag 排空 |
| Controller 1/3 中断 | 集群继续可用（quorum 2/3） |
| Controller 2/3 失守 | 元数据操作明确不可用；生产不返回假成功（PENDING 重试）；恢复后自动排空 |
| 事件完成延迟 P99（正常态） | ≤ 5s（EXP-09 口径，HA 档复测） |
| 对账 | outbox SENT == inbox == notif；事件 ID 三段可追踪 |

## 5. Baseline（EXP-16 末态）

- HA 档基线 600 rps（读 p99 77ms / 写 p99 337ms / 失败 0.001%）；
- 事件链路：sent == inbox == notif = 45059，lag=0；
- 单 Broker 投递延迟（EXP-09）：P50 336ms / P99 1.97s（200 TPS 稳态）。

## 6. Hypothesis

1. 3 副本 + acks=all + min.isr=2 下，Leader Broker 强杀：生产在 ISR 选举期间
   阻塞/重试（≤ 30s），完成后恢复投递；**已确认事件不丢**（RF=3 的 ISR=2
   副本承接 Leader）；
2. min.isr=2 在 3 副本下允许 1 个副本故障仍可写；若再杀 1 个（同分区 ISR=1
   <2），生产**明确失败**（NotEnoughReplicas）→ relay 保持 PENDING →
   下单不受影响、恢复后排空——「不可用而非丢数据」；
3. 消费组在 Broker 选举后 ≤ 60s 完成重平衡并追平 Lag；
4. Controller 1/3 死亡不影响可用性（quorum 满足）；2/3 失守时集群无法进行
   Leader 选举，新分区写入受阻，但已 ISR 饱和的分区……（实测观察边界并如实记录）；
5. 全程事件 ID 对账 1:1:1，重复投递（选举期间未确认消息重发）被 Inbox 去重，
   无重复副作用（EXP-16 的 INSERT IGNORE 修复是前置条件）。

## 7. 实验方案

1. **场景 G1（拓扑迁移与基线）**：部署 kafka-i ×3 → 重建 topic（RF=3/min.isr=2）
   → relay/consumer 切换 → 600 rps × 3min 基线（投递延迟 P99、完成延迟 P99、
   lag、1:1:1）。旧单实例 kafka-0 退役（scale 0，PVC 存档）。
2. **场景 G2（Leader Broker 强杀）**：600 rps 持续下单事件流中，定位
   orders-0 分区 Leader 所在 Broker → force delete Pod → 采样：
   relay 投递速率（relay_sent_total）、Kafka ISR/Leader
   （kafka-topics --describe）、consumer lag、恢复时刻；
   对账：窗口内事件无遗漏、无重复副作用。
3. **场景 G2b（min.isr 压力，可选深入）**：同分区再杀第二副本 → 生产明确
   NotEnoughReplicas → PENDING 积压 → 恢复 → 排空（验证「明确失败」语义）。
4. **场景 G3（Controller 多数派）**：杀 1 个 Controller 进程（docker kill，
   Pod 保留）→ 集群仍可用；再杀第 2 个 → quorum 失守 → 观察生产/消费/元数据
   操作行为 → 恢复 → 排空。
5. **场景 G4（对账）**：全程 outbox/inbox/notif 1:1:1 + 事件 ID 抽查三段追踪
   （Outbox payload → Kafka header event_id → inbox event_id）。

## 8. Code Change

- `internal/mq/kafka/kafka.go`：`ensureTopic` 增加 topic 级配置
 （RF 由参数传入；`min.insync.replicas=2` 显式设置，不依赖 broker 默认）；
  Producer 无改动（RequiredAcks=RequireAll 已有，EXP-09 冻结）。
- 其余无代码变更（relay at-least-once + Inbox 去重 + EXP-16 INSERT IGNORE
  修复直接承载故障语义）。

## 9. Kubernetes Change

- `deploy/k8s/ha/kafka-kraft.yaml`（新增）：
  - Headless Service `kafka-hs`（quorum + per-broker DNS）；
  - StatefulSet `kafka-i`（replicas 3，apache/kafka:3.9.0，combined 角色，
    NODE_ID 按序号，PVC 2Gi/成员，retention 预算沿用）；
  - Service `kafka`（client 9092 → 副本轮询）。
- `deploy/k8s/base/all.yaml`：relay/consumer 的 `KAFKA_BROKERS` 切换到新集群
  bootstrap（`kafka.kafka-hs` 成员列表）；旧 kafka STS scale 0。

## 10. Load Test

- `tests/load/fixed-rate.js`：600 rps × 8:1:1（HA 档基线口径）；
  G2/G3 全程持续（5min/场景），观察事件链路而非 HTTP 层（HTTP SLO 由
  EXP-16 保证，本实验关注投递/消费时序）。

## 11. Failure Injection

- Leader Broker 强杀：`kubectl delete pod kafka-i-<leader> --force --grace-period=0`
- 第二副本强杀（G2b）：同法
- Controller 进程暴亡：`docker kill -s KILL`（Pod 保留，容器自动重启——
  与 EXP-14 A3 同路径；重启后 controller 重新入 quorum）
- 恢复：STS 重建 / 容器重启后自动 rejoin

## 12. Observability

- Kafka 侧：`kafka-topics --describe`（ISR/Leader 逐分区）、
  `kafka-metadata-quorum --status`（controller quorum）；
- relay：`relay_sent_total` / `relay_batch_*` / Outbox PENDING 水位与告警
  （OutboxRelayNotDelivering 已配置）；
- consumer：`consumer_processed_total` / lag（kafka-consumer-groups）；
- 完成延迟：notification created_at - order created_at 分布（SQL 抽样）；
- 对账：1:1:1 + 事件 ID 抽查。

## 13. Results

实测环境：Rancher Desktop k3s（单机 6 核/16GB，HA 模拟档）。Kafka KRaft
combined ×3（apache/kafka:3.9.0，NODE_ID 1/2/3，每实例 PVC 2Gi），MySQL 侧
沿用 EXP-16（InnoDB Cluster + Router）。topic `orders`：3 分区 / RF=3 /
min.isr=2（relay 显式创建），relay `acks=all`（EXP-09 冻结）。

### 部署历程（两个 K8s 级缺陷，均在手册记录）

| 问题 | 现象 | 修复 |
| --- | --- | --- |
| OrderedReady 死锁 | kafka-i-0 独自等待 quorum（2/3）无法 Ready → STS 永不创建后续成员 → UnknownHostException 循环 | `podManagementPolicy: Parallel` |
| Headless DNS 鸡生蛋 | headless Service 默认只发布 Ready 端点 → 成员在就绪前无法互相解析 → quorum 无法成立 | `publishNotReadyAddresses: true` |
| 同名 Service 覆写 | base/all.yaml 旧 kafka 的 `kafka-hs`/`kafka` 与 HA 清单同名——apply 把 selector 覆写回旧标签 → 新集群 DNS 全 NXDOMAIN | 从 all.yaml 移除旧 Service 定义（同名即冲突，多清单管理纪律） |

### 场景 G1：HA 档事件链路基线（600 rps）

| 指标 | 实测（12min 窗口，n=11566） | 对照单 Broker（EXP-09） |
| --- | ---: | ---: |
| 投递延迟（Outbox→Kafka 确认） | avg 0.47s / p99 1.29s / max 1.92s | P50 336ms / P99 1.97s |
| 完成延迟（订单→通知） | avg 0.50s / **p99 1.34s** / max 2.08s | p99 29.2s（EXP-09 保序）→538ms（EXP-10） |
| 完成延迟 SLO（≤5s） | **✓ 通过**（p99 1.34s） | — |
| 1:1:1 / lag | 50056=50056=50056，lag=0 | — |

- HTTP 层在冷缓存窗口出现回源风暴（TTL 600s 过期 + qemu Router 慢回源，
  EXP-16 F1 已知边界），与本实验的事件链路目标正交；预热后 0.57% 失败。

### 场景 G2：Leader Broker 强杀（600 rps × 300s，T+60s 杀 kafka-i-0）

kafka-i-0 同时是 orders-0 分区 Leader **和** KRaft controller leader——单次
注入同时演练数据面与控制面故障。

| 阶段 | 时刻 | 证据 |
| --- | ---: | --- |
| kill | T+0 | `delete pod kafka-i-0 --force --grace-period=0` |
| 分区 Leader 切换 | **T+20s 内** | orders-0：Leader 1→2；ISR=2,3,1（被杀 Broker 重建后回 ISR） |
| Controller leader 切换 | 同窗口 | quorum describe：LeaderId 1→**3** |
| 投递影响 | 瞬时 | PENDING 采样全程 ≤15 条（批内 in-flight 重试），无积压 |
| 下单影响 | **零** | orders 增速全程 ~55/s 不变（EXP-09 语义：Outbox 同事务） |
| HTTP 失败率 | 3.98%（含重建窗口 CPU 争用） | — |
| **对账** | | **66506 四表 1:1:1、PENDING=0、0 孤儿——零丢失零重复** |

### 场景 G2b+G3：双 Broker 强杀（数据面 ISR 崩 + 控制面 quorum 失守）

600 rps 负载中 T+45s 强杀 kafka-i-1 与 kafka-i-2（剩 1/3 成员）：

| 观测 | 证据 |
| --- | --- |
| **明确拒写**（非假成功） | relay 日志：`[19] Not Enough Replicas: the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1`——min.isr=2 语义生效 |
| 元数据停摆 | consumer 日志：`[16] Not Coordinator For Group`、`[15] Group Coordinator Not Available`——controller 多数派失守，协调者不可用 |
| at-least-once 自愈 | 失败批次保持 PENDING（瞬时 658 条），STS 重建后 T+37s 排空至个位数 |
| 下单零影响 | orders 全程 ~55/s 增长（HTTP 失败 1.39%） |
| **终局对账** | **76713 四表 1:1:1、PENDING=0、lag=0、0 孤儿、0 broker 重启风暴** |

- **恢复速度的关键**：STS 的秒级 Pod 重建把「持续多数派失守」压缩成
  ~30s 瞬态；真正的持续失守（如三机房同时挂二）行为等同于窗口期语义——
  明确拒写 + PENDING 积压 + 恢复后自动排空，与 EXP-10 反压水位模型衔接。

### 场景 G4：事件 ID 三段追踪（抽查）

- Outbox `payload.event_id` == Kafka 消息 key/header `event_id` ==
  Inbox `event_id`（EXP-09 建立的三段贯穿在 HA 集群上不变）；
- 全程 1:1:1 对账含所有故障窗口：76713（G1+G2+G2b 累计）。

## 14. Root Cause

- **「明确拒写」的机制**：acks=all 下 broker 只在 ISR≥min.insync.replicas 时
  确认写入；双杀后 ISR=1 <2 → broker 返回 NOT_ENOUGH_REPLICAS 而非接受写入
  ——**不可能出现「返回成功但未持久化」的假成功**，这正是 EXP-17 的核心命题。
- **controller 多数派失守的边界**：KRaft quorum 2/3 失守后，与元数据相关的
  能力停摆（coordinator 选举、offset commit、partition leader 变更），但
  **已建立的分区若 ISR 满足仍可读写**（本实验双杀后无幸存分区满足 min.isr，
  故全部拒写）；下单链路完全不受影响（Outbox 同事务，不依赖 Kafka 可达）。
- **恢复主导项是 STS 重建速度**：Pod 重建 ~10-20s + broker 启动 ~15-25s +
  rejoin ISR——数据面恢复在 T+30-40s 完成，与 GR（EXP-16）不同点在于
  Kafka 的控制器选举与新 leader 注册是元数据操作，需要 quorum 先恢复。
- **DNS 是 KRaft on K8s 的隐形依赖**：quorum 成员互连发生在「就绪前」，
  headless Service 必须发布未就绪地址；且多清单同名 Service 会互相覆写
  selector——k8s 的「最后 apply 者赢」对共享命名的资源是破坏性的。

## 15. Trade-offs

- **combined 模式（broker+controller 同进程）**：省内存（3 JVM 而非 6），
  代价是数据面故障必然伴随控制面成员损失（G2 单杀即演练）——资源受限档
  接受；分离部署（Controller 独立 3 实例）是生产推荐形态，本课程选做。
- **min.isr=2 / RF=3**：可用性上限 = 容忍 1 副本故障可写、2 副本故障明确
  拒写。若 min.isr=1 可换「2 故障仍可写」但重新引入单副本持久化风险——
  roadmap 明令禁止的「用非同步副本强行选主」同类取舍。
- **STS 快速重建 vs 真实多数派失守**：本环境 Pod 重建 ~30s，使双杀成为
  瞬态故障；真实场景（机房级）重建以分钟/小时计，行为语义相同（拒写+
  积压+排空），但 Outbox 积压会逼近 EXP-10 反压水位（200000），该水位是
  最终的保护层。
- **Kafka 事件不迁移**：新集群从空 topic 开始，历史事件存档在旧 PVC——
  事件真相在 Outbox 的架构决定（EXP-09）让 Kafka 集群替换成为「无迁移
  切换」；代价是新旧集群的 consumer offset 语义断代（本实验消费组从
  FirstOffset 起新，对账从 45059 起算已收敛）。

## 16. Architecture Decision

- **ADR-036**：Kafka HA 采用 KRaft combined ×3 + topic 显式 RF=3/min.isr=2
  + relay acks=all；headless Service 必须 `publishNotReadyAddresses: true`、
  STS 必须 `podManagementPolicy: Parallel`（KRaft on K8s 引导约束）。
  多副本投递语义（min.isr）落在 topic 配置而非 broker 默认。
- **ADR-037**：共享名资源（Service/ConfigMap）在多 YAML 清单间禁止同名
  定义——同名 apply 相互覆写（EXP-17 实测：selector 被覆写导致全集群
  DNS 失效）。退役组件的清单定义必须同步删除。

## 17. Lessons Learned

- **「不丢不重」是三层机制叠出来的**：acks=all+min.isr（broker 层明确
  确认）→ relay PENDING 重试（at-least-once）→ Inbox 去重 + INSERT IGNORE
  （EXP-16 修复）——每层都验证过失败路径，缺任何一层故障注入都会露馅。
- **KRaft on K8s 的两个引导死锁都有标准解**：Parallel + publishNotReady
  ——任何「成员互连先于就绪」的集群（etcd/ZooKeeper/consul 同理）都需要
  这两个开关，值得写进个人 checklist。
- **故障注入要杀得准**：杀 kafka-i-0 一个 Pod 同时命中分区 Leader 与
  controller leader——combined 模式下「数据面故障=控制面故障」是常态
  而非巧合，演练设计时必须意识到两种角色的叠加。
- **Outbox 让「换 Kafka 集群」变成运维操作而非数据项目**：事件真相在
  MySQL，Kafka 可重建可替换；反代价是 relay 的持续轮询开销与 EXP-10
  的积压预算——这套取舍在 HA 场景下再次证明正确。
- **对账数字是唯一的裁判**：G2b 双杀后 76713 四表 1:1:1——无论中间过程
  多混乱（NotEnoughReplicas/Coordinator 不可用/重平衡），终点对账通过
  才算通过。

## 18. Interview Questions

- acks=all + min.insync.replicas=2 在 Leader 死亡时保证了什么？没保证什么？
  → 保证：确认过的写入至少在 2 副本持久化，Leader 死亡不丢已确认消息；
  不保证：写入「不阻塞」——ISR 选举期间生产阻塞/重试（本实验瞬时
  PENDING ≤15）；也不保证延迟不变（重试叠加）。若 ISR<2，宁可
  NOT_ENOUGH_REPLICAS 拒写也不降级确认——「可用性让位于持久性」。
- 为什么杀两个 ISR 副本是「正确失败」的验证？NotEnoughReplicas 后链路如何自愈？
  → 双杀把 ISR 压到 1<2，broker 拒绝 acks=all 写入 → relay 收到明确错误、
  批次保持 PENDING → 下单不受影响（Outbox 同事务）→ STS 重建 → ISR 恢复
  → relay 下一轮自动排空。自愈的每一环都是已有机制（EXP-09/10），
  HA 只是让它们第一次在「真故障」下运行。
- KRaft Controller quorum 失守时集群的哪些能力停摆、哪些仍在？
  → 停摆：新 leader 选举/注册、coordinator（offset commit、消费组管理）、
  topic 配置变更——consumer 报 Coordinator Not Available。
  仍在：无需元数据的已有连接的分区读写（若 ISR 幸存≥min.isr）。
  双杀场景下幸存分区 ISR=1 → 全部拒写——「明确不可用」。
- 消费组重平衡在 Broker 故障中扮演什么角色？位点提交的时机约束是什么？
  → Broker 故障 → 分区 Leader/coordinator 迁移 → 消费组重平衡重新分配
  分区。位点提交约束（EXP-10 冻结）：watermark 之前不允许越过未完成
  消息——coordinator 不可用时 commit 失败、位点不前进，恢复后从上次
  提交点重消费，重复投递由 Inbox 去重吸收。
- 事件 ID 如何把 Outbox / Kafka / Inbox 三段串起来做对账？
  → 事务内生成 UUID 为 event_id：Outbox payload 字段 == Kafka key/header ==
  Inbox 主键。生产侧重发不改 ID（同一行重投），消费侧同 ID 第二次插入
  1062 跳过副作用。三段任意一段的丢失/重复都会在对账（1:1:1 + ID 抽查）
  中暴露——故障注入后的对账不是仪式，是判定性证据。
