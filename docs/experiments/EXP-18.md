# EXP-18：独立备份恢复与跨组件对账

## 1. 实验目标

- 在**持续负载**下对 InnoDB Cluster 主库做可验证的一致性备份（mysqldump
  `--single-transaction --source-data=2 --set-gtid-purged`）＋ binlog 归档，
  产物全部落**故障域外**（宿主机），并以客户端成功响应账本逐单记录业务真相；
- 模拟「原数据环境不可用」：隔离 order-lab 数据层（mysql-ic ×3 + router +
  应用层全部冻结），在**新 Namespace / 新 PVC** 上从备份恢复业务；
- 两档恢复对照：**仅全量备份**（实测 RPO = 备份点后丢失清单）vs
  **全量 + binlog PITR**（GTID 自动跳过重放，目标 RPO = 0）；
- 以恢复后的订单与保留的 Outbox 为权威，在**隔离 Topic + 新消费组**中重放
  未完成事件（保留原 event_id，复用恢复的 Inbox 去重）；
- 全程分层计时 RTO，跨组件核验订单/库存/幂等/Outbox/Inbox/副作用。

## 2. 背景问题

EXP-16/17 验证的是**在线故障转移**（多数派存活、原卷原库），但「原环境整体
不可用」（误删、数据损坏、卷级故障、机房级事件）只能靠**备份恢复**：

1. **备份从未被验证过**：仓库里没有任何备份机制；binlog 虽默认开启（GR 前提，
   gtid_mode=ON），但从未归档到故障域外；
2. **「恢复」≠「重启」**：新 Namespace、新卷、空 gtid_executed 的目标实例
   上，schema（GORM AutoMigrate 在 order-api 启动时建表）、数据、GTID 状态、
   事件链路（Outbox/Inbox/Kafka 位点）要按依赖顺序逐层重建；
3. **RPO 与 RTO 必须实测**：只有全量备份时，备份点之后的数据是恢复不了的；
   roadmap 明确要求 RPO 目标小于备份间隔时必须 binlog 归档/PITR——这两档
   的真实代价与结果要分别拿到数字。

## 3. 冻结目标（实验前登记）

| 目标 | 数值 | 口径 |
| --- | --- | --- |
| RTO | ≤ 30 分钟 | T_FAIL（原环境隔离）→ DR 环境 order-api 冒烟通过（读+写+幂等） |
| RTO（事件链路） | ≤ 30 分钟 | → 重放排空、Outbox/Inbox/通知 1:1:1 |
| RPO（全量档） | 备份点之后的写入全部丢失 | 账本逐单核验，输出丢失清单（结果而非目标） |
| RPO（PITR 档） | 0（已提交事务零丢失） | 账本 strict 核验 lost=0 且 GTID 与源库故障前对齐 |
| 正确性 | 库存差分守恒、幂等无孤儿、事件三段可追踪 | reconcile 全项 PASS |
| 负载 | 600 rps × 600s（8:1:1，LEDGER=1） | HA 档基线口径 |

## 4. 备份与恢复方案（主线冻结）

```text
【备份侧——负载持续写入中】
mysqldump --single-transaction --source-data=2 --set-gtid-purged=ON
         --hex-blob --triggers --databases flash        → T_b 一致性快照
  · --single-transaction：InnoDB REPEATABLE READ 一致性快照
  · --source-data=2：记录 binlog 文件+位点（CHANGE MASTER TO 注释）
  · --set-gtid-purged=ON：导入时还原 gtid_purged（PITR 跳过基准）
binlog 归档：tar 全部 binlog 文件 → 宿主机（T_b 一次、T_f 静默后一次）
账本：k6 LEDGER=1 逐单 console.log → nohup 日志 → 提取到宿主机

【恢复侧——新 Namespace order-dr、新 PVC、GTID ON】
全量档：导入 dump → reconcile（水位+守恒+账本）→ order-api → 冒烟 → RTO-full
PITR 档：导入 dump → 宿主机 mysqlbinlog 解析归档 | mysql（kubectl stdin）
         → GTID 自动跳过已执行事务、只补 (T_b, T_f] 增量 → strict 账本 → RTO-pitr
重放：replay-scope（PENDING ∪ SENT未消费）→ SQL 重置 PENDING（保留原
       event_id）→ relay（隔离 topic orders-dr-replay，RF=3/minISR=2）
       → consumer（每恢复代一个新消费组，FirstOffset 起）
       → Inbox 去重吸收重复投递 → 1:1:1
```

### 为什么 GTID 重放是「自动跳过」

dump 导入后目标实例 `gtid_executed = gtid_purged = G_b`；binlog 归档覆盖
`[G_0, G_f]`。重放时 MySQL 对已在 gtid_executed 中的 GTID 事务**静默跳过**，
只有 `(G_b, G_f]` 的增量真正执行——无需手工对位点，位点对齐由 GTID 集合
运算保证（前提：dump 的 gtid_purged 与其快照内容精确对应，mysqldump 在
`--source-data` 锁内完成，本实验实测对齐）。

## 5. Baseline（EXP-17 末态）

- 四表 1:1:1 = 76713（EXP-17 G4 终局）；P1 库存 925109＋订单 76713 = K 1001822
  （守恒常数，差分法不依赖绝对初始值——实验期间多次补过库存）；
- HA 档基线 600 rps（读 p99 77ms / 写 p99 337ms / 失败 0.001%）。

## 6. Hypothesis

1. 仅全量备份恢复：数据水位停在 T_b，T_b 之后的账本订单全部 lost（实测即 RPO），
   库存差分守恒仍成立（守恒在任意两个自洽状态间都应成立）；
2. 全量 + binlog PITR：恢复水位 = T_f（GTID 对齐），账本 strict 核验 lost=0；
3. 隔离 Topic + 新消费组重放：原 event_id 不变 → 恢复的 Inbox 主键去重吸收
   重复投递，无重复副作用、无孤儿事件；
4. RTO 主导项是 binlog 重放时长与镜像拉起/建库，远低于 30 分钟目标；
5. 备份在负载中有真实代价（qemu Router + 128M buffer pool 下 dump 扫表
   挤占热点页），但正确性不受影响。

## 7. 实验方案

| 场景 | 内容 | 验收 |
| --- | --- | --- |
| R1 | 600rps 负载中 T_b 全量备份 + binlog 归档 #1 + 守恒基线快照 | dump 完成标记/gtid_purged/位点可验证 |
| R2 | 负载结束静默后 binlog 归档 #2（T_f）+ 账本提取 → 隔离原环境（mysql-ic/router/order-api/relay/consumer 全部 scale 0） | 备份与账本均在故障域外；RTO 计时开始 |
| R3 | 新 ns order-dr 全量恢复 → 对账（损失清单）→ 受控恢复写入 → 冒烟 → 重放 → 1:1:1 | RTO-full、RPO=lost 清单 |
| R4 | reset 重来：全量 + PITR → GTID 对齐 → strict 账本 → 受控顺序恢复（api → relay → 新组 consumer） | RTO-pitr、lost=0 |
| R5 | DR 环境清理；原环境恢复（GR 全灭重启引导）+ 终局对账 | 原环境 17705 verified / lag=0 |

## 8. Code Change

- `cmd/reconcile`（新增）：跨组件对账器。三种模式：
  - `snapshot`：事务内（REPEATABLE READ 一致快照）采集全局计数 + 每 SKU
    订单/库存水位 + GTID，输出 JSON 基线；
  - `check`：7 项集合一致性（orders↔outbox↔inbox↔notif、幂等孤儿、DEAD）+
    库存差分守恒（Δorders+Δstock=0，不依赖初始库存绝对值）+ 账本逐单核验
    （批量载入 k6 用户域订单/幂等行内存比对；输出 lost/mismatch/在窗未确认；
    `-strict-ledger` 时 lost>0 判 FAIL）；
  - `replay-scope`：输出重放范围（PENDING ∪ SENT 未消费）与重置 SQL。
  - 设计要点：**账本 lost 是 RPO 的测量结果而非对账失败**（全量档预期有 lost）；
    「在窗未确认」（DB 有、账本无）是「提交成功但响应丢失」的合法状态
    （客户端以原幂等键重试收敛），报告为信息项。
- `tests/load/fixed-rate.js`：新增 `LEDGER=1` 开关——成功下单响应（200/201）
  逐单 `console.log("LEDGER|id|user|sku|key|ts")`，经 nohup 日志落盘、事后
  提取到宿主机；200（幂等重放）与 201（首发）按 order_id 去重。
- relay/consumer **零代码变更**：`KAFKA_TOPIC`/`CONSUMER_GROUP` 环境变量
  （EXP-09/10 已有）直接承载「隔离 Topic + 新消费组」重放语义。

## 9. Kubernetes Change

- `deploy/k8s/dr/mysql-dr.yaml`（新增）：DR 恢复栈（ns order-dr）——
  单实例 MySQL（新 PVC 3Gi，`--gtid-mode=ON --binlog_checksum=NONE`，binlog
  8.0 默认开启）、order-api（`CACHE_ENABLED=false`，DR 档读直连 DB）、
  relay（topic=orders-dr-replay，跨 ns 复用 kafka-i）、consumer；后三者初始
  replicas=0，由 Runbook 按受控顺序逐步拉起（避免空库 AutoMigrate 先于
  数据导入、relay 提前消费空表）。
- `deploy/backup/backup.sh`（新增）：`full`（一致性 dump + 位点/元数据 +
  binlog 归档 + SHA256）/ `binlogs`（静默后刷新归档）/ `ledger`（从 k6 日志
  提取账本）；自动定位 GR PRIMARY（performance_schema.replication_group_members）。
- `deploy/backup/restore.sh`（新增）：`deploy` / `reset`（删 ns 含 PVC，
  保证「新卷恢复」）/ `import` / `binlog`（**宿主机 mysqlbinlog** 解析归档 →
  kubectl stdin 灌入）/ `status`。

## 10. Load Test

- `tests/load/fixed-rate.js -e RATE=600 -e DURATION=600s -e LEDGER=1`；
  热身 cache-warmup（10000 SKU）后启动；账本窗口 [T0, T_f]。

## 11. Failure Injection

- 「原环境不可用」= 计划内隔离：`kubectl scale sts/mysql-ic deploy/mysql-router
  deploy/order-api deploy/outbox-relay deploy/consumer --replicas=0`
  （PVC 保留不删——灾备验证不销毁唯一副本，roadmap §3.3）；
- kafka-i / redis / k6-load / 监控栈保留（独立故障域；Kafka 事件可重建，
  重放走隔离 Topic）。

## 12. Observability

- k6 汇总 + LEDGER 提取计数；Prometheus 历史查询（5xx 按状态码时间线、
  container_memory_working_set）；
- 恢复侧：restore.sh 各阶段输出（水位/GTID）、reconcile 全项、
  kafka-consumer-groups（成员分区数、lag）、事件三段抽查。

## 13. Results

实测环境：Rancher Desktop k3s（单机 6 核/16GB，HA 模拟档，EXP-17 末态）。
时间线（UTC）：T0=08:07:30 负载启动；T_b=08:08:41 备份点；T_f=08:17:30 负载
结束（88812 单、lag=0）；T_FAIL=08:33:52 隔离原环境。

### R1：备份建立（负载中）

| 产物 | 大小 | 关键内容 |
| --- | --- | --- |
| flash-full-*.sql | 70MB | 7 表全量；`SET @@GLOBAL.GTID_PURGED=...`；`CHANGE MASTER TO MASTER_LOG_FILE='binlog.000013', POS=88499734`；Dump completed 标记 |
| binlogs-*.tar（#1，负载中） | 203MB | 活跃 binlog 尾部撕裂警告（tar 读时文件在写）——中间产物 |
| binlogs-*.tar（#2，T_f 静默后） | 203MB | 干净归档，GTID ab69be63:1-167558 |
| ledger-r1.txt | 865KB | 17705 条成功响应（含幂等重放） |
| baseline-snapshot.json | — | 事务化快照：orders=80285、P1 K=1001822 |

### 意外事件：备份的代价（重要发现）

负载 + 备份并行期间服务明显劣化，且 secondary 被 OOM 杀死：

| 时刻 | 事件 | 证据 |
| --- | --- | --- |
| 08:08 起 | 5xx 开始（备份窗口：dump 扫表挤占 128M buffer pool + 203MB tar 污染页缓存 + qemu Router 放大） | Prometheus：504 峰值 77/s @08:12，503 峰值 179/s @08:17 |
| 08:14:40 | **mysql-ic-0（SECONDARY）OOMKilled**，Exit 137 | 内存曲线：实验开始前已 1500/1536Mi（EXP-16/17 遗留未释放），复制积压把它推过上限；恢复后 GR 2/3 重建 |
| k6 终局 | checks_failed 10.43%（读 96%、latest 66%、写 52% 接受）；dropped_iterations 25768；写 p95 1.46s | 阈值全破——**这是备份期资源代价 + 组件故障的如实记录，不是本实验验收口径**（正确性验收以账本/对账为准，本窗口零数据损坏） |

结论：**备份不是免费的**。资源受限档上，dump 扫表与 binlog 归档会挤占
buffer pool / 页缓存并放大到全链路；生产启示：备份应跑在专用副本、限速、
或用物理备份（XtraBackup）避免扫表污染。EXP-16「故障态内存预算 ≠ 正常态」
扩展为第三条预算纪律：**备份期资源预算 ≠ 空闲期**。

### R3：全量备份恢复（仅 dump）

| 阶段 | 时刻 | 耗时 | 结果 |
| --- | --- | --- | --- |
| mysql-dr 就绪（新 ns/新卷） | 08:34:40 | 48s | 空库、GTID ON |
| 导入 dump | 08:35:23→08:35:29 | **7s** | 水位=77684（T_b 快照）；1 条 PENDING（dump 时刻 relay 在途——at-least-once 正常态） |
| reconcile | ~08:36 | ~60s | 集合一致全 OK；**10000 SKU 差分守恒 PASS**；账本 verified=6577 **lost=11128** mismatch=0；在窗未确认=0 |
| order-api + 冒烟（读 200/写 201/同键重放 200） | 08:36:41 | — | **RTO-full = 2min49s** ✓（目标 30min） |
| 重放（范围=2：dump 在途 1 + 冒烟 1）→ 1:1:1 | ~08:44:30 | — | 77685 四表对齐，PASS；RTO-链路 ≈ 10min38s（含下述陷阱排查绕路） |

- **RPO（全量档）实测 = 11128 单**（账本成功响应、恢复数据中不存在），
  即 (T_b, T_f] ≈ 25 分钟窗口的全部成功订单；清单落 `lost-full-restore.txt`。
- **ID 空间复用现象**：恢复后新订单 id=103944（T_b 的 auto_increment 位），
  该 ID 在原库曾属于一条丢失订单——外部系统持有的旧订单 ID 不再指向原数据；
  业务语义由幂等键（user+key）而非订单 ID 决定，客户端用原键重试/查询收敛。

### 陷阱：consumer 先于 Topic 存在时入组 → 0 分区分配

relay 与 consumer 同时拉起：consumer 加入组时隔离 topic 尚未被 relay 创建，
range 分配器按空元数据分配 **0 个分区**（组 Stable、无消费、无报错），
kafka-go 不会因 topic 后续出现自动重平衡。**修复：重启 consumer**（topic
已存在，重新入组拿到 3 分区）→ 立即消费。**Runbook 顺序固化为：先 relay
（建 topic）→ 确认 topic 存在 → 再 consumer（入组）**。

### R4：全量 + binlog PITR

| 阶段 | 时刻 | 耗时 | 结果 |
| --- | --- | --- | --- |
| reset（删 ns 含 PVC，新卷）+ mysql-dr | 08:52:38 | ~40s | 空库、GTID ON |
| 导入 dump | 08:52:45→52 | 7s | 水位 77684（G_b） |
| binlog 重放（13 文件，宿主机 mysqlbinlog → kubectl stdin） | 08:56:01→08:59:05 | **3min04s** | **水位 88812 = T_f 精确值**；GTID 与源库故障前完全一致（ab69be63:1-167558）——增量 11128 单全部补回 |
| strict reconcile | ~08:59:40 | ~40s | 集合一致全 OK；守恒 PASS；**账本 verified=17705 lost=0 mismatch=0** → **实测 RPO=0** ✓ |
| api + 冒烟 | 08:59:48 | — | **RTO-pitr ≈ 7min38s**（自 reset 起；binlog 重放为主导项）✓ |
| relay（重建隔离 topic）→ consumer（**新组 order-consumer-dr2**） | 09:00:11 | — | 88813 四表 1:1:1 PASS（含冒烟订单） |

- **镜像工具链坑**：mysql:8.0 官方镜像**不含 mysqlbinlog**（精简版），改用
  宿主机 mysqlbinlog（9.6，向后兼容 8.0 binlog v4）解析归档、SQL 流经
  kubectl stdin 灌入；
- **每恢复代一个新消费组**：R3 的隔离 topic 里残留第一代消息（冒烟事件不在
  R4 恢复数据中），复用旧组/旧 topic 盲消费会造成 Inbox 孤儿——新代用新
  topic（删除重建）+ 新组（FirstOffset），不消费恢复点之外的旧事件，
  也不复用领先位址。

### R5：清理与原环境恢复

- order-dr 整栈删除（备份/账本/对账证据均在宿主机）；
- 原环境恢复：mysql-ic scale 0→3 后 GR 全灭成员 OFFLINE → 集群内跑
  mysql/mysql-operator 镜像 mysqlsh 执行 `dba.rebootClusterFromCompleteOutage()`
  → 3/3 ONLINE（mysql-ic-1 回归 PRIMARY，88812 水位不变）；router/order-api/
  relay/consumer 全部拉起；
- 原环境终局对账：**17705 verified / 0 lost / lag=0 / PASS**——隔离-恢复
  全流程对原数据零损伤。

### RTO/RPO 汇总

| 档位 | RTO 实测 | RPO 实测 | 主导项 |
| --- | --- | --- | --- |
| 全量备份 | **2min49s**（写入恢复） | **11128 单丢失**（≈25min 窗口） | 镜像拉起+建库（导入仅 7s） |
| 全量+PITR | **≈7min38s** | **0**（GTID 对齐） | binlog 重放 3min04s |
| 目标 | ≤30min | PITR 档 0 | 均达标 |

## 14. Root Cause

- **PITR 的正确性锚点是 gtid_purged 与快照的精确对应**：mysqldump 在
  `--source-data` 的锁窗口内同时取一致快照与位点/GTID 集，保证导入后
  gtid_executed 恰好覆盖快照内容；binlog 重放时 MySQL 对已执行 GTID
  静默跳过——无需位点计算，集合运算即对齐（实测 GTID 与水位双对齐）。
- **备份期劣化机制**：128M buffer pool 下 dump 顺序扫描把热点页逐出，
  叠加 203MB binlog tar 的页缓存污染（cgroup 计入容器内存）与 qemu Router
  的 ~8 倍 CPU 放大；secondary 的 OOM 则是「实验前已 1500/1536Mi 的存量
  压力 + 复制积压」——资源预算要按「正常态/故障态/备份态」三档评估。
- **0 分区陷阱机制**：Kafka 消费组分配基于入组时的订阅元数据；topic 不存在
  时 range 分配器给出空分配且组进入 Stable，成员不会感知 topic 后续创建。
  任何「依赖前置资源存在」的组件启动顺序都必须显式编排，不能并发拉起。
- **为什么逐单账本是判定性证据**：计数对账只能说「数量对得上」，账本把
  「客户端已收到成功响应的每一单」与恢复数据逐条比对——11128 的丢失清单、
  PITR 档的 0 丢失，都是这个口径给出的；「在窗未确认」区分了「响应未知」
  （合法）与「响应成功但丢失」（违约）两种语义。

## 15. Trade-offs

- **mysqldump vs 物理备份**：逻辑备份慢、污染 buffer pool，但产物可读、
  跨实例恢复简单、GTID 语义清晰——教学主线选它；生产规模应选 XtraBackup
  （不扫表污染）+ 增量 binlog（same-city DR 设计稿 §5.7 的路线）。
- **DR 档 MySQL 单实例**：本实验验证恢复流程而非重复 HA 命题；恢复后如需
  HA 按 EXP-16 Runbook 重新引导（记录为边界：DR 恢复完成 ≠ HA 就绪）。
- **复用 kafka-i 集群承载隔离 Topic**：Kafka 事件可重建、真相在 Outbox
  （EXP-09/17 结论），跨 ns 复用使演练聚焦数据恢复；真实机房级 DR 需要
  独立 Kafka 或对象存储级事件归档（选做边界）。
- **每恢复代新 topic + 新消费组**：代价是旧 topic 残留（按 retention 24h
  自然过期）；收益是代与代之间零污染——「隔离重放」的隔离性由命名空间
  （topic/group）而非重放逻辑保证。
- **备份时点选在负载中**：制造了真实的资源冲突（并抓到了 OOM），代价是
  该窗口 SLO 破防；若只为验证流程，静默备份更「干净」——但会掩盖
  「备份代价」这一真实结论。

## 16. Architecture Decision

- **ADR-038（备份与恢复主线）**：可验证一致性备份 = mysqldump
  `--single-transaction --source-data=2 --set-gtid-purged` + binlog 归档至
  故障域外；PITR = 空库导入 + mysqlbinlog 全量重放（GTID 自动跳过）。
  RPO 目标 < 备份间隔时 binlog 归档为必做（roadmap 要求），两档 RTO/RPO
  分别报告。DR 恢复栈（deploy/k8s/dr/）与恢复代隔离（新 topic+新组）进
  Runbook。
- **ADR-039（恢复 Runbook 顺序）**：受控顺序 = DB 就绪 → 导入 → PITR →
  对账 → order-api（写入恢复）→ relay（建隔离 topic）→ **确认 topic 存在**
  → consumer（新组入组）。依赖前置资源存在的组件禁止并发拉起（0 分区
  陷阱）；mysql:8.0 精简镜像不含 mysqlbinlog，重放工具在宿主机。

## 17. Lessons Learned

- **没验证过的备份等于没有备份**：本次第一次真正从备份恢复出业务，第一
  次就撞上三个坑（镜像无 mysqlbinlog、0 分区陷阱、GR 全灭引导）——恢复
  演练的价值就是把坑留在演练里。
- **RPO 是结果不是口号**：「全量备份」四个字在数字上是 11128 单；
  「PITR」是 0。同一个故障、同一份账本，两档恢复的差距必须用清单说话。
- **账本要在故障域外**：k6 日志在 pod 里，故障注入前拷出宿主机才作数；
  「成功订单不丢」的核验以它为准，DB 自身的计数无法自证。
- **守恒校验用差分不用绝对值**：实验里补过库存（K=1001822≠1000000），
  「初始库存 = 剩余 + 已提交」的绝对式校验会误报；差分式（Δorders+
  Δstock=0）对任意两个自洽时刻成立，且天然免疫人工干预。
- **恢复的「受控顺序」是设计出来的**：空库时 order-api 会 AutoMigrate 建表、
  relay 会建 topic、consumer 会入组——每个组件启动都有副作用，顺序错误
  轻则污染（孤儿事件）、重则卡死（0 分区）。
- **GR 全灭重启 ≠ 自动恢复**：成员 OFFLINE 等待引导；Runbook 里必须有
  `rebootClusterFromCompleteOutage`（或等价的手动 bootstrap）步骤。

## 18. Interview Questions

- 只有全量备份时 RPO 由什么决定？加 binlog 归档后为什么能做到 0？
  → RPO = 备份点到故障点的已提交写入（本实验 11128 单）。binlog 归档
  覆盖到 T_f，重放时 GTID 已执行集合自动跳过 dump 已含事务、只补增量
  ——对齐不靠位点计算，靠 gtid_purged（快照内容）与 binlog（增量）的
  精确衔接，实测 GTID 与水位双对齐。
- mysqldump 哪些参数保证「可恢复」？每个参数保护什么？
  → `--single-transaction`（一致性快照）；`--source-data=2`（记录位点，
  锁内与快照对齐）；`--set-gtid-purged=ON`（导入后还原 GTID 基准，PITR
  跳过的依据）；`--databases`（跨库范围显式化）。缺任何一个，恢复 либо
  撕裂 либо 无法 PITR。
- 隔离 Topic/新消费组重放解决什么问题？为什么不能复用旧 topic 旧组？
  → 旧 topic 混有恢复点之外的旧事件（盲消费 → Inbox 孤儿/重复副作用），
  旧组的位点可能领先于恢复数据（跳过任务）。新代新 topic+新组+FirstOffset
  → 只消费本代投递的事件；重复投递由恢复的 Inbox 主键去重吸收（保留原
  event_id 是去重生效的前提）。
- DR 演练中「原环境隔离」而不是「销毁」的边界价值是什么？
  → roadmap §3.3：不以销毁唯一副本为学习前提。隔离保留了回退路径（本
  实验末尾原环境完整恢复且数据零损伤）；同时演练本身仍满足「新卷+故障域
  外备份」的恢复语义——真实销毁演练需要第二套故障域外的完整备份。
- 恢复后新订单复用了丢失订单的 ID，这算数据错误吗？
  → 不算：ID 只是自增主键，恢复点之后丢失订单的行不存在，auto_increment
  回到 T_b 位点后复用 ID 空间是必然结果。业务身份由幂等键承载——丢失窗口
  内客户端用原键重试会创建新订单（旧幂等行同样回退），语义自洽；风险在
  外部系统若把订单 ID 当全局坐标引用，需要显式补偿清单（这正是丢失清单
  的用途）。
