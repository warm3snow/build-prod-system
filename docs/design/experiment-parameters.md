# 实验参数表

> 所有实验开始前必须冻结本表对应行的数值，不得事后降低阈值把失败改成成功。
> EXP-01 先冻结：负载档、数据档、SLO 档。资源档待 EXP-02 部署后填写，实测档由对应实验回填。

## 1. 负载档（EXP-01 冻结）

| 场景 | 目标 QPS | 下单 TPS | 时长 | 用途 |
|---|---:|---:|---:|---|
| Smoke | 10 | 1 | 30s | 冒烟、部署验证 |
| Average | 500 | 50 | 5min | 日常回归 |
| Stress | 1,000 | 100 | 15min | 容量验证（缩尺档上限） |
| Spike | 100→1,000→100 | 对应缩放 | 10min | 突发与恢复 |
| Soak | 800 | 80 | 2h | 长稳（毕业前） |

- 请求混合：商品查询 80% / 下单 10% / 订单查询 10%。
- 下单接口带 `Idempotency-Key`，重试使用同一 key。
- 数据热点：10% 商品承担 80% 查询流量。

## 2. 数据档（EXP-01 冻结）

| 项 | 值 |
|---|---:|
| 商品数 | 10,000 |
| 用户数 | 50,000 |
| 初始库存（每 SKU） | 1,000 |
| 订单预置 | 0（随压测增长） |

## 3. SLO 档（EXP-01 冻结）

| 指标 | 正常态 | 备注 |
|---|---:|---|
| 查询 P99 | ≤ 200ms | 商品/库存查询 |
| 下单 P99 | ≤ 500ms | 不含客户端重试时间 |
| 系统错误率 | ≤ 0.1% | 库存不足、限流不计入 |
| 可用性 | ≥ 99.95%（月度折算） | 见 sla.md |

## 4. 资源档（EXP-02 已冻结）

| 组件 | CPU req/limit | 内存 req/limit | 副本 | 备注 |
|---|---:|---:|---:|---|
| order-api | 100m / 1000m | 64Mi / 256Mi | 1 | GOMAXPROCS=2（EXP-05 冻结） |
| MySQL | 250m / 1000m | 512Mi / 1Gi | 1 | local-path PVC 2Gi |

## 5. 实测档（各实验回填）

| 指标 | 值 | 来源实验 |
|---|---:|---|
| 单实例最大稳定 QPS（SLO 内） | ≈2,000 req/s（1000m+GOMAXPROCS=2 后） | EXP-04/05 |
| 1000 QPS 档延迟 | 读 P99 16.4ms / 写 P99 39.5ms，0 失败 | EXP-04 |
| DB 连接池（单副本） | 25（MaxOpen）/10（Idle），甜点位实测 | EXP-06 |
| 游标分页 vs 深分页 | 0.4ms vs 80.7ms（OFFSET 15 万） | EXP-06 |
| 缓存命中率 | 98.8%（热缓存，P1..P100 热点） | EXP-07 |
| 回源保护（EXP-08 冻结） | 合并 + 12 并发 + 200ms 等待 + 1s 超时 + 异步失效队列 1024 | EXP-08 |
| 热点过期合并率 | 97.9%（DB 回源减少 92%） | EXP-08 |
| Redis 故障态失败率 | 0.28%（stale 降级 + 明确拒绝，故障 90s） | EXP-08 |
| 事件链路（EXP-09 冻结） | Outbox 同事务 + Relay at-least-once + Inbox 去重；投递 P99 ≤ 2s、完成延迟 P99 ≤ 5s | EXP-09 |
| 事件投递延迟（200 TPS 稳态） | P50 336ms / P99 1969ms | EXP-09 |
| 业务完成延迟 | P50 1.96s / P99 29.2s（保序串行消费 200 msg/s 上限，EXP-10 解决） | EXP-09 |
| 崩溃点验证 | B1 排空无遗漏；B2/B3 dup 去重、副作用唯一；随机 SIGKILL 12 万单 0.003% 失败 | EXP-09 |
| Kafka 故障下单可用性 | scale→0 90s：48000 单 0 失败，恢复后自动排空 | EXP-09 |
| 资源预算重估（EXP-09） | MySQL 1→2 核、order-api 1→2 核、Kafka 2Gi/768m heap | EXP-09 |
| 消费吞吐（EXP-10 冻结） | 单实例 550-650 msg/s（8 并发 + 批 50 + 合并提交）；2/3 实例无收益 | EXP-10 |
| 完成延迟 P99 | 538ms（正常态，SLO 5s；EXP-09 实测 29.2s） | EXP-10 |
| 突发积压模型 | 峰值 ≈ 输入-消费×时长；12 万回放峰值 107600、排空 105s（偏差 <3%） | EXP-10 |
| 反压验证 | 水位 30000 封顶 30227；拒绝 27933 与计数精确一致；恢复自动受理 | EXP-10 |
| Kafka 排空时间 | 30201→0（恢复后 ~3min，含消费 550/s） | EXP-10 |
| 缩尺→最终放大系数验证 | 待测 | EXP-19 |
| 准入控制（EXP-11 冻结） | 令牌桶 1500 rps/副本 + burst 300、在途 200/副本、读/写总预算 500ms/1s、探针豁免 | EXP-11 |
| 安全重试（EXP-11 冻结） | 存储层仅 1213/1205 重试 ≤3 次（指数退避+抖动）；客户端仅对 status 0/5xx 幂等写重试 ≤2 次 | EXP-11 |
| 过载对照（3000 rps） | 无准入 96% 失败 vs 有准入系统错误率 0.041%、429 占 50%、0 重启 | EXP-11 |
| 临时失败对照（P1 锁 30s） | B1 无预算：tx 重试 398、在途 503×786、readyz 失败 5；B2 有预算：重试 111、0 在途拒绝、写 504 明确分类 | EXP-11 |
| 依赖保护（EXP-12 冻结） | 隔离池 8 + 有界等待 50ms + 独立超时 200ms + 熔断（连续失败 5 → 开 10s → 半开探测 1） | EXP-12 |
| 依赖故障对照 | C1 无隔离：核心读通过率 16%、sim 6000 调用×2s；C2 隔离：核心 99.6%、sim 仅 14 调用 | EXP-12 |
| 熔断生命周期 | fail 60s：12 次 open/half-open 循环、71969 次短路降级；恢复后自动关闭、35865 正常调用 | EXP-12 |
| 副本容量（EXP-13 冻结） | 读路径线性（2 副本纯读 3000 rps P99 50ms）；写路径 P1 行锁 ~150-200 TPS 串行点，扩容负收益 | EXP-13 |
| HPA（EXP-13 冻结） | CPU 70% × requests=1 核、min 1 / max 5（连接预算 25N+20 ≤ 151）；缩容 60s 稳定 + 50%/60s | EXP-13 |
| 扩容时序 | 指标可用 ~15s → rescale 一步 1→5 → 全 Ready 60-90s；缩容 5→2→1 每步 ~75s | EXP-13 |
| 缩容稳定性 | 3→1 负载中 97.2% 通过、~2.5s 摘除窗口、0 订单丢失 | EXP-13 |
| 应用故障域（EXP-14 冻结） | 固定 3 副本、maxSurge 0/maxUnavailable 1、PDB minAvailable 2、HPA 停用 | EXP-14 |
| 三类中断失败率 | 优雅退出 0.002%（2/96000）、突然中断 0.01%（14/96001）、容器崩溃 0.04%（44/96001），1:1:1 全程成立 | EXP-14 |
| PDB 驱逐语义 | minAvailable 2：第 1 次驱逐 201、第 2 次 429、恢复后自动放行 | EXP-14 |
| 节点故障结论 | 单机 cordon/drain = 全停（模拟档）；节点级 HA 推迟到多节点档 | EXP-14 |
| 发布（EXP-15 冻结） | 滚动 maxSurge 0/maxUnavailable 1；门禁=5xx 率 >1% 自动 undo；Schema 只做 expand（新列带默认值），contract 与发布分离 | EXP-15 |
| 正常发布窗口失败率 | 0.39%（connection refused，服务端不可见）；rollout ~60s | EXP-15 |
| 坏版本识别与回滚 | 识别 ~85s（错误率 3.74%）、回滚 14s；0 脏数据；undo 只回退一个 revision | EXP-15 |
| Schema 兼容 | expand：旧代码读写正常（DEFAULT 'web'）；contract 反例：DROP COLUMN → 新代码 1054 | EXP-15 |
| 发布流程 | tests/release.sh：smoke→回归→发布→SLO 检查→自动回滚，exit=0 一次通过 | EXP-15 |
| MySQL HA（EXP-16 冻结） | InnoDB Cluster 单主 ×3 + Router ×2 直接运行（烘焙配置）；BEFORE_ON_PRIMARY_FAILOVER + unreachable_majority_timeout=5 + exitStateAction=READ_ONLY；成员 1 核/1.5Gi limit（故障态预算）；探针 150s 容忍 | EXP-16 |
| HA 档基线 | 600 rps（读 p99 77ms / 写 p99 337ms / 失败 0.001%）；缩尺自 qemu Router（~8×CPU），非 MySQL 集群瓶颈 | EXP-16 |
| 主库切换 RTO | GR 选举 0.6s；客户端写恢复 ~30s（Router 刷新+连接池替换）；总失败 3.01%（600 rps×300s 强杀主库） | EXP-16 |
| 切换数据完整性 | 0 已提交订单丢失（kill 前最后订单新主可读）；1:1:1=45059 全程含 3 次切主+全灭恢复 | EXP-16 |
| 多数派行为 | 默认 timeout=0 时少数派主库继续写（RPO 风险，实测反例）；timeout=5+暴亡 → T+21s super_read_only=1 明确拒写 | EXP-16 |
| Router 修复 | entrypoint 自引导（MYSQL_HOST 单点）→ 直接运行烘焙配置：写恢复 77s→30s、失败 51.6%→3.01% | EXP-16 |
| GORM GR 兼容 | OnConflict{DoNothing}→ON DUPLICATE KEY UPDATE id=id 在 GR 上 Error 1869（单实例无）；副作用表统一 INSERT IGNORE（ADR-034） | EXP-16 |
| Kafka HA（EXP-17 冻结） | KRaft combined ×3（Parallel + publishNotReadyAddresses）；topic orders RF=3/min.isr=2 显式；relay acks=all；旧单 Broker 退役（PVC 存档） | EXP-17 |
| 事件链路 HA 基线 | 600 rps：投递 p99 1.29s、完成 p99 1.34s（SLO 5s ✓）、1:1:1、lag=0 | EXP-17 |
| Leader 强杀 | T+20s 内分区/Controller Leader 双切换；PENDING ≤15 瞬时；下单零影响；对账 66506 全绿 | EXP-17 |
| 双杀（多数派失守） | NotEnoughReplicas 明确拒写 + Coordinator 不可用（元数据停摆）；PENDING 瞬时 658 → T+37s 排空；终局 76713 四表 1:1:1 | EXP-17 |
| K8s 多清单纪律 | 同名 Service 会被 apply 相互覆写 selector（EXP-17 实测全集群 DNS 失效）；退役组件清单定义必须同步删除（ADR-037） | EXP-17 |
| 备份主线（EXP-18 冻结） | mysqldump --single-transaction --source-data=2 --set-gtid-purged + binlog 归档至宿主机（故障域外）；产物 SHA256 留档 | EXP-18 |
| RTO 实测 | 全量档 2min49s（写入恢复）；PITR 档 ≈7min38s（binlog 重放 3min04s 主导）——目标 ≤30min 达标 | EXP-18 |
| RPO 实测 | 全量档 = 11128 单丢失（(T_b,T_f] ≈25min 窗口，清单落档）；PITR 档 = 0（GTID 与水位双对齐，账本 17705 全 verified） | EXP-18 |
| 备份代价 | 负载中备份：dump 扫表挤占 128M buffer pool + tar 页缓存污染 → 5xx 10.4%、secondary OOMKilled（实验前已 1500/1536Mi）——备份期是第三档资源预算 | EXP-18 |
| 恢复代隔离 | 每 DR 代新 topic（删旧重建）+ 新消费组（FirstOffset）；consumer 必须在 topic 存在后入组（否则 0 分区分配、无自动重平衡）——Runbook 顺序 ADR-039 | EXP-18 |
| 过载复核（HA 档） | 900 rps：有效吞吐封顶 ~820/s，1.69% 明确拒绝（429×1173/504×1479/503×196/500×14），无雪崩 | EXP-19 |
| 剩余容量 | 热（缓存）下单 api+单 router 扛 600 rps：0.19% 失败、读 p95 148ms ✓、写 p95 553ms 边缘 | EXP-19 |
| Game Day（Redis 停机+Pod 强杀） | 总失败 44.22%（读 503 有界拒绝为主，stale 库 1024 键 << 20000 键空间）；写路径存活 22275 单零丢失；三条症状告警全链路验证；Redis 无持久化→恢复=冷缓存重预热风暴 | EXP-19 |
| 2h 长稳（否定结论） | 相变塌陷：热缓存相（6min，60 TPS）→ 回源刀锋均衡（34min，16-20 TPS）→ 写路径塌陷（80min，**0 TPS**）；全程内存平稳/队列有界/数据零损伤（47540 verified）——机制：TTL 过期需求 33/s ≈ 回源容量 24-52/s 零余量；短窗口（≤12min）结构上不可见 | EXP-19 |
