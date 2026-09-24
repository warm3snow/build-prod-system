# EXP-16：MySQL 在线高可用与主库切换

## 1. 实验目标

- 部署 **MySQL InnoDB Cluster 单主模式＋Router**（3 实例），记录成员、多数派、
  路由、持久化与一致性配置，替换 EXP-02 以来的单实例 MySQL；
- 持续下单流量中中断主库，观察故障检测、自动选主、应用连接失效、重连与写入恢复，
  实测切换 RTO 与数据损失；
- 验证切主时的一致性等待策略（`group_replication_consistency`），
  防止新主尚未应用完积压事务便返回陈旧结果；
- 观察旧主重新加入（不得以独立可写主继续接受流量）；失去多数派时验证「优先正确拒写」。

## 2. 背景问题

EXP-14 只验证了应用故障域，并如实声明「MySQL 仍是单实例」；EXP-15 的发布
前提同样是单实例数据库。Phase 4 的剩余核心问题：

1. **主库故障后业务写路径何时恢复**：单实例 MySQL 故障 = 下单全停；
   主库切换需要「故障检测 + 选主 + 客户端重连」三段，从未实测过 RTO；
2. **切主瞬间的已提交事务去向**：主库故障时，部分事务可能已提交但未复制到
   新主；若新主立刻接受写入，客户端会读到「刚才下单成功、现在查不到」的
   陈旧结果——一致性等待策略是本实验的核心正确性命题；
3. **客户端连接行为**：Router 不会透明续接中断中的事务（roadmap 明示），
   应用侧的连接失效、重试与幂等键收敛从未在「切主」场景验证；
4. **旧主回归与多数派**：旧主重启后必须作为 SECONDARY 回归，不能脑裂成
   双写主；3 成员失去 2 个时集群必须拒写而非冒风险续写。

## 3. HA 拓扑（本实验冻结，HA 模拟档）

```text
order-api × 2（HA 档释放 1 核给 MySQL × 3）
      │  DB_DSN → mysql-router（6446 rw / 6447 ro）
      ▼
mysql-router × 2（Deployment，无状态）
      │  bootstrap 自 InnoDB Cluster metadata
      ▼
InnoDB Cluster（单主模式，Group Replication）
  mysql-ic-0（PRIMARY，可写）
  mysql-ic-1（SECONDARY）
  mysql-ic-2（SECONDARY）
  每实例独立 PVC 2Gi；多数派 = 2/3

故障域：单机（HA 模拟档）——3 实例分布在同一宿主机，
  只验证「软件/进程故障转移」；宿主机级 HA 不属于本实验结论
  （roadmap：单机多实例结果标注模拟档）
```

### 资源预算重算（单机 6 核，HA 档）

| 组件 | 副本 | CPU req | 说明 |
| --- | ---: | ---: | --- |
| order-api | 3→2 | 1 核/副本 | HA 档释放 1 核（EXP-15 已交付发布流程，副本数在 HA 档重新冻结） |
| mysql-ic | 3 | 250m/实例（limit 1 核） | 替换单实例 mysql（250m/2 核 limit 释放） |
| mysql-router | 2 | 50m/实例 | 新增 |
| 其余（kafka/redis/relay/consumer/dep-sim/监控/k6） | — | ≈ 2 核 | 不变 |
| **合计** | | **≈ 4.8/6 核** | 留 1.2 核调度余量 |

### 关键配置（冻结）

```text
MySQL 8.0（Group Replication，InnoDB Cluster 单主）：
  group_replication_group_name          = 固定 UUID
  group_replication_local_address       = mysql-ic-<i>.mysql-ic-hs:33061
  group_replication_group_seeds         = 3 个成员 33061
  group_replication_bootstrap_group     = 仅首成员启动时 ON
  group_replication_single_primary_mode = ON
  group_replication_consistency         = BEFORE_ON_PRIMARY_FAILOVER
      （切主前新主先应用完积压事务；8.0.14+）
Router：
  6446 = 读写端口（路由到 PRIMARY）；6447 = 只读（轮询 SECONDARY）
  实验主线只用 6446（读写分离不在本实验范围）
应用：
  DB_DSN 指向 mysql-router 服务（order-api/relay/consumer 全部切换）
```

## 4. SLA / SLO（冻结）

| 指标 | 目标 |
| --- | ---: |
| 主库故障 → 写入恢复（RTO） | ≤ 60s（GR 检测 ~5-10s + 选主 + Router 重连 + 应用重试） |
| 切主数据损失 | 0 已提交订单丢失（BEFORE_ON_PRIMARY_FAILOVER） |
| 切换期间错误分类 | 连接失败/超时可观测，幂等键重试收敛，无脏数据 |
| 旧主回归 | 作为 SECONDARY 加入，不接受独立写 |
| 多数派丢失（3 失 2） | 明确拒写，不返回假成功 |
| 切换后容量 | 继续满足 800 rps 混合负载 SLO |
| 对账 | 1:1:1、库存守恒、幂等无孤儿、成功响应账本逐单核验 |

## 5. Baseline

- 单实例 MySQL 下：EXP-14 应用故障实验期间 MySQL 零故障；EXP-04/06 基线
  都在单实例上测得；本实验切换后需在 Router 链路上重新验证基线；
- 应用层连接行为：gorm 连接池 25/副本 + `ConnMaxLifetime 5min`，
  主库切换时旧连接全部失效，重连走 Router；
- 客户端幂等重试：EXP-03/11 冻结口径（原键重试收敛）。

## 6. Hypothesis

1. InnoDB Cluster 在 PRIMARY 进程死亡后 ~5-10s 检出（GR 成员失效检测），
   多数派选出新主；Router 感知 topology 变化并重路由；应用连接失效→重试→
   恢复写入，总 RTO ≤ 60s；
2. BEFORE_ON_PRIMARY_FAILOVER 下，故障前已提交（收到 ack）的事务在新主
   就绪前全部应用——切主后查询能立刻读到故障前的订单，无「已成功但丢失」；
3. 故障瞬间「提交结果未知」的请求（客户端超时）可由原幂等键重试收敛，
   不产生重复订单（幂等占位在切主后仍有效）；
4. 旧主恢复后自动以 SECONDARY 回归（group_replication_start_on_boot），
   不形成双写主；Router 不会把写流量路由到它；
5. 失去多数派（2/3 死亡）时剩余成员拒写（GR 强制），读也可能受限——
   「正确拒写」优先于「冒险可用」。

## 7. 实验方案

1. **场景 F1（部署与基线）**：3 实例 InnoDB Cluster 引导成功（cluster.status()
   OK + 2 SECONDARY）；Router bootstrap 成功；应用切换 DSN；冒烟 + 800 rps
   基线（对比单实例：读/写 P99、失败率）。
2. **场景 F2（主库中断切换）**：持续下单（800 rps 混合）中 kill PRIMARY Pod，
   逐秒记录：故障检出 → 选主（新 PRIMARY）→ Router 重路由 → 应用写入恢复；
   记录切换窗口错误分类（连接拒绝/超时/未知结果）与受影响请求数；
   成功响应账本逐单核验（切换前后订单全部可查、无重复）。
3. **场景 F3（一致性等待验证）**：主库故障瞬间以短间隔持续提交小批量订单，
   记录故障时刻的「最后成功响应订单 ID」；切换完成后立即查询该订单在新主
   上是否可读（BEFORE_ON_PRIMARY_FAILOVER 应保证可读）；对照一次
   `group_replication_consistency=EVENTUAL` 的窗口（选做，若时间允许）。
4. **场景 F4（旧主回归）**：重启被杀 PRIMARY，观察其作为 SECONDARY 回归
   （非 PRIMARY）；尝试对其写入被拒（只读/路由正确）；cluster.status 三成员 OK。
5. **场景 F5（多数派丢失）**：kill 2/3 实例，剩余成员观察写入行为
   （应明确报错/超时，不返回成功）；恢复 2 实例后集群重新可用、数据核对。
6. **对账**：全程 1:1:1、库存守恒、幂等无孤儿；切换窗口订单逐单核验。

## 8. Code Change

- 应用代码无 Go 变更（连接行为由 gorm 连接池 + 现有重试/幂等承载）；
- `DB_DSN` 指向 Router 服务（K8s 层变更）。

## 9. Kubernetes Change

- `deploy/k8s/ha/mysql-innodb-cluster.yaml`（新增，HA 档专用）：
  - Headless Service `mysql-ic-hs`；
  - StatefulSet `mysql-ic`（replicas 3，mysql:8.0，独立 PVC 每实例 2Gi，
    group_replication 参数 env/init 注入，`--server-id` 按序号）；
  - 引导 Job `mysql-ic-bootstrap`（一次性：bootstrap 首成员 + addInstance 其余）；
  - Deployment `mysql-router`（replicas 2，bootstrap 后运行，6446/6447）；
  - Service `mysql-router`。
- `deploy/k8s/base/all.yaml`：order-api/relay/consumer 的 `DB_DSN` 改为
  `mysql-router.order-lab:6446`；order-api replicas 3→2（HA 档资源预算）。

## 10. Load Test

- `tests/load/fixed-rate.js`：800 rps × 8:1:1；F1 基线 3min、F2 切换窗口
  全程持续（5min），逐秒错误分类（k6 日志 + 应用日志 + metrics）。

## 11. Failure Injection

- PRIMARY 中断：`kubectl delete pod mysql-ic-<primary> --force --grace-period=0`
  （进程级故障，PVC 保留——与真实主库崩溃等价）；
- 多数派丢失：force delete 2 个成员 Pod；
- 恢复：重启被删 Pod（StatefulSet 自动重建，PVC 数据仍在）。

## 12. Observability

- Cluster 状态：mysql shell `cluster.status()`（或 SQL 查询
  `performance_schema.replication_group_members`）；
- 成员角色迁移时序：MEMBER_ROLE 轮询（PRIMARY/SECONDARY）；
- 应用侧：RED 指标、`mysql_*` 连接池指标（go-sql-driver 已有 RegisterDBStats）、
  写恢复时刻 = orders_created_total 重新增长的第一个时间戳；
- Router：Router 日志 topology 变化；6446 端口的连接失败/重连；
- 对账：1:1:1 + 成功响应账本逐单核验。

## 13. Results

实测环境：Rancher Desktop k3s（单机 6 核/16GB，**HA 模拟档**：3 逻辑成员同宿主机）。
InnoDB Cluster：mysql-ic-0/1/2（mysql:8.0，GR 插件，PVC 2Gi/成员），
Router ×2（mysql/mysql-router:8.0，**amd64 镜像经 qemu 模拟**——无 arm64 构建，
~8 倍 CPU 开销，limit 提至 2 核补偿）。引导：宿主机 mysqlsh 8.0.41 /
operator 镜像内 mysqlsh 8.0.32（`--dns` 指向 CoreDNS 直连成员）。

### 部署历程（三次迭代，各对应一个真实缺陷）

| 尝试 | 结果 | 缺陷 |
| --- | --- | --- |
| v1：命令行传 GR 变量 | CrashLoop（`unknown variable 'group_replication_group_name'` → init 中断 → 半初始化字典 → 后续重启跳过 init → `mysql.user doesn't exist` 循环） | mysql:8.0 entrypoint 的 `--initialize` 阶段不认插件变量 |
| v2：ConfigMap 只留 `plugin_load_add`+标准参数，GR 变量交给 mysqlsh `SET PERSIST` | **成功**：3 成员 Running，createCluster + clone 加入，3/3 ONLINE | — |
| v3：Router 用镜像 env 自引导（`MYSQL_HOST=mysql-ic-0`） | bootstrap 循环（21 次重启） | entrypoint 每次启动检查 MYSQL_HOST 是否 ONLINE 成员——固定成员单点（F2 根因，见下） |

### 场景 F1：HA 档基线（三次迭代收敛到 600 rps）

| 迭代 | 现象 | 根因与修复 |
| --- | --- | --- |
| 初测 800 rps | 99% 失败 | fresh 集群只有 P1/P2 种子，k6 查 P1..P10000 全部 miss → 负缓存回源风暴 → 503 rejected 7 万+。修复：补种 10,000 商品/库存（EXP-06 口径） |
| 复测 800 rps | 75% 失败 | TTL 60s 稳态回源（10000 SKU×2 端点/60s ≈ 333/s）超过 qemu Router 回源容量（~100/s）。修复：CACHE_TTL 60s→600s（商品不可变；P1 库存由下单失效保持新鲜） |
| 800 rps 终测 | 11.8% 失败（写 p99 1.18s） | qemu 下每语句 ~50-100ms×写事务 6 语句逼近 1s 写预算。**按 roadmap「切档重基线」降至 600 rps** | 
| **600 rps 终值** | **读 p99 77ms ✓ 写 p99 337ms ✓ 失败 0.001%（1/107999）✓** | HA 档冻结基线（对比：单实例档 1000-1200 rps——缩尺来自 qemu Router，多副本 MySQL 本身非瓶颈） |

### 场景 F2：主库强杀切换（三轮迭代，最终达标）

**F2b（修复前）：51.6% 失败、切换级联失败**

- kill PRIMARY（ic-2）→ GR 切主到 ic-0 → **ic-0 OOMKilled（Exit 137）**：
  切主瞬间新主承接全部写流量+积压应用+GR certification，内存冲破 768Mi limit
  → 新主被杀 → 剩 1/3 无多数派 → 全 OFFLINE；
- 交叉证据：`kubectl top` 稳态即 765Mi/768Mi（余量 0.4%）；
- 另发现 liveness 探针（5s 超时）在高负载+切主动荡下误杀成员（RESTARTS 3-4 次）。

**修复（三处）**：内存 limit 768Mi→1.5Gi（节点实际占用仅 53%，有余量）；
liveness 容忍 150s（30s×5 失败×10s 超时）；Router 改直接运行（见下）。

**F2-final（600 rps × 300s，T+60s 强杀 PRIMARY ic-1）**

| 阶段 | 时刻 | 证据 |
| --- | ---: | --- |
| kill | T+0 | `delete pod mysql-ic-1 --force --grace-period=0` |
| GR 检测+选举+解除只读 | **T+0.6s** | ic-1 日志：「Primary server left the group. Electing new Primary」「A new primary mysql-ic-1…（新主 ic-0）」「will execute all previous group transactions before allowing writes」「super_read_only=OFF」 |
| Router 元数据刷新+应用连接池替换 | T+1~30s | 订单计数采样：T+17s 起恢复增长，吞吐 ~4/s→14/s→40/s→**51/s（满速）T+30s** |
| 被杀成员回归 | T+31s | STS 重建 ic-1 → auto-rejoin 为 SECONDARY:ONLINE |
| **总失败率** | | **3.01%（5413/179774）**，切换窗口 ~30s/300s ≈ 10% 写降速+读抖动 |

- **RTO 分解**：GR 层 0.6s（force delete 立断 TCP，检测即时）；客户端视角写恢复
  ~20-30s（瓶颈=Router 元数据刷新+应用连接池替换陈旧连接），≤ 60s 目标 ✓；
- **0 已提交订单丢失**：kill 前最后订单 id=49822（F3 核验）；对账窗口全绿。

**Router 根因与修复（F2 迭代的核心发现）**

- F2b 中 Router 在主库死亡后**停止监听 6446**（探针 connection refused）→ 被 kubelet
  判死 → 重启后 entrypoint 检查 `MYSQL_HOST=mysql-ic-0`（恰为被杀主库）是否 ONLINE
  → bootstrap 失败 → CrashLoop → **写恢复被拖到 T+77s**（而 GR 本身 0.6s 切完）；
- 修复：一次性 bootstrap 产物（conf/keyring/密钥/state.json/证书）烘焙进
  Secret+ConfigMap，initContainer 组装，主容器直接 `exec mysqlrouter --config`
  （绕过 entrypoint 检查）；运行期 metadata cache（ttl=0.5s，全成员
  cluster-metadata-servers）在成员死亡时自动切换幸存者——修复后 51.6%→3.01%。

### 场景 F3：切主一致性（BEFORE_ON_PRIMARY_FAILOVER）

- kill 前最后成功响应订单 id=49822 → 切换完成后立即经 Router 查询 → **可读**；
- GR 日志直接证据：新主「will execute all previous group transactions before
  allowing writes」——新主先应用完积压再放开写入，无「已成功但查不到」窗口；
- 切换窗口「提交结果未知」的请求由幂等键收敛（0 孤儿幂等记录）。

### 场景 F4：旧主回归（多次实证）

- 三轮 F2 中被杀 PRIMARY 全部以 **SECONDARY** 回归（角色反转、无脑裂）；
  Router 不向其路由写流量；
- auto-rejoin 生效：`group_replication_start_on_boot` 被 configureInstance 持久化，
  STS 重建 Pod 后 ~30-60s 自动重入组。

### 场景 F5：多数派丢失（否定性发现 + 最终达标）

| 子场景 | 注入 | 结果 |
| --- | --- | --- |
| F5a 默认配置 | kill 2 SECONDARY | **幸存主库继续接受写入**（`unreachable_majority_timeout=0` 默认——发现 RPO 风险窗口：失去多数派的主库可持续积累未复制事务） |
| F5b 优雅缩容 | STS scale 3→1 | 成员优雅关停发 LEAVE → **组视图收缩为合法单成员组**，继续可写——「优雅缩减」与「多数派丢失」是两种不同拓扑事件 |
| F5c 暴亡+超时 | `docker kill` 两成员进程（无 LEAVE）+ `unreachable_majority_timeout=5` | **T+21s `super_read_only=1`**；写入经 Router 明确失败（ERROR 2003），**无假成功** ✓ |
| 全灭恢复 | — | GR 不自动重组全灭组：`dba.rebootClusterFromCompleteOutage`（自动选 GTID 最超前成员为新主=ic-1）→ 3/3 ONLINE ✓ |

### 附带发现：GORM OnConflict 在 GR 上的 Error 1869（EXP-17 前置修复）

- 现象：failover 后 consumer 卡死重试 41 条批——`Error 1869: Auto-increment value
  in UPDATE conflicts with internally generated values`（notification 批量插入）；
- 机制：GORM 把 `OnConflict{DoNothing:true}` 渲染为 `ON DUPLICATE KEY UPDATE id=id`，
  带自增列的表在 **Group Replication** 上触发 1869（单实例 MySQL 8.0 同版本无此
  行为）；failover 期间 relay 重投产生的批内 dup 是稳定触发路径；
- 修复：notification 插入改为真正的 `INSERT IGNORE`（`clause.Insert{Modifier:"IGNORE"}`，
  无 UPDATE 子句）；修复后缺口 33 条全部排空，lag=0。

### 对账（全实验窗口）

- outbox SENT == inbox == notif == **45059**（1:1:1 全程含三次切主+全灭恢复）；
- PENDING=0、孤儿幂等=0；HA 集群订单从 0 起算（旧单实例数据存档于 mysql-data PVC，
  不迁移——新窗口对账从 0 计）。

## 14. Root Cause

- **RTO 的真正构成**：GR 层（检测+选举+只读解除）在强杀下 <1s——网络立断、
  XCom 即时检测；客户端写恢复的主导项是 **Router 元数据刷新 + 应用侧陈旧连接
  替换**（~20-30s）。而 F2b 证明 Router 的**启动路径**可以把 RTO 拖到 77s+：
  entrypoint 的 bootstrap 检查绑定固定成员，成员死亡 → 崩溃循环。HA 代理层的
  「启动依赖」与「运行依赖」必须分离。
- **故障态资源预算 ≠ 正常态**：切主 = 剩余成员瞬时承接 100% 写流量 + 积压应用 +
  certification 三重叠加；768Mi limit（稳态余量 0.4%）在切主瞬间 OOM——
  内存 limit 必须按「故障态峰值」预留（1.5Gi = 稳态 ~2 倍）。
- **探针误杀在 HA 数据层是级联源**：liveness 5s 超时在高负载/切主动荡下周期性
  杀 mysqld → 被组驱逐 → 非计划切主 → 乃至多数派失守。数据组件存活探针必须
  宽松（150s 容忍），可用性交给 GR 自己的成员失联检测。
- **1869 的机制差异**：GR 强制 row-based binlog + 组内事务认证，对
  `ON DUPLICATE KEY UPDATE` 涉及自增列的语句执行额外校验（单实例无）；
  「在单实例上验证过的 ORM 生成 SQL」在 GR 上不等于安全。

## 15. Trade-offs

- **HA 档缩尺 600 rps（vs 单实例档 1000-1200）**：瓶颈是 qemu 模拟的 Router
  （每代理查询 ~8 倍 CPU），不是 MySQL 集群本身。多成员 GR 写吞吐实测与单实例
  相当（写仍受 P1 行锁限制，与副本数无关——与 EXP-13 结论一致）。原生 arm64
  Router 镜像可得后应回调 limit 并重测基线。
- **Router 直接运行 vs entrypoint 自引导**：直接运行把「配置产物」变成需要维护的
  烘焙件（重建集群时需重新生成 bundle，见 manual）；换来启动零依赖、成员死亡
  时运行期自动切换。生产对应物是 MySQL Operator 的 router sidecar 管理。
- **unreachable_majority_timeout=5**：太短可能在网络抖动时误触发只读；
  太长放大 RPO 风险窗口。5s 对局域网抖动偏激进，实验值；生产建议 10-30s
  + 结合 exitStateAction 语义评估。
- **优雅关停 vs 暴亡**：STS scale-down 是优雅路径（LEAVE→组收缩，无 RPO），
  节点级暴亡才是多数派丢失。**演练「多数派丢失」必须用暴亡注入**（docker kill
  / 节点断电），用缩容会得到完全不同的（且合法的）结果。

## 16. Architecture Decision

- **ADR-033**：MySQL HA 采用 InnoDB Cluster 单主 + Router 直接运行：
  bootstrap 产物烘焙进 Secret/ConfigMap（启动零依赖）；全成员作为 metadata
  servers（ttl=0.5s）；`BEFORE_ON_PRIMARY_FAILOVER`（防陈旧读）+
  `unreachable_majority_timeout=5` + `exit_state_action=READ_ONLY`（多数派
  丢失主动拒写）；成员资源按故障态峰值预算（1 核 limit CPU / 1.5Gi limit 内存），
  探针 150s 容忍。禁止 Router entrypoint 自引导模式（固定成员单点）。
- **ADR-034**：副作用/通知表对 GR 的插入统一使用 INSERT IGNORE（禁用
  GORM OnConflict{DoNothing}——其 UPDATE id=id 渲染在 GR 上触发 1869）。
  该修复是 EXP-17（Kafka HA 下的重复投递对账）的前置条件。
- **ADR-035**：全灭恢复路径为 `dba.rebootClusterFromCompleteOutage`（人工触发，
  自动选 GTID 最超前成员）；STS 滚动更新会重启全部成员，等同计划内全灭，
  更新后必须执行恢复脚本（记入 Runbook，EXP-18 细化）。

## 17. Lessons Learned

- **HA 的 RTO 数字要按层拆开报**：GR 0.6s、Router/连接层 20-30s、坏 Router
  启动路径 77s——「切换很快」与「业务恢复很快」是三个不同的测量点，
  只报其中一个都是误导。
- **代理层的启动依赖是隐形单点**：Router 运行期有全成员列表，但启动期
  只认 MYSQL_HOST 一个——「运行高可用」被「启动不高可用」拖垮。任何 HA
  组件都要问：它重启时需要什么？那个东西死了吗？
- **切主是内存/ CPU 的故障态峰值事件**：稳态资源预留（request）与故障态
  上限（limit）必须分开评估——EXP-13（扩容不破下游）与 EXP-16（故障不破
  内存）是同一条预算纪律的两次实证。
- **「在单实例验证过的 SQL」到集群上要重验**：1869 只在 GR 上出现；
  引入任何复制/集群层，ORM 生成语句都要过一遍新环境的对照测试。
- **演练注入方式必须匹配目标故障**：缩容（优雅）与 kill 进程（暴亡）在
  GR 里是两种拓扑事件，结论完全不同。写 Runbook 时必须写明「用什么注入、
  等价于什么故障」。
- **k8s 声明式系统里全灭恢复不会自动发生**：GR 的多数派机制恰恰「拒绝」
  自动重组（防脑裂）——rebootFromCompleteOutage 是刻意的人工决策点。

## 18. Interview Questions

- 主库故障后写入恢复（RTO）由哪几段组成？哪段最不可控？
  → GR 检测+选举（强杀下 <1s）→ Router 元数据刷新+应用连接池替换（20-30s，
  主导项）→ 若代理启动路径绑定死亡成员，额外 +60s 崩溃循环（最不可控）。
  F2 三轮实测：77s → 3.01%/30s 恢复。
- BEFORE_ON_PRIMARY_FAILOVER 解决了什么问题？有什么代价？
  → 保证故障前已提交事务在新主可读前全部应用（无「已成功但查不到」）；
  代价是切主后新主写开放前的应用积压延迟（本实验毫秒级，积压小）。
- 为什么 Router 不能透明续接中断中的事务？应用侧必须做什么？
  → 老主上的在途事务随进程死亡，GR 不会在别处「续跑」半截事务；Router 只
  保证后续连接路由到新主。应用侧：连接失效后按原幂等键重试（EXP-03/11 口径），
  F2-final 3.01% 失败全部由该路径收敛，0 孤儿。
- 旧主回归为什么不会脑裂成双写主？多数派在其中起什么作用？
  → 单主模式下 GR 只允许组内一个可写成员（super_read_only 机制）；旧主回归
  是「申请入组→被分配 SECONDARY 角色」，不接受独立写。多数派保证组视图唯一——
  失去多数派的成员要么主动只读（timeout=5 配置后）要么积累风险事务（默认 0，
  F5a 实测的反例）。
- 「多数派丢失时拒写」为什么是正确的选择？如果强行可用会有什么后果？
  → 少数派主库的写入无复制保障：该成员再死 → 事务全丢（RPO 无界）；且与
  重入成员的组视图冲突需要仲裁。F5a 证明默认配置就是「强行可用」——生产
  必须显式设 unreachable_majority_timeout。
