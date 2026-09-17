# EXP-07：Cache Aside 与缓存一致性边界

## 1. 实验目标

- 引入 Redis 查询缓存（Cache Aside），验证缓存对 DB 读负载与读 P99 的收益；
- 测量并解释一致性代价：陈旧读窗口、旧值回填竞态、负缓存遮蔽窗口；
- 验证失效失败可恢复，缓存错误或丢失不破坏下单不变量。

## 2. 背景问题

EXP-06 之后，读路径全部直连 MySQL：2000 QPS 负载下 80% 读请求持续占用 DB 连接与
CPU，且后续扩容会按副本数放大 DB 读压力。商品信息与展示库存对实时性不敏感
（允许有限陈旧），是典型的可缓存查询；下单正确性仍由 MySQL 事务与条件更新保证。

## 3. 架构与一致性边界（本实验冻结）

```text
读路径（商品/展示库存）：GET → Redis 命中?
  ├─ 命中/负缓存 → 直接返回（X-Cache: hit/neg）
  └─ 未命中 → MySQL 回源 → SET 回 Redis（TTL 60s）→ 返回（X-Cache: miss）

写路径（下单）：MySQL 事务提交（库存+订单+幂等，同一事务）
  └─ 事务提交后 → DEL {product,stock,neg} 键
       └─ DEL 失败 → EXPIRE 1s 截短 → 主 TTL 60s 兜底
```

- **只缓存商品与展示库存**；扣库存仍以 MySQL 事务结果为准，缓存库存不作为下单依据。
- **陈旧上界**：正常模式 = 提交到 DEL 完成（毫秒级）；旧值回填竞态最坏 = TTL（60s）。
- **负缓存**：不存在商品独立键 `cache:neg:{sku}`，TTL 5s；创建商品时随失效路径一并删除。
- **Redis 是可选依赖**：连接失败启动时降级为无缓存；运行中故障时读降级回源、写只记指标。

## 4. SLA / SLO

| Metric | Target |
|---|---:|
| 读 P99 | ≤ 200ms（沿用 EXP-04） |
| 系统错误率 | ≤ 0.1% |
| 陈旧读窗口（正常模式） | ≤ 100ms 量级（提交→失效 RTT） |
| 陈旧读窗口（旧值回填竞态，最坏） | ≤ TTL = 60s |
| 负缓存遮蔽窗口 | ≤ negTTL = 5s |

## 5. Baseline（无缓存，来自 EXP-04/06）

| 指标 | 值 |
|---|---:|
| 2000 QPS 恒定负载 | 送达 ~2000 QPS，错误率 0% |
| 读 P95 / P99 | ~2ms / ~8ms |
| DB 读负载 | 80% 流量全部回源 MySQL（约 1600 读 QPS） |

## 6. Hypothesis

1. 对固定热点 SKU 集合（P1..P100），热缓存命中率 > 95%，DB 读 QPS 下降一个数量级；
2. 冷缓存与无缓存性能相当（miss 只多付一次 Redis RTT，无收益但有额外延迟）；
3. Cache Aside 的陈旧窗口有界：正常毫秒级；旧值回填竞态最坏不超过 TTL；
4. 负缓存短 TTL 可防穿透且不被无限遮蔽；
5. 缓存错误/丢失不影响下单不变量（下单正确性完全在 MySQL 事务内）。

## 7. 实验方案

1. **三轮对照**（固定 2000 QPS × 3min、P1..P100 热点、8:1:1）：无缓存 / 冷缓存（FLUSHDB）/ 热缓存（预热脚本）。
   观察命中率（应用指标 + X-Cache 头）、DB Com_select 差量、p95/p99、Redis 资源。
2. **陈旧窗口场景 A**：`CACHE_INVALIDATE_DELAY_MS=300` 放大「提交→失效」窗口，高频读 + 中途下单，
   统计读到旧值的持续时间；再关闭放大测自然窗口。
3. **陈旧窗口场景 B（旧值回填竞态）**：`CACHE_FILL_DELAY_MS=2000` 放大回源回填延迟，
   构造「读请求携带旧值 → 下单已删缓存 → 旧值回填污染缓存」，验证污染持续 ≤ TTL 后自愈。
4. **负缓存**：请求不存在 SKU 两次（回源 → neg）；SQL 插入商品后验证 5s 内遮蔽、之后可见。
5. **失效失败恢复**：scale Redis → 0，下单 + 读，观察降级与 `cache_errors_total{op=del}`、
   `cache_invalidate_total{result}`；恢复后验证缓存重建。
6. **不变量对账**：三轮压测后 `stock + p1_orders = 压测前水位`（守恒）。

## 8. Code Change

- `internal/cache/cache.go`（新增）：Cache Aside 读写、负缓存、失效兜底（DEL 失败 → EXPIRE 1s → TTL）、
  指标（hits/misses/negative_hits/errors/invalidate）。
- `internal/api/handler.go`：getProduct/getStock 接入缓存（X-Cache 头）；createOrder 事务提交后失效
  （重放订单不失效）。
- `internal/config/config.go`：REDIS_*、CACHE_ENABLED/TTL/NEG_CACHE_TTL 与两个实验开关
  （CACHE_INVALIDATE_DELAY_MS / CACHE_FILL_DELAY_MS，生产必须为 0）。
- `cmd/order-api/main.go`：Redis 初始化（PoolSize 20 有界）、启动失败降级无缓存。
- 依赖：`github.com/redis/go-redis/v9 v9.22.0`。

## 9. Kubernetes Change

- 新增 Redis Deployment（`redis:7.4-alpine`，maxmemory 64mb + allkeys-lru，无持久化）+ Service。
- order-api 注入 `REDIS_ADDR`、`CACHE_ENABLED=true`、`CACHE_TTL=60s`、`NEG_CACHE_TTL=5s`；
  最终镜像 `order-api:exp07c`（exp07b 修复启动降级，exp07c 修复负缓存 miss 响应头）。
- Grafana 新增面板：Cache hit ratio / Cache ops & errors / Cache invalidations / DB backfill。

## 10. Load Test

- 脚本：`tests/load/cache-product.js`（SKU 热点收敛到 P1..P100，统计 X-Cache 命中）+
  `tests/load/cache-warmup.js`（热缓存预热）。
- 恒定 2000 QPS × 3min × 3 组；下单 SKU 固定 P1，压测前补库存并记录水位。

## 11. Failure Injection

- Redis 停服（scale → 0）与恢复；缓存删除失败（停服下单触发 DEL 错误路径）。

## 12. Observability

- 新增：`cache_hits_total{op}` / `cache_misses_total{op}` / `cache_negative_hits_total{op}` /
  `cache_errors_total{op=get,set,del,expire}` / `cache_invalidate_total{result=ok,degraded,error}`。
- 响应头 `X-Cache: hit|neg|miss|off` 便于 curl 与 k6 直接观察。
- DB 侧用 `Com_select` 差量核算回源 QPS。

## 13. Results

实测环境：Rancher Desktop k3s（单机），镜像 `order-api:exp07b/exp07c`（三轮对照为 exp07b，
负缓存/失效恢复为 exp07c），Redis `7.4-alpine`（maxmemory 64mb，allkeys-lru），
恒定 2000 QPS × 3min，热点 SKU 集合 P1..P100，8:1:1 读写混合。

### 三轮对照（2000 QPS × 3min，P1..P100）

| 组 | 送达 iter/s | dropped | 错误率 | 命中 | miss | 命中率 | DB select/s | avg | p95 | max | 备注 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| A 无缓存 | 1992.9 | 1226 | 0% | — | — | 0%（X-Cache off=287148） | **2035** | 25.6ms | 107.0ms | 4426ms | 对照组：全部读回源 |
| B 冷缓存 | 1999.3 | 0 | 0% | 284641 | 3383 | 98.8% | **476** | 8.4ms | 14.1ms | 3112ms | 冷窗口 <2s，随后即热 |
| C 热缓存 | 2000.0 | 0 | 0% | 284323 | 3358 | 98.8% | **492** | 1.9ms | 7.8ms | 409ms | 与 EXP-06 基线一致 |

- **DB 读降载**：2035 → 476/492 select/s，**约 76% 的读负载被缓存吸收**，且与 EXP-06 热基线（p95 7.6ms）吻合。
- **冷 vs 热**：100 个热点 SKU 在压测开始 ~2s 内全部回填，冷窗口转瞬即逝，两组命中率几乎相同；
  但冷启动期间的回源风暴拉高延迟（avg 8.4ms vs 1.9ms，max 3.1s vs 0.4s）——冷缓存与热缓存必须分开测量。
- 下单请求理论 ≈10.8 万，实际新订单 77371：差异为业务拒绝（幂等重放/库存拒绝，均非系统错误，
  checks 失败 0）；库存守恒不受影响。
- A 组 p95 107ms 长尾：无缓存时 80% 读全部打 DB + P1 下单热点行锁竞争叠加所致。

### 陈旧窗口

| 场景 | 约定上界 | 实测窗口 | 结论 |
|---|---:|---:|---|
| A 自然模式 | 毫秒级 | 30.4ms* | 通过（*含下单事务耗时与 8ms 轮询精度，实际提交→DEL 窗口为亚毫秒级） |
| A 放大 300ms（CACHE_INVALIDATE_DELAY_MS） | ≈ 300ms + 事务 | **337.9ms** | 窗口随开关线性平移，机制成立 |
| B 旧值回填（CACHE_FILL_DELAY_MS=2000） | TTL 60s | **复现污染**：缓存返回 199999（hit）vs MySQL 199998；TTL 剩余 48s，65s 后自愈 | 通过：最坏陈旧 = TTL，有界可解释 |

### 负缓存与失效恢复

| 验证项 | 结果 |
|---|---|
| 负缓存命中不打 DB | 通过：第一次 404（X-Cache: miss，回源+写负缓存）→ 第二次 404（neg） |
| 商品创建后遮蔽窗口 ≤ 5s | 通过：SQL 插入后立即查询 404（neg 遮蔽），6s 后 200，再请求 hit |
| Redis 故障期间下单正确、读降级回源 | 通过：下单 201；GET 200 且值正确（X-Cache: miss） |
| DEL 失败有指标 | 通过：`cache_errors_total{op=del}`、`cache_invalidate_total{result=error}` 均有计数 |
| 恢复后缓存重建、无永久陈旧 | 通过：Redis 恢复后 miss → hit，值正确 |
| 三轮压测后库存守恒 | **通过**：P1 200000+485172 = 122629+562543；P2 200000+21 = 199998+23 |

## 14. Root Cause（实测解释）

- 无缓存/冷缓存下 80% 读请求必然回源；热缓存把读负载从 MySQL 转移到 Redis，
  DB 只剩 10% 写 + 少量回填与 10% 订单查询（实测 select 2035 → 476/s）。
- 场景 A 陈旧窗口 = 事务提交到 DEL 执行完成的间隔；放大开关直接平移该窗口
  （实测 337.9ms ≈ 300ms 放大 + ~30ms 下单事务）。
- 场景 B：读请求「DB 查询」与「SET 回填」之间下单并 DEL，旧值在 DEL 之后回填，
  覆盖新状态；单次 DEL 无法消除该竞态（业界用双删/延迟双删缓解，但仍有残余），
  本实验选择 TTL 兜底，实测污染持续 ≈ 剩余 TTL（48s 后 65s 内自愈），最坏陈旧绑定到 60s。
- 失效失败：DEL 错误路径尝试 EXPIRE 1s；Redis 整体不可用时靠主 TTL 与「Redis 无持久化、
  恢复即冷启动」保证不会永久陈旧（实测恢复后 miss → hit 重建正常）。

## 15. Trade-offs

- 库存接口牺牲实时性换 DB 降载：展示库存可陈旧 ≤ TTL；任何下单相关判断不允许读缓存。
- 下单路径增加一次同步 DEL（Redis RTT ≈ 亚毫秒），换取缓存新鲜度；Redis 故障时
  下单 P99 会多等一个 Redis 超时（100ms），可观测、可接受，EXP-08 再评估异步化。
- 双删/消息驱动失效可缩小场景 B 窗口，但复杂度上升；本实验以 TTL 兜底换取简单性。
- Redis 单实例、无持久化：数据可重建，故障即冷缓存；HA 不在本实验范围。

## 16. Architecture Decision

- ADR-013：商品/展示库存采用 Cache Aside（TTL 60s + 负缓存 5s），下单提交后同步失效、
  失败 EXPIRE 1s 兜底；严格读后写一致需求绕过缓存。
- ADR-014：Redis 是可选依赖——启动 Ping 失败保留客户端、读降级回源、就绪自动恢复，不永久降级。

## 17. Lessons Learned

- 缓存对照必须固定 SKU 集合：SKU 随机化（P1..P10000）会稀释命中率，掩盖真实收益。
- 陈旧窗口用实验开关放大后必须归零；生产配置残留放大开关属于事故。
- 压测前补库存并记录水位，否则库存耗尽后写路径变成纯 409，三轮对照失真。
- **启动时序**：Redis 与 order-api 同时部署时，Redis readiness（initialDelay 5s）晚于应用
  首次 Ping，启动失败若永久降级等于静默丢缓存。修复：启动 Ping 失败保留客户端、读降级
  miss，Redis 就绪后自动恢复（exp07b 修复）。
- **竞态复现要求精确时序**：场景 B 若 GET 直接命中缓存（不经过回源），fillDelay 不生效、
  污染不会发生；必须先删缓存让 GET 走 miss 回源路径，让「读 DB」与「SET 回填」被下单+DEL
  分隔开。实验脚本设计必须理解目标竞态的触发条件。
- **冷窗口比想象更短**：热点集合 100 个 SKU、2000 QPS 下冷缓存 ~2s 内转热；
  "冷缓存组"的 DB 收益接近热缓存，但延迟指标（avg/p95/max）能区分冷启动回源风暴——
  观察冷热差异要看延迟分布与回源时序，不能只看总 DB QPS。
- **go-redis 连接池日志**：Redis 不可用期间每次请求都会产生 "failed to dial" 日志淹没
  业务日志，需用 `logging.VoidLogger` 抑制；失败观测依靠指标（cache_errors_total）而非日志。

## 18. Interview Questions

- Cache Aside 为什么先更新 DB 再删缓存？先删缓存会有什么问题？
  → 先删缓存时，删除到提交之间的读必然回填旧值污染缓存；先更新 DB 后 DEL 的残留竞态需精确交错且 TTL 有界。
- 旧值回填竞态为什么单次 DEL 无法消除？有哪些缓解方案？各自代价？
  → DEL 删不掉「还在路上的旧 SET」；双删（延迟值难选、仍有残余）、binlog 失效（新组件）、TTL 兜底（本实验，最坏陈旧=TTL）。
- 负缓存为什么要独立键和短 TTL？与正缓存共用键会有什么解析歧义？
  → 独立键避免与业务值解析歧义；短 TTL 保证新对象最多被遮蔽 5s（实测插入后到期可见）。
- 缓存里库存是旧的，用户按旧库存下单会超卖吗？为什么？
  → 不会：下单走 MySQL 条件更新按真实库存判定，缓存只服务展示接口（实测污染场景下库存守恒）。
- DEL 失败后系统靠什么保证不会永久陈旧？Redis 重启（无持久化）后缓存是什么状态？
  → EXPIRE 1s → 主 TTL 60s 兜底，陈旧有界；Redis 无持久化重启后全 miss 回源，天然无旧值残留。
