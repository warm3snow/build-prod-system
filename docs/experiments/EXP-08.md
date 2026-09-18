# EXP-08：缓存失效与有界回源

## 1. 实验目标

- 以热点 Key 过期为对照场景，验证进程内请求合并（singleflight）对重复回源的压缩；
- 为回源建立独立超时、并发上限与有界等待；超限时对允许陈旧的查询返回本地旧值（stale），
  无旧值可用时明确拒绝（503）；
- 用同一条保护链路验证 Redis 不可用与恢复：数据库负载、核心下单能力、缓存重建突发；
- 验证负缓存对不存在 ID 的回源效果。

## 2. 背景问题

EXP-07 引入 Cache Aside 后，热缓存吸收了 76% 的读负载。但两个场景没有保护：

1. 热点 Key 到期瞬间，同一 Key 的并发 miss 会各自回源，形成"回源风暴"——风暴不仅多打 DB，
   还会挤占 Redis 连接池与 CPU，反过来拖慢正常命中请求（本实验场景 A 实测）。
2. Redis 整体不可用时，读请求全部降级回源，回源若不受限，等于把缓存流量直接转嫁给 MySQL；
   同时 EXP-07 的下单同步失效路径会把 Redis 超时（100ms×2）叠加到下单延迟上。

## 3. 保护链路（本实验冻结）

```text
读路径：GET → Redis 命中? 
  ├─ hit → 返回 + 刷新本地旧值库（滑动续期）
  ├─ neg → 404（不打 DB）
  ├─ Redis 故障 + 本地旧值 → stale（X-Cache: stale，不打 DB）
  └─ miss → [singleflight 合并] → [回源信号量 12] → [独立超时 1s] → DB → SET
         ├─ 回源成功 → miss
         ├─ 额度等待 >200ms / DB 失败 / 回源超时 → 有旧值 stale，无旧值 503 reject
         └─ DB 确认不存在 → 404 + 负缓存（5s）

写路径：下单事务提交 → 异步失效队列（容量 1024，满则丢弃，TTL 兜底）
  └─ worker：DEL {product,stock,neg} → 失败 EXPIRE 1s → 失败靠主 TTL 兜底
```

- **请求合并范围仅限单进程**：多副本下每个副本各自回源一次；本实验单副本。
- **回源并发上限 12 < DB 连接池 25**，为下单写路径保留预算（EXP-06 连接预算表）。
- **本地旧值库**：有界（1024 条目）、TTL 60s、**滑动续期**——故障期间被持续读取的 Key
  不会同步过期后一起挤向 DB；旧值陈旧度上界 = 上次成功回源/命中 + 2×TTL，仅服务展示类查询。
- **异步失效**：下单不再同步等待 Redis；健康时队列延迟可忽略（毫秒级），故障时队列满丢弃。

## 4. SLA / SLO（冻结）

| Metric      |       正常态 |                          故障态（Redis 不可用） |
| ----------- | -----------: | ----------------------------------------------: |
| 读 P99      |     ≤ 200ms |                  允许陈旧（stale），上界 2×TTL |
| 系统错误率  |      ≤ 0.1% |                        ≤ 5%（含明确 503 拒绝） |
| 下单成功    | 正确且可查询 |                **必须保持**（不依赖缓存） |
| DB 回源并发 |           — |                                 ≤ 12（信号量） |
| 恢复后重建  |           — |               有界：合并 + 额度限制，无二次风暴 |
| 拒绝可见性  |           — | 503 +`backfill_overloaded/timeout/error` 分类 |

## 5. Baseline（来自 EXP-07）

- 热缓存命中率 98.8%，DB 读 ~490 select/s，读 p95 7.8ms。
- 下单同步失效：Redis 故障时下单 P99 多付 ~200ms（DEL+EXPIRE 两次超时）。
- 无保护回源：EXP-08 场景 A 初测中 Redis 池（20）在 4000 QPS 下被耗尽，
  GET 错误率 12.7%，stale 兜底触发（`cache_errors_total{op=get}` 183036）。

## 6. Hypothesis

1. 同 Key 并发 miss 经 singleflight 合并后，DB 回源数接近"过期事件数"而非"请求数"；
2. 无合并时回源风暴会放大 Redis 池压力与 CPU 消耗，拉高全局延迟（不只是 miss 请求）；
3. 有界回源（12 并发 + 200ms 等待 + 1s 超时）能把 Redis 故障期间的 DB 负载锁死在预算内；
4. 本地旧值库滑动续期可让故障期间读服务以 stale 持续降级，而不是周期性拒绝风暴；
5. 异步失效让下单延迟与 Redis 可用性解耦；
6. 负缓存 + 合并把不存在 Key 的穿透压缩到"每 Key 首批一次"。

## 7. 实验方案

1. **场景 A（热点过期对照）**：单热点 P1、`CACHE_TTL=5s`（放大过期频率）、
   纯读 4000 QPS × 3min × 2 组：`CACHE_COALESCE_ENABLED=true/false`。
   观察 wanted/executed（合并率）、Com_select 差量、X-Cache 分布、延迟分位。
2. **场景 B（Redis 不可用及恢复）**：8:1:1 混合 2000 QPS × 5min，压测 60s 后 scale redis→0，
   故障持续 90s 后恢复。故障期观察 stale 服务率、503 拒绝、下单 TPS 与延迟、DB 线程数；
   恢复期观察空库重建（stale → miss → hit 收敛、重建回源速率上界、有无二次无界回源）。
   该场景驱动了三次实现迭代（见第 13 节）。
3. **场景 C（负缓存回源）**：不存在 SKU 集合 × 多 VU 并发双轮请求（k6 `shared-iterations`，
   `__ITER` 为 per-VU，实际效果为 40 个 Key × 50 并发），观察 DB 回源数与 neg 命中数。
4. **对账**：压测前后 `stock + orders` 守恒；按时间窗口核算下单数 = 库存扣减数。

## 8. Code Change

- `internal/cache/cache.go`：
  - 读路径重构为 `GetOrLoadProduct/GetOrLoadStock`（`readCache` + `doBackfill` + `runBounded`）；
  - `singleflight.Group`（`golang.org/x/sync`）按 Key 合并，`Forget` 防内存增长；
  - 回源信号量（`chan struct{}`）、`BackfillAcquireTO` 有界等待、`BackfillTimeout` 独立超时；
  - `staleStore`：有界本地旧值库，命中滑动续期，写入半 TTL 节流；
  - 失效路径异步化：有界队列（`InvalidateQueueSize`）+ 单 worker，满则丢弃计 `dropped`；
  - 新增指标：`cache_backfill_{wanted,executed}_total{op}`、`cache_backfill_outcome_total{op,result}`、
    `cache_backfill_inflight`、`cache_stale_hits_total{op}`、`cache_stale_entries`、
    `cache_invalidate_queue_len`；
  - `ReadStatus` 扩展为 hit/neg/miss/stale/reject（`StatusNegFresh` 区分"本次回源确认不存在"）。
- `internal/api/handler.go`：`getProduct/getStock` 接入新 API，`StatusRejected` → 503 + 分类 code。
- `internal/config/config.go` + `cmd/order-api/main.go`：`CACHE_COALESCE_ENABLED`、
  `BACKFILL_MAX_CONCURRENCY=12`、`BACKFILL_ACQUIRE_TIMEOUT=200ms`、`BACKFILL_TIMEOUT=1s`、
  `STALE_MAX_ENTRIES=1024`、`STALE_TTL=60s`、`INVALIDATE_QUEUE_SIZE=1024`、
  `REDIS_POOL_SIZE=128`（EXP-07 的 20 在 4000 QPS 下耗尽，见场景 A 初测）。
- 依赖：`golang.org/x/sync v0.22.0`。

## 9. Kubernetes Change

- 镜像 `order-api:exp08e`（最终；exp08a→exp08e 迭代，见第 13 节构建说明）。
- order-api 注入上述 env；Redis 部署不变。
- 构建说明：实验期间构建机无法访问 Docker Hub/gcr.io，镜像通过
  「复用 exp07c 镜像底座 + `docker cp` 注入新二进制 + `docker commit`」离线构建，
  Dockerfile 未改动，运行底座与 EXP-07 完全一致（distroless nonroot）。

## 10. Load Test

- `tests/load/hotkey-expire.js`（新增）：纯读、单热点、可配 RATE/DURATION/HOT。
- `tests/load/cache-product.js`：`countCache` 扩展 stale/reject 计数（口径沿用 EXP-07）。
- `tests/load/cache-warmup.js`：`vus` 随 SKU_COUNT 收缩（修复小集合预热失败）。
- `tests/load/nx-backfill.js`、`tests/load/nx-single-round.js`（新增）：不存在 SKU 穿透场景。

## 11. Failure Injection

- Redis 停服（scale→0）90s 与恢复；故障期间持续下单与读。

## 12. Observability

- 第 8 节新增指标全部接入 ServiceMonitor（沿用 EXP-07 抓取）。
- 响应头 `X-Cache` 扩展 `stale` / `reject`。
- Grafana 面板已实现并接入（`deploy/monitoring/grafana-dashboard-order-api.json`，
  uid `order-api-red`，经 Grafana sidecar ConfigMap 自动加载，见 deploy/README.md）：
  - Backfill wanted vs executed（合并率）
  - Backfill outcome stacked（fresh/neg/stale/rejected 堆叠）
  - Invalidate queue len
  - Stale entries

## 13. Results

实测环境：Rancher Desktop k3s（单机），最终镜像 `order-api:exp08e`。
**镜像迭代**：exp08a（neg 语义 bug：回源确认不存在误标 neg）→ exp08b（修复）→
exp08c（Redis 池 20→128）→ exp08d（异步失效）→ exp08e（stale 滑动续期，最终）。

### 场景 A：热点过期对照（P1 单热点、TTL 5s、纯读 4000 QPS × 3min）

| 组        | k6 miss | wanted | executed |          合并率 | Com_select 增量 | stale | GET 错误 | 延迟 med/p95          |
| --------- | ------: | -----: | -------: | --------------: | --------------: | ----: | -------: | --------------------- |
| A1 合并   |    6694 |   6696 |      140 | **97.9%** |            +141 |  3036 |     3036 | ~1ms / ~18ms          |
| A2 无合并 |    1785 |   7796 |     1787 |              0% | **+1829** | 50746 |    44737 | 31ms /**210ms** |

- **DB 回源减少 92%**（140 vs 1787 次）；合并率 97.9%（wanted 6696 → executed 140）。
- 无合并时回源风暴把应用拖慢（med 31ms vs 1ms），Redis 连接/CPU 争抢导致 GET 错误
  44737 次（13 倍于合并组），大量请求被迫走 stale 兜底——但 0% 系统失败：
  保护链路兜住了"坏配置"。
- A1 组 stale 3036 的来源：4000 QPS 下单热点瞬时并发超过 Redis 池短暂容量时的超时，
  滑动续期后仍以旧值服务成功。
- 初测（exp08b，池 20）在 4000 QPS 下 GET 错误率 12.7%，据此把池扩到 128。

### 场景 B：Redis 不可用及恢复（8:1:1、2000 QPS × 5min、故障 90s）

| 轮次（实现）                             |          失败率 |  reject 计数 | stale 计数 | 说明                                                      |
| ---------------------------------------- | --------------: | -----------: | ---------: | --------------------------------------------------------- |
| b1 同步失效（exp08b）                    | **21.9%** |           — |         — | 下单叠加 200ms 失效超时 → P1 行锁排队 → DB 池耗尽       |
| b2 异步失效（exp08d）                    |           17.1% |        81559 |     154870 | 拒绝风暴：本地旧值同步过期 → 周期性回源洪峰              |
| **b3 异步失效+滑动续期（exp08e）** | **0.28%** | **21** |     201760 | 故障全程 stale 服务；残余失败为下单/latest 的 DB 热点排队 |

b3 故障窗口细节：

- 读请求以 stale 持续服务（X-Cache: stale，200 + 正确旧值）。
- 下单保持可用：故障中实测 201（延迟 1.3s，含 P1 单行 200 TPS 热点锁竞争）；
  故障期间新单 30860，无因缓存故障产生的错误下单。

b3 恢复窗口细节（同一时间线，Redis 恢复后）：

- **恢复初期仍以 stale 兜底**：恢复后 70s 内 P1 读仍为 `X-Cache: stale`——应用侧
  go-redis 连接池在 Redis 不可用期间已全部失效，重连需要若干请求周期；
  此窗口内本地旧值库继续服务，请求不碰 DB。
- **收敛路径**：stale → miss（回源重建）→ hit。恢复后 `cache_errors_total`
  归零（get/del/expire 全 0），重建回源速率 ~8/s；随后观察窗口内 P1 完成
  miss → hit 收敛，100 个热点 Key 在压测流量下逐批回填。
- **重建有界、无二次风暴**：重建受合并 + 信号量 12 限制；与 b2 对照——b2
  （无滑动续期）恢复期曾出现 81559 次拒绝，b3 恢复期 reject 仅 21，
  **未出现第二次无界回源**。
- 库存守恒（横跨故障与恢复全程）：补库存后水位 P1=200000+orders，压测后
  stock 169140 + 新单 30860（时间窗口核算）——精确相等；
  另 21899 个下单为跨轮幂等重放（200），不扣库存。

### 场景 C：负缓存回源（不存在 SKU × 多 VU 并发）

| 轮                 | 请求 | X-Cache miss | X-Cache neg | executed | Com_select 增量 |
| ------------------ | ---: | -----------: | ----------: | -------: | --------------: |
| 40 keys × 50 并发 | 2000 |         1147 |         853 |       66 |             +67 |
| 20 keys × 50 并发 | 1000 |          572 |         428 |       32 |             +33 |

- **2000 个不存在 Key 的请求只打 DB 66 次（96.7% 压缩）**：同 Key 50 并发 miss 被合并为
  1 次回源（首批），写负缓存后其余全部 neg 命中、零 DB。
- k6 `shared-iterations` 的 `__ITER` 为 per-VU（grafana/k6 latest），脚本实际覆盖
  40/20 个 Key × 每 Key 50 并发——恰好构成"同 Key 并发穿透"的理想对照。

## 14. Root Cause

- **合并收益**：过期风暴的 DB 负载 ∝ 过期事件数而非请求数；合并组 140 次回源 ≈
  5s TTL × 3min ≈ 36 次过期 × 2 操作 × ~2 批，与模型一致。
- **无合并的放大链**：并发 miss 各自回源 → 回源路径（GET+Exists+SET）成倍占用 Redis 连接
  → 池耗尽 → 正常 hit 请求超时 → stale 兜底 → 但全局延迟恶化（p95 210ms）。
- **同步失效的故障放大**（b1）：下单提交后同步 DEL，Redis 故障时每单 +200ms →
  200 TPS 下单在 P1 单行上排队 → 事务持锁变长 → `latest` 查询与回源在 DB 池（25）中排队
  → 全链路 21.9% 失败。异步化后该链条断开。
- **本地旧值同步过期**（b2）：stale 条目固定 TTL 到期 → 100 个 Key 同时无旧值 →
  同时回源 → 信号量排队 200ms 超时 → 81559 次明确拒绝。滑动续期后故障期间
  条目随读取持续有效，拒绝风暴消失（21 次）。
- **k6 口径**：`shared-iterations` 的 `__ITER` 为 per-VU；按 Key 构造请求体必须显式
  分配，不能依赖"全局迭代号"假设（cache-product.js 的幂等键用法不受影响）。

## 15. Trade-offs

- **stale 服务是有界陈旧**：上界 = 上次成功回源/命中 + 2×TTL（滑动续期），只用于
  商品/展示库存；下单依据永远走 MySQL 事务。
- **异步失效引入失效延迟**：健康时队列延迟毫秒级，正常模式陈旧窗口几乎不变；
  故障时可能丢弃（TTL 兜底），换取下单延迟与 Redis 解耦。EXP-07 的
  `CACHE_INVALIDATE_DELAY_MS` 实验开关保留（作用于 worker），陈旧窗口结论仍可复现。
- **合并范围仅限单进程**：多副本下回源数 ∝ 副本数，EXP-13 扩容时需同步评估。
- 随机 TTL 未采用：滑动续期解决故障期批量过期；正常态批量过期由合并吸收
  （roadmap 提示：随机 TTL 不能解决单热点持续问题，本实验以合并+有界回源正面解决）。

## 16. Architecture Decision

- **ADR-015**：读回源统一走保护链路——singleflight 合并 + 信号量（12）有界并发 +
  200ms 有界等待 + 1s 独立超时；超限降级顺序：本地旧值 → 明确 503 拒绝。
- **ADR-016**：缓存失效从同步改为异步（有界队列 1024 + 单 worker，满丢弃）。
  下单响应语义不变（事务已提交），失效保证从"同步必达"变为"尽力 + TTL 兜底"。
- **ADR-017**：本地旧值库（1024 条目）作为故障态第二级缓存，滑动续期 + 半 TTL 写入节流；
  仅在回源受限/Redis 故障时服务。
- Redis 连接池从 20 调到 128：保持有界（4000 QPS × p95 ~18ms 的并发模型下足够），
  消除实验负载下的人为瓶颈。

## 17. Lessons Learned

- **先跑对照组再下结论**：场景 A 初测（池 20）的 stale 数据不是"保护生效"而是
  人为瓶颈；扩大池后对照才干净。基线环境的隐藏瓶颈会污染归因。
- **故障注入要查全链路**：b1 的失败率 21.9% 表面是"Redis 故障"，实际根因是写路径
  同步失效叠加；只盯着读路径指标会误判。
- **滑动续期解决的是"同步过期风暴"**：固定 TTL 的本地旧值库在故障期间会
  周期性集体到期，把拒绝风暴变成节拍器；滑动续期后故障期服务稳定。
- **Prometheus 抓取间隔 10s**：指标查得太快会看到旧值（executed +0 的假象），
  短实验的差分要等一个抓取周期。
- **k6 `__ITER` 是 per-VU**：构造"每个请求不同参数"的场景要用随机或显式分配，
  不要依赖全局迭代号；否则多 VU 会重复请求同一参数（本实验误打误撞构成了
  理想的同 Key 并发穿透对照）。
- **离线镜像构建**：外网不可达时复用既有镜像底座 + `docker cp` 注入二进制 + `commit`
  是可行的迭代手段，但必须记录在报告里保证可追溯（本实验 exp08a→e 均如此构建）。

## 18. Interview Questions

- 为什么合并范围只能单进程？多副本下回源数如何放大？
  → singleflight 是进程内机制；N 副本每个都回源一次。EXP-13 扩容时回源预算
  必须按副本数重算，或用带锁的分布式合并（复杂度更高，本课程不采用）。
- 有界等待 200ms 和独立超时 1s 的区别？为什么两个都要？
  → 有界等待限制"排队等额度"的时间；独立超时限制"拿到额度后执行回源"的时间。
  只有等待上限，慢 DB 会长期占额度；只有执行上限，队列无界堆积。
- stale 与 reject 的分界是什么？为什么展示库存可以 stale、下单不可以？
  → 有旧值且查询允许陈旧 → stale；无旧值或语义不允许 → 503。展示库存的陈旧只影响
  展示，下单正确性由 MySQL 条件更新保证，任何读路径降级都不参与下单判断。
- 异步失效后，下单成功但缓存未删除的窗口内读到旧库存，会超卖吗？
  → 不会：缓存只服务展示接口；下单按 DB 真实库存条件扣减。旧展示值最多存活到
  主 TTL（60s）或失效队列排空。
- 失效队列满丢弃后，缓存陈旧窗口是多少？为什么可以接受？
  → 上界 = 主 TTL（60s）+ 本地旧值 2×TTL（若走 stale）。展示类查询允许该有界陈旧；
  若需要强一致读取，绕过缓存直读 DB（EXP-07 ADR-013 的口径）。
- 为什么滑动续期用"半 TTL 节流"？
  → 避免每次命中都写锁（4000 QPS 下的锁竞争）；条目寿命充足时跳过刷新，
  只剩 < TTL/2 时才延长，写入频率降到每 TTL/2 一次。
