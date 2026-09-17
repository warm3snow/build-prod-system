# EXP-06：MySQL 查询、索引与连接预算

## 1. 实验目标

- 固定一条真实慢查询，分析执行计划与索引设计；
- 测量连接池配置对吞吐与延迟的影响，形成连接预算。

## 2. 背景问题

订单列表深分页是典型慢查询；连接池大小无依据，后续扩容可能压垮 MySQL。

## 3. 初始架构

- 数据：orders 44.8 万行（含 heavy-user-1 的 20 万订单）。
- 索引：`idx_orders_user_created (user_id, created_at)`。
- 查询：`SELECT ... WHERE user_id=? ORDER BY created_at DESC LIMIT 10 OFFSET N`。

## 4. SLA / SLO

| Metric | Target |
|---|---:|
| 读 P99 | ≤ 200ms |
| 系统错误率 | ≤ 0.1% |

## 5. Baseline

| 查询 | 耗时 | 执行计划 |
|---|---:|---|
| 深分页 OFFSET 150000（有索引） | 80.7ms | ref + Backward index scan，扫 22.4 万行 |
| 深分页 OFFSET 150000（无索引） | 132.6ms | ALL 全表扫 44.7 万行 + Using filesort |
| 连接池 25（EXP-02 默认） | — | 未测量 |

## 6. Hypothesis

1. 深分页的 OFFSET 跳过是根本问题，游标分页（keyset）可以常数级翻页；
2. 连接池不是越大越好：太小排队，太大浪费连接且增加 MySQL 负担。

## 7. 实验方案

1. EXPLAIN + profiling 对比：有索引深分页 / 无索引 / 游标分页。
2. 删除并重建索引，验证写入路径与重建代价。
3. 2000 QPS 恒定负载下，DB_MAX_OPEN_CONNS = 10 / 25 / 50 三轮对照。
4. 新增连接池指标（db_pool_*），Prometheus 采集。

## 8. Code Change

- `internal/config/config.go`：DB_MAX_OPEN_CONNS / DB_MAX_IDLE_CONNS 环境变量。
- `internal/observability/dbstats.go`（新增）：连接池 collector（open/in_use/wait_count/wait_duration）。
- `cmd/order-api/main.go`：连接池参数化 + 注册 collector。

## 9. Kubernetes Change

- 镜像 order-api:exp06b；连接池通过 env 注入（最终 25/10）。

## 10. Load Test

恒定 2000 QPS × 3 分钟 × 3 组连接池配置。

## 11. Failure Injection

不适用。

## 12. Observability

- 新增 db_pool_open_connections / db_pool_in_use_connections /
  db_pool_wait_count_total / db_pool_wait_duration_seconds_total。

## 13. Results

### 查询与索引

| 方案 | 耗时 | 扫描行数 | 结论 |
|---|---:|---:|---|
| 深分页 OFFSET 150000（有索引） | 80.7ms | 22.4 万（index scan） | OFFSET 天然要跳过 |
| 深分页 OFFSET 150000（无索引） | 132.6ms | 44.7 万（ALL + filesort） | 索引仍是必要非充分 |
| 游标分页（keyset，WHERE (created_at,id)<(...)） | 0.4ms | ≤10 | **200 倍提升** |

### 连接池对照（2000 QPS）

| MaxOpenConns | 送达 QPS | 错误率 | p90 | p95 | 等待计数 |
|---|---:|---:|---:|---:|---:|
| 10 | 1999 | 0% | 10.9ms | 34.0ms | 排队可见 |
| 25 | 2000 | 0% | 2.3ms | 7.6ms | 440（低） |
| 50 | 2000 | 0% | 3.0ms | 11.6ms | 无改善 |

## 14. Root Cause

- 深分页：OFFSET N 必须扫描并丢弃前 N 行，索引无法消除；游标分页以最后一条记录的
  (created_at, id) 作为锚点，索引直接定位。
- 连接池 10：写并发突发时连接耗尽进入等待队列 → p95 抬升到 34ms。
- 连接池 50：MySQL Threads_connected 未升（写入并发有限），多余连接无收益，
  还增加每连接的 MySQL 线程成本。

## 15. Trade-offs

- 游标分页不能跳页（只能上一页/下一页），业务需接受；深分页场景可保留首页直跳。
- 连接数 25 是当前负载甜点位；扩容后总连接 = 副本数 × 25，须控制在
  MySQL max_connections 的 60% 水位内（见连接预算）。

## 16. Architecture Decision

- ADR-011：订单列表查询采用游标分页；保留 OFFSET 接口仅用于小 offset 场景。
- ADR-012：连接池预算：单副本 25 连接；全局预算 = N副本 × 25 ≤ MySQL 上限的 60%，
  扩容/发布/新消费者纳入同一预算（EXP-13 校验）。

## 17. Lessons Learned

- EXPLAIN 的 rows 是估算，profiling 实测才是结论；两者必须一起看。
- 连接池实验要同负载、同压测机对照，否则波动会掩盖结论。
- prometheus 指标别同时用 promauto 和自定义 collector 注册同一名字（双重注册冲突）。

## 18. Interview Questions

- OFFSET 100000 为什么慢？索引帮不上忙的原因是什么？
  → OFFSET 语义是「扫描并丢弃前 N 行」，索引降低每行成本但消除不了跳过 N 行的工作量（实测扫 22.4 万行）。
- 游标分页为什么能 200 倍提速？它的代价是什么？
  → keyset 以上页最后一条 (created_at,id) 为锚点，索引 seek 直达只扫 ≤10 行（0.4ms）；代价是不能跳页。
- 连接池 10 vs 25 vs 50 各暴露什么问题？
  → 10 池耗尽排队（p95 34ms）、25 甜点（7.6ms）、50 无收益（MySQL 并发未升）；池等待指标说了算。
- 扩容 4 副本后连接预算怎么算？
  → 4×25=100 ≤ max_connections 的 60% 水位；发布期新旧副本并存与 Relay/Consumer 共享同一预算（EXP-13 校验）。
