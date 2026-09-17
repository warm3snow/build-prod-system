# EXP-03：事务、并发库存与请求幂等

## 1. 实验目标

验证并发下单的正确性：
- 库存守恒（不超卖、不虚扣）；
- 幂等重放（同键重复提交只产生一个订单）；
- 同键不同参数拒绝（409）；
- 失败路径回滚（无孤儿订单/幂等记录）。

## 2. 背景问题

`SELECT stock → UPDATE stock` 的两步操作在并发下必然超卖；客户端超时重试若服务端无幂等保护，
会产生重复订单。需要证明写路径在并发与重试下保持不变量。

## 3. 初始架构

沿用 EXP-02：order-api（Gin）→ MySQL（GORM），下单走单个数据库事务。

## 4. SLA / SLO

本实验不压测吞吐；验收为正确性不变量：
- 库存永远 >= 0；
- 初始库存 = 剩余库存 + 已提交订单数；
- 同 (user_id, idem_key) 最多一个订单。

## 5. Baseline（改造前缺陷）

| 缺陷 | 现象 | 测试证据 |
|---|---|---|
| OrderModel.ID 缺 autoIncrement | 所有下单 500（Error 1364） | TestIdempotencyConflict 首轮失败 |
| SELECT FOR UPDATE 空记录 | 并发同键 gap lock 死锁（1213） | TestDumpIdemErr：19/20 死锁 |
| 占位记录无 paramHash | 重放误判为冲突 | 诊断测试：10/20 idempotency conflict |

## 6. Hypothesis

insert-first 幂等（先插入带参数哈希的幂等占位行，靠复合主键唯一约束串行化同键并发）＋
死锁有限重试，可以消除死锁并正确判定重放/冲突。

## 7. 实验方案

1. `tryInsertIdem`：先 INSERT (user_id, idem_key, param_hash)；1062 重复键 → 读回记录判定重放/冲突。
2. 重放路径不加 `FOR UPDATE`（幂等行提交后不可变，普通读足够），消除锁序反转。
3. `withTxRetry`：1213（死锁）/1205（锁等待）最多重试 3 次，25ms×attempt 回退。
4. 条件扣库存 `WHERE stock > 0` 保持原子性。
5. 集成测试用唯一用户前缀隔离共享库数据。

## 8. Code Change

- `internal/store/mysql/store.go`：
  - `OrderModel.ID` 加 `autoIncrement`；
  - `CreateOrder` 改造为 insert-first 幂等；
  - 新增 `tryInsertIdem`、`withTxRetry`、`isRetryable`。
- `internal/store/mysql/store_integration_test.go`（新增）：5 个并发/幂等集成测试。

## 9. Kubernetes Change

- 镜像升级为 `order-api:exp03`（仅应用层变更）。

## 10. Load Test

150 并发 HTTP 下单（40 线程）：恰好 100 成功、50 库存不足、库存守恒 OK。

## 11. Failure Injection

事务中途失败（库存不足触发回滚）：无孤儿订单、无残留幂等记录（TestOutOfStockRollback）。

## 12. Observability

无新增；请求日志沿用 EXP-02。

## 13. Results

| 检查项 | 结果 |
|---|---|
| 库存守恒（100 库存 / 200 并发） | 100 成功 + 100 拒绝，库存 0，订单 100 ✅ |
| 库存守恒（HTTP 层 100/150 并发） | created=100, 409=50, conservation OK ✅ |
| 同键幂等重放 | 201→200→200，同 ID，replayed=true ✅ |
| 同键 200 并发高压 | 仅 1 创建，199 重放，库存只扣 1 ✅ |
| 同键不同参数 | 409 idempotency_conflict ✅ |
| 库存不足回滚 | 0 订单行、0 幂等行 ✅ |

## 14. Root Cause

- **1364 缺默认值**：GORM 模型漏 `autoIncrement`。
- **1213 死锁**：`SELECT ... FOR UPDATE` 在空记录上取 gap lock；并发同键插入导致相互等待。
- **重放误判**：幂等占位行提交前不含 paramHash，重放读到空哈希判为冲突。

## 15. Trade-offs

- insert-first 在库存不足时也会先插入幂等占位（同事务回滚，无残留），换取了无死锁的正确并发。
- 重试上限 3 次覆盖实验负载；更高竞争下的重试策略在 EXP-11 与限流/超时联合再评估。
- 幂等记录永久保留（课程内），代价是存储增长；保留期策略后续实验定义。

## 16. Architecture Decision

- ADR-005：幂等采用 insert-first + 复合主键唯一约束，重放读不加锁。
- ADR-006：死锁/锁等待有限重试（3 次），不无限重试。

## 17. Lessons Learned

- 单元模型测试无法发现 SQL 层死锁——必须真实 MySQL 并发测试。
- 共享测试库需要唯一前缀隔离，否则测试互相污染。
- 死锁是数据库并发控制的一部分，业务层必须能安全重试。

## 18. Interview Questions

- 为什么 `SELECT FOR UPDATE` 在空记录上会死锁？
  → 空记录上只能加 gap lock，并发事务互相持有同一间隙锁又等对方的插入意向锁 → 死锁（实测 19/20）。
- insert-first 幂等为什么能避免这个死锁？
  → 直接 INSERT 靠复合主键唯一约束串行化并发，冲突方转读已提交记录，不存在间隙锁相互等待。
- 为什么重放读不需要加锁？
  → 幂等行提交后不可变，快照读即可；加锁反而与占位持有者形成锁序反转，制造新死锁。
- 死锁重试为什么必须有上限？
  → 无限重试放大 DB 负载、占满连接且客户端早已超时；只对可重试错误（1213/1205）重试，最多 3 次。
- 库存守恒不变量如何用一句话验证？
  → 每个 SKU：初始库存 = 剩余库存 + 已提交订单数，且库存非负。
