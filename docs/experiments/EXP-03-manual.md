# EXP-03 手工操作手册：事务、并发库存与请求幂等

> 目标：用真实 MySQL 验证「不超卖、幂等重放、同键冲突、失败回滚」四类不变量。
> 前置：EXP-02 环境运行中。
> 预期耗时：约 20 分钟。

## Step 1：打通到 MySQL 的端口转发

```bash
kubectl -n order-lab port-forward svc/mysql 13306:3306 &
```

## Step 2：运行并发集成测试（核心步骤）

```bash
cd build-prod-system
TEST_MYSQL_DSN='flash:flash@tcp(127.0.0.1:13306)/flash?parseTime=true' \
  go test ./internal/store/mysql/ -run 'TestConcurrent|TestIdempotency|TestOutOfStock' -v
```

预期 5 个测试全 PASS：

| 测试 | 验证内容 |
|---|---|
| TestConcurrentNoOversell | 库存 100、并发 200：恰好 100 成功 + 100 库存不足，库存归 0 |
| TestConcurrentIdempotencyHeavy | 同键 200 并发：仅 1 创建 + 199 重放，库存只扣 1 |
| TestConcurrentIdempotency | 同键 50 并发同上 |
| TestIdempotencyConflict | 同键不同参数拒绝，不扣库存 |
| TestOutOfStockRollback | 库存不足回滚：无孤儿订单、无残留幂等行 |

> 未设置 TEST_MYSQL_DSN 时测试会 Skip，不会报错——注意确认真的跑了。

## Step 3：HTTP 层并发验证（可选，验证整条链路）

重置 P1 库存为 100，然后跑并发下单脚本：

```bash
kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash \
  -e "UPDATE inventory SET stock=100 WHERE sku='P1';"

kubectl -n order-lab port-forward svc/order-api 18080:8080 &
```

```python
# 保存为 /tmp/concurrency_check.py 后执行 python3 /tmp/concurrency_check.py
import json, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

BASE = "http://localhost:18080"

def call(key, uid, sku="P1"):
    req = urllib.request.Request(
        f"{BASE}/api/orders",
        data=json.dumps({"user_id": uid, "sku": sku, "qty": 1}).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code

def get_stock():
    with urllib.request.urlopen(f"{BASE}/api/products/P1/stock") as r:
        return json.load(r)["stock"]

before = get_stock()
with ThreadPoolExecutor(max_workers=40) as ex:
    codes = list(ex.map(lambda i: call(f"exp03-batch-{i}", f"exp03-u-{i%20}"), range(150)))
after = get_stock()
created = codes.count(201)
print(f"stock: {before} -> {after}")
print(f"created={created}, out_of_stock={codes.count(409)}")
print("conservation:", "OK" if before - after == created else "VIOLATION")
```

**预期**：`created=100, out_of_stock=50, conservation: OK`。

## Step 4：幂等重放 HTTP 验证

```python
# 同 key 提交三次：201 → 200(replayed) → 200(replayed)，同一订单 ID
# 同 key 换 SKU 提交：409 idempotency_conflict
```

预期：

```text
replay: 201->200->200, same_id=True, replayed=True
conflict: 409, code=idempotency_conflict
```

## 本实验要理解的三个缺陷（课程中的真实发现）

1. **缺 autoIncrement**：GORM 模型 ID 未标自增 → 所有下单 500（Error 1364）。
2. **gap lock 死锁**：`SELECT ... FOR UPDATE` 查不存在的幂等行 → 并发同键时 1213 死锁。
   修复：insert-first 幂等（先插带 paramHash 的占位行，靠复合主键唯一约束串行化）。
3. **重放误判**：占位行不带参数哈希 → 重放读到空哈希判为冲突。

修复后的关键实现要点（可对照代码 `internal/store/mysql/store.go`）：

- `tryInsertIdem`：先 INSERT，1062 重复键 → 读回记录判定重放/冲突；
- 重放路径**不加** `FOR UPDATE`（幂等行提交后不可变，普通读即可，避免锁序反转）；
- `withTxRetry`：1213/1205 最多重试 3 次，25ms×attempt 回退。

## 验收清单

- [ ] 5 个集成测试全 PASS（真实 MySQL，非 sqlmock）
- [ ] 库存守恒：初始 = 剩余 + 已提交订单数
- [ ] 同键 200 并发只有 1 个订单
- [ ] 库存不足回滚无残留
- [ ] 能解释为什么 select-for-update 空记录会死锁
