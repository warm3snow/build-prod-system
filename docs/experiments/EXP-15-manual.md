# EXP-15 手工操作手册：持续流量下的发布、回滚与 Schema 兼容

> 目标：正常发布（D1）、坏版本发布与回滚（D2）、Schema expand/contract（D3）、
> 可重复发布流程（D4）。
> 前置：EXP-14 环境运行中（3 副本 + maxSurge 0/maxUnavailable 1 + PDB minAvailable 2）。
> 预期总耗时：约 90 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get deploy order-api -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl -n order-lab get rs | grep order-api        # 记录当前 revision
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
```

## 1. 构建镜像（离线注入方式，网络受限时）

```bash
# 仓库根目录：交叉编译（代码含 channel 字段 + BAD_MODE 开关）
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/order-api-exp15a ./cmd/order-api
# 复用 exp12b 底座注入
CID=$(docker create --name tmp-exp15a order-api:exp12b)
docker cp /tmp/order-api-exp15a $CID:/order-api
docker commit $CID order-api:exp15a && docker rm $CID
# 坏版本镜像（同一二进制，靠 BAD_MODE env 区分行为）
CID=$(docker create --name tmp-exp15bad order-api:exp12b)
docker cp /tmp/order-api-exp15a $CID:/order-api
docker commit $CID order-api:exp15-bad && docker rm $CID
```

## 2. 场景 D1：正常发布（约 15 分钟）

```bash
# 800 rps × 3min 压测中，T+30s 发布 exp15a（含 Schema expand：AutoMigrate 加 channel 列）
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=180s /tmp/fixed-rate.js > /tmp/d1.log 2>&1 &'
sleep 30
kubectl -n order-lab set image deploy/order-api order-api=order-api:exp15a
kubectl -n order-lab rollout status deploy/order-api --timeout=300s
# 验证 expand：新版本带 channel 下单（显式/缺省）
mysql-lab -e "SHOW COLUMNS FROM orders LIKE 'channel';"
```

**完成标志**：发布窗口失败率 ~0.4%（connection refused，非 5xx）；
channel 列存在且默认 'web'；1:1:1 对账成立。

## 3. 场景 D2：坏版本发布与回滚（约 20 分钟）

```bash
# 压测中发布坏版本：注意 set image 与 set env 必须合并为一次 revision！
#（分开执行会产生两个 revision，undo 只回退一步——本实验踩过的坑）
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=800 -e DURATION=180s /tmp/fixed-rate.js > /tmp/d2.log 2>&1 &'
sleep 30
kubectl -n order-lab set image deploy/order-api order-api=order-api:exp15-bad
kubectl -n order-lab set env deploy/order-api BAD_MODE=error
kubectl -n order-lab rollout status deploy/order-api --timeout=300s
# 观察：下单 500 bad_release；release_bad_mode_total 增长；orders_created_total 归零
# 回滚（计时！）：
kubectl -n order-lab rollout undo deploy/order-api
kubectl -n order-lab rollout status deploy/order-api --timeout=300s
# 确认镜像与 env 都回退干净；否则二次 undo / set image
```

**完成标志**：识别窗口 ≤ 2min（错误率 ~3.7% vs 基线 0.4%）；回滚 ~15s；
坏版本窗口 0 脏数据；回滚后下单恢复 201。

## 4. 场景 D3：Schema expand/contract（约 20 分钟）

```bash
# D3a：旧版本（exp12b）在 expand 后的 schema 上读写
kubectl -n order-lab set image deploy/order-api order-api=order-api:exp12b
kubectl -n order-lab rollout status deploy/order-api --timeout=300s
# 下单 + 查询（不带 channel 字段）→ 应 201；DB 中 channel='web'（默认值生效）

# D3b：破坏性收缩反例（先备份！）
mysql-lab -e "ALTER TABLE orders DROP COLUMN channel;"
# 新版本语义 INSERT（含 channel）→ ERROR 1054 Unknown column
# 旧版本下单 → 仍成功（不引用该列）
# 恢复列：
mysql-lab -e "ALTER TABLE orders ADD COLUMN channel varchar(32) NOT NULL DEFAULT 'web' AFTER status;"
```

**完成标志**：expand 兼容（旧代码读写正常）；contract 反例（新代码 1054）；
恢复后数据无损。

## 5. 场景 D4：可重复发布流程（约 15 分钟）

```bash
# 完整跑一遍 release.sh（发布 exp15a；--no-build 复用已有镜像）
./tests/release.sh exp15a --no-build
# 预期：pre-smoke → regression(5单+重放) → 发布 → post-smoke/regression → SLO 检查 OK
# 单独验证门禁：
./tests/release.sh --slo-check
# 回滚入口：
./tests/release.sh --rollback
```

**完成标志**：exit=0 一次通过；slo-check 门禁（5xx > 1% 失败）可触发自动 undo。

## 6. 对账模板

```sql
SELECT (SELECT COUNT(*) FROM outbox_events WHERE status='SENT'),
       (SELECT COUNT(*) FROM inbox_events), (SELECT COUNT(*) FROM order_notifications);
SELECT COUNT(*) FROM idempotency WHERE order_id = 0;
SELECT COUNT(*) FROM orders WHERE channel NOT IN ('web','app','release') OR channel IS NULL;
```

## 本实验要回答的问题

1. 发布窗口的客户端失败为什么在服务端指标里看不见？
2. 坏版本识别用哪个指标、多少时间？回滚为什么只有 ~15s？
3. undo 为什么停在 exp15-bad（无 BAD_MODE）？发布变更为什么要一个 revision？
4. Schema expand 的安全前提是什么？contract 为什么必须与发布分离？
5. 发布可用性（客户端失败率）与 5xx 错误率的区别是什么？
