# EXP-11 手工操作手册：准入控制、时间预算与安全重试

> 目标：过载对照（A1 无准入 vs A2 准入）、临时失败重试放大对照（B1 无预算 vs B2 有预算）。
> 前置：EXP-10 环境运行中（order-lab），order-api 镜像 exp11b。
> 预期总耗时：约 90 分钟（不含 VM 崩溃恢复）。

## 0. 环境确认

```bash
kubectl -n order-lab get pods
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
alias mysql-root='kubectl -n order-lab exec deploy/mysql -- mysql -uroot -pflash-root'
# 压测机（资源与到达率匹配：2 核 / 2Gi，1Gi 会在 3000 rps 下 VU 封顶 ~826 丢迭代）
kubectl -n order-lab apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata: {name: k6-load, namespace: order-lab}
spec:
  restartPolicy: Never
  containers:
    - name: k6
      image: grafana/k6:latest
      imagePullPolicy: IfNotPresent
      command: ["sleep", "infinity"]
      resources:
        requests: {cpu: 500m, memory: 1Gi}
        limits: {cpu: "2", memory: 2Gi}
EOF
kubectl -n order-lab cp tests/load/overload-retry.js k6-load:/tmp/
```

## 1. 构建与部署

```bash
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/order-api-exp11 ./cmd/order-api
docker create --name exp11-tmp order-api:exp10a && docker cp /tmp/order-api-exp11 exp11-tmp:/order-api
docker commit exp11-tmp order-api:exp11b && docker rm exp11-tmp
# 修改 all.yaml image tag 后 apply
kubectl apply -f deploy/k8s/base/all.yaml
```

## 2. 场景 A：过载对照（约 25 分钟）

> 冻结口径：3000 rps × 2min、8:1:1；A1 全关、A2 开 1500/300/200/500ms/1s。

```bash
# A1：关闭全部防线（对照）
kubectl -n order-lab set env deploy/order-api RATE_LIMIT_RPS=0 RATE_LIMIT_BURST=0 MAX_INFLIGHT=0 READ_BUDGET=0 WRITE_BUDGET=0
kubectl -n order-lab rollout status deploy/order-api --timeout=90s
kubectl -n order-lab exec k6-load -- k6 run -e RATE=3000 -e DURATION=2m /tmp/overload-retry.js > /tmp/a1.log 2>&1
# 预期：96%+ 失败、dropped_iterations>0、VU 耗尽；记录到 /tmp/a1.log

# A2：恢复防线
kubectl -n order-lab set env deploy/order-api RATE_LIMIT_RPS=1500 RATE_LIMIT_BURST=300 MAX_INFLIGHT=200 READ_BUDGET=500ms WRITE_BUDGET=1s
kubectl -n order-lab rollout status deploy/order-api --timeout=90s
kubectl -n order-lab exec k6-load -- k6 run -e RATE=3000 -e DURATION=2m /tmp/overload-retry.js > /tmp/a2.log 2>&1
# 服务端分类（NodePort 32261 或 pod 内无 shell，用宿主机）：
curl -s localhost:32261/metrics | grep -E '^http_(requests_total|rate_limited_total|overloaded_total|deadline_exceeded_total)'
```

**完成标志**：A2 dropped≈0、系统错误率 ≤0.1%、429 单独计数、探针全程 200、Pod 0 重启。

## 3. 场景 B：临时失败与重试放大（约 40 分钟）

> 冻结口径：200 rps × 90s，T+25s 注入 P1 行锁 30s。
> 注意：锁注入必须验证「锁真实存在」——`information_schema.innodb_trx` 可见 trx、
> 并发 UPDATE 被阻塞计时。注入用**前台 exec**（容器内 nohup 后台会被 exec 会话回收杀掉）；
> `-e` 语句必须带库名（否则 `No database selected` 直接报错）。

```bash
# B1：无写预算 + 锁等待 1s + 客户端超时 5s（让服务端重试链完整展开）
kubectl -n order-lab set env deploy/order-api WRITE_BUDGET=0
mysql-root -e "SET GLOBAL innodb_lock_wait_timeout=1;"
kubectl -n order-lab rollout restart deploy/order-api && kubectl -n order-lab rollout status deploy/order-api
# 编排：k6 后台 + T+25s 前台持锁 30s（本地 shell，单命令内完成）
(kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=200 -e DURATION=90s -e WRITE_TIMEOUT=5000ms /tmp/overload-retry.js > /tmp/b1.log 2>&1') &
sleep 25
kubectl -n order-lab exec deploy/mysql -- mysql -uroot -pflash-root flash -e "START TRANSACTION; SELECT * FROM inventory WHERE sku='P1' FOR UPDATE; SELECT SLEEP(30); COMMIT;"
wait
# 预期：mysql_tx_retries_total ~400、写 500、在途 503、readyz 偶发失败、迭代 max ~15s

# B2：写预算 1s + 锁等待 2s + 客户端超时 1.5s
kubectl -n order-lab set env deploy/order-api WRITE_BUDGET=1s
mysql-root -e "SET GLOBAL innodb_lock_wait_timeout=2;"
kubectl -n order-lab rollout restart deploy/order-api && kubectl -n order-lab rollout status deploy/order-api
# 同上编排，WRITE_TIMEOUT=1500ms
# 预期：写 504 明确分类、无 500、无在途拒绝、readyz 0 失败、迭代 max ~3s

# 恢复
mysql-root -e "SET GLOBAL innodb_lock_wait_timeout=50;"
```

**完成标志**：B1 与 B2 在「同一故障、同一负载」下失败形态对比成立；
`mysql_tx_retries_total`、500/504/503 分类、readyz 失败次数均有记录。

## 4. 对账模板

```sql
-- 窗口守恒：库存扣减 == 新订单（记录场景前后值）
SELECT sku, stock FROM inventory WHERE sku='P1';
SELECT COUNT(*) FROM orders;
-- 1:1:1
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
SELECT (SELECT COUNT(*) FROM inbox_events), (SELECT COUNT(*) FROM order_notifications);
-- 幂等无孤儿
SELECT COUNT(*) FROM idempotency WHERE order_id = 0;
```

## 本实验要回答的问题

1. 无准入时 3000 rps 过载的失败形态是什么？客户端重试如何放大？
2. 429（限流）与 503（在途）的语义区别？为什么都不计系统错误率？
3. 总时间预算如何取消下游无用工作？B1/B2 的 DB 占用时间差在哪里？
4. 重试放大率如何计算？B1 的完整放大链条有几层？
5. 为什么探针必须豁免限流？豁免后限流健康如何观察？
6. 单实例限流预算在多副本下如何核算？有哪些坑（EXP-13 前置）？
