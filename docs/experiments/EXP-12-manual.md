# EXP-12 手工操作手册：熔断、资源隔离与业务降级

> 目标：慢依赖/持续失败对照（C1 无隔离 vs C2 隔离+熔断）、熔断生命周期与恢复（C3）。
> 前置：EXP-11 环境运行中（order-lab），order-api 镜像 exp12b。
> 预期总耗时：约 70 分钟。

## 0. 环境确认

```bash
kubectl -n order-lab get pods   # 预期多出 dep-sim
alias mysql-lab='kubectl -n order-lab exec deploy/mysql -- mysql -uflash -pflash flash'
# 模拟器控制面（NodePort）
SIM=http://localhost:30099
curl -s $SIM/state     # {"mode":"ok","calls":N,...}
```

## 1. 构建与部署

```bash
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/o ./cmd/order-api
GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /tmp/d ./cmd/dependency-simulator
docker create --name t1 order-api:exp11b && docker cp /tmp/o t1:/order-api && docker commit t1 order-api:exp12b && docker rm t1
docker create --name t2 order-api:exp11b && docker cp /tmp/d t2:/dep-sim \
  && docker commit --change='ENTRYPOINT ["/dep-sim"]' t2 dependency-simulator:exp12a && docker rm t2
kubectl apply -f deploy/k8s/base/all.yaml
```

## 2. 场景 C0/C1/C2：依赖慢响应对照（约 40 分钟）

> 冻结口径：1000 rps × 1min，混合 extra 60% / 商品读 30% / 库存读 5% / 下单 5%；
> sim slow = 每请求 sleep 2s。

```bash
# C0 正常态基线
curl -s -X POST $SIM/control -d '{"mode":"ok"}'
kubectl -n order-lab exec k6-load -- k6 run -e RATE=1000 -e DURATION=1m /tmp/extra-info.js > /tmp/c0.log 2>&1

# C1 无隔离对照（慢依赖耗尽共享资源）
kubectl -n order-lab set env deploy/order-api DEP_ISOLATION=false DEP_POOL_SIZE=0 DEP_TIMEOUT=2s
kubectl -n order-lab rollout status deploy/order-api --timeout=90s
kubectl -n order-lab rollout restart deploy/dep-sim && kubectl -n order-lab rollout status deploy/dep-sim
curl -s -X POST $SIM/control -d '{"mode":"slow","latency_ms":2000}'
kubectl -n order-lab exec k6-load -- k6 run -e RATE=1000 -e DURATION=1m /tmp/extra-info.js > /tmp/c1.log 2>&1
# 预期：核心读通过率 ~16%、http_overloaded_total ~5 万、sim calls ~6000×2s

# C2 隔离+熔断
kubectl -n order-lab set env deploy/order-api DEP_ISOLATION=true DEP_POOL_SIZE=8 DEP_TIMEOUT=200ms
kubectl -n order-lab rollout status deploy/order-api --timeout=90s
kubectl -n order-lab rollout restart deploy/dep-sim && kubectl -n order-lab rollout status deploy/dep-sim
kubectl -n order-lab exec k6-load -- k6 run -e RATE=1000 -e DURATION=1m /tmp/extra-info.js > /tmp/c2.log 2>&1
# 预期：核心通过率 99%+、overloaded=0、sim calls 仅 ~14、circuit_open ~3.6 万
```

**完成标志**：C1 与 C2 的核心通过率、503 计数、模拟器调用数三组对照齐全。

## 3. 场景 C3：熔断生命周期与恢复（约 15 分钟）

> 冻结口径：sim fail（fail_rate=1.0）1000 rps × 2min，T+60s 恢复 mode=ok。

```bash
kubectl -n order-lab rollout restart deploy/dep-sim && kubectl -n order-lab rollout status deploy/dep-sim
curl -s -X POST $SIM/control -d '{"mode":"fail","fail_rate":1.0}'
# 编排：k6 后台 + T+60s 前台恢复（本地 shell）
kubectl -n order-lab exec k6-load -- sh -c 'k6 run -e RATE=1000 -e DURATION=2m /tmp/extra-info.js > /tmp/c3.log 2>&1' &
sleep 60
curl -s -X POST $SIM/control -d '{"mode":"ok"}'
wait
# 服务端观测
curl -s localhost:32261/metrics | grep -E '^dep_(requests_total|circuit_state|circuit_transitions_total|degraded_total)'
curl -s $SIM/state   # calls 应 ≈ ok 计数（恢复后的正常调用）
```

**完成标志**：open/half-open/closed 三态迁移完整；恢复后 ok 计数与模拟器 calls 精确对账；
最终 `dep_circuit_state == 0`。

## 4. 对账模板

```sql
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;
SELECT (SELECT COUNT(*) FROM inbox_events), (SELECT COUNT(*) FROM order_notifications);
SELECT COUNT(*) FROM idempotency WHERE order_id = 0;
```

## 本实验要回答的问题

1. 为什么「慢依赖」比「失败依赖」更危险？无隔离时慢依赖如何饿死核心路径？
2. 隔离池 8 + 有界等待 50ms + 独立超时 200ms 各自解决什么问题？
3. 熔断打开期间模拟器为什么几乎零负载？恢复时为什么探测并发必须是 1？
4. 降级响应为什么是 200 + degraded 而不是 503？如何保证「降级不伪装成完整成功」？
5. 核心路径（库存事务/幂等）为什么不能熔断？依赖故障的边界在哪里？
6. 依赖调用为什么必须继承请求 context（EXP-11 总预算）？
