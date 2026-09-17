# EXP-02 手工操作手册：Kubernetes 最小订单运行底座

> 目标：在 Rancher Desktop k3s 上部署 order-api（Go+Gin+GORM）＋ MySQL（PVC），
> 验证探针、优雅退出、数据持久化与幂等写路径。
> 前置：Go 1.25+、docker（Rancher Desktop 自带）、kubectl 指向 rancher-desktop。
> 预期耗时：约 20 分钟（含镜像构建与 MySQL 首次启动）。

## 0. 环境确认

```bash
kubectl get nodes
# 预期：lima-rancher-desktop Ready（k3s v1.33+）
docker info --format '{{.ServerVersion}}'
# 预期：有版本号输出（Rancher Desktop 的 VM 内 docker 已启动）
kubectl get storageclass
# 预期：local-path (default) 可用 —— MySQL PVC 依赖它
```

## Step 1：确认代码可编译

```bash
cd build-prod-system
go build ./... && go vet ./...
```

## Step 2：构建镜像（注意 tag 规则）

```bash
docker build -t order-api:exp02 .
```

关键约定：**每次改代码必须换 tag**（如 exp02、exp02-v2），
否则 kubelet 会复用同 tag 的旧镜像，改动不生效。

## Step 3：部署

```bash
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab get pods -w
```

预期过程：
- mysql Pod 先 `Pending`（PVC 绑定）→ `ContainerCreating`（镜像拉取/初始化）→ `Running`；
- order-api 因 startupProbe 依赖 DB，等 MySQL ready 后才 ready。

## Step 4：验证端口转发与探针

```bash
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
curl localhost:18080/healthz      # 预期 {"status":"ok"}（存活探针，不查 DB）
curl localhost:18080/readyz       # 预期 {"status":"ready"}（就绪探针，查 DB）
```

## Step 5：验证业务接口

```bash
curl localhost:18080/api/products/P1
# 预期 {"sku":"P1","name":"Flash Phone Case","price_cents":9900}

curl localhost:18080/api/products/P1/stock
# 预期 {"sku":"P1","stock":1000}

curl -i -X POST localhost:18080/api/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: t1' \
  -d '{"user_id":"u1","sku":"P1","qty":1}'
# 预期 201 Created，返回订单 JSON
```

## Step 6：验证幂等重放与冲突

```bash
# 相同 key 再次提交 → 200 + replayed:true，同一订单 ID
curl -i -X POST localhost:18080/api/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: t1' \
  -d '{"user_id":"u1","sku":"P1","qty":1}'

# 同 key 不同参数 → 409 idempotency_conflict
curl -i -X POST localhost:18080/api/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: t1' \
  -d '{"user_id":"u1","sku":"P2","qty":1}'
```

## Step 7：验证数据持久化（删 Pod）

```bash
kubectl -n order-lab delete pod -l app=order-api
# 等待新 Pod ready 后（注意：端口转发会断，需重建）
kubectl -n order-lab port-forward svc/order-api 18080:8080 &
curl localhost:18080/api/products/P1/stock
# 预期：stock 仍是上一步扣减后的值（数据在 MySQL PVC 里）
```

## Step 8：验证优雅退出

```bash
kubectl -n order-lab logs -l app=order-api --tail=5
# 预期最后一行：{"level":"INFO","msg":"order-api stopped"}
```

## 常见坑（实测踩过）

| 现象 | 原因 | 处理 |
|---|---|---|
| order-api CrashLoopBackOff，日志 1064 | MySQL 驱动 multiStatements 限制 | 建表语句逐条执行 |
| 改了代码但行为不变 | 同 tag 复用旧镜像 | 换新 tag 重新 build + apply |
| mysql 一直 Pending | PVC 未绑定/镜像拉取中 | `kubectl describe pvc` 看事件，等 local-path 分配 |
| 端口转发突然断开 | 转发的 Pod 被删/重启 | 重建 port-forward |

## 验收清单

- [ ] 干净 Namespace 下 `kubectl apply` 可重建
- [ ] readiness 就绪前不接流量（startupProbe 生效）
- [ ] 删 Pod 自动恢复，数据仍在
- [ ] 同 Idempotency-Key 重放返回同一订单
- [ ] 同键不同参数返回 409
- [ ] 日志出现 `order-api stopped`（优雅退出）
