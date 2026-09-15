# 部署说明（Rancher Desktop / k3s）

> EXP-02 已验证环境：Rancher Desktop k3s v1.33.5，容器运行时为 VM 内 Docker，
> 宿主机 `docker build` 的镜像对 k3s 直接可见，无需 import。

## 1. 构建镜像（每次代码变更换新 tag）

```bash
docker build -t order-api:exp02-v3 .
# 同步修改 deploy/k8s/base/all.yaml 中 order-api 的 image tag，再 apply
```

注意：同 tag 重建镜像会被 kubelet 复用旧镜像，本地迭代必须换 tag。

## 2. 部署

```bash
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab get pods -w
```

## 3. 本地访问

```bash
kubectl -n order-lab port-forward svc/order-api 18080:8080

curl localhost:18080/healthz
curl localhost:18080/api/products/P1
curl localhost:18080/api/products/P1/stock
curl -i -X POST localhost:18080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: test-1' \
  -d '{"user_id":"u1","sku":"P1","qty":1}'
curl 'localhost:18080/api/orders/latest?user_id=u1'
```

## 4. 清理

```bash
kubectl delete namespace order-lab   # 注意：PVC 删除后数据丢失
```

## 5. EXP-02 验收清单（已全部通过）

- [x] `kubectl apply` 幂等可重建
- [x] readiness 就绪前不接流量（startupProbe）
- [x] 删除 Pod 自动恢复，数据仍在（PVC）
- [x] 相同 Idempotency-Key 返回同一订单（replayed=true）
- [x] 同键不同参数返回 409 idempotency_conflict
- [x] 优雅退出（15s terminationGracePeriod）
