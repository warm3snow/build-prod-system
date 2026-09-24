# EXP-16 手工操作手册：MySQL 在线高可用与主库切换

> 目标：InnoDB Cluster 部署与 HA 档基线（F1）、主库强杀切换（F2）、一致性核验
> （F3）、旧主回归（F4）、多数派丢失（F5）、全灭恢复。
> 前置：EXP-15 环境；宿主机可运行 docker（Rancher Desktop moby context）。
> 预期总耗时：约 150 分钟（含三次部署迭代踩坑）。

## 0. 关键设计决定（先读）

```text
1. GR 变量不能走命令行/ConfigMap：mysql:8.0 entrypoint 的 --initialize 阶段
   不认插件变量（unknown variable → init 中断 → 半初始化字典 → privilege
   tables 崩溃循环）。命令行只留 server-id/report-host；插件用
   plugin_load_add 预加载；GR 变量由 mysqlsh SET PERSIST。
2. 引导工具：宿主机 mysqlsh（macOS arm64）或 mysql/mysql-operator:latest
   镜像内 mysqlsh 8.0.32（amd64，qemu 可跑）+ docker run --dns <CoreDNS IP>。
3. Router 禁用 entrypoint 自引导（MYSQL_HOST 固定成员单点）：bootstrap 产物
   烘焙进 Secret，直接运行 mysqlrouter --config。
4. 镜像坑：mysql/mysql-router 无 arm64 构建（qemu ~8 倍 CPU，limit 需 2 核）。
```

## 1. 部署 InnoDB Cluster

```bash
kubectl apply -f deploy/k8s/ha/mysql-innodb-cluster.yaml   # STS×3 + hs Service
# 等 3 成员 Running（首次 init 各 ~1min）
kubectl -n order-lab get pods -l app=mysql-ic

# 引导（宿主机经 operator 镜像，DNS 直连）：
docker run --rm --dns $(kubectl -n kube-system get svc kube-dns -o jsonpath='{.spec.clusterIP}') \
  -v $PWD/tests/bootstrap-cluster-run.js:/bootstrap.js \
  --entrypoint bash mysql/mysql-operator:latest -c 'mysqlsh --file /bootstrap.js'
# 失败重跑：先 configureInstance（脚本已含），再 createCluster。
# ic-2 若 join 超时（Timeout on wait for view），单独 rejoin：
#   tests/rejoin-ic2.js（connect 到 PRIMARY 后 rejoinInstance）

# 一致性与多数派保护（每成员 SET PERSIST，重启保留）：
for p in 0 1 2; do
  kubectl -n order-lab exec mysql-ic-$p -- mysql -h 127.0.0.1 -uroot -pflash-root -e \
    "SET PERSIST group_replication_consistency='BEFORE_ON_PRIMARY_FAILOVER'; \
     SET PERSIST group_replication_unreachable_majority_timeout=5; \
     SET PERSIST group_replication_exit_state_action=READ_ONLY;"
done
```

## 2. 部署 Router（直接运行版）与切换应用 DSN

```bash
# 2.1 生成 bootstrap 产物（一次性，重建集群时需重做）：
docker run -d --name router-bootstrap --dns <CoreDNS IP> \
  -e MYSQL_HOST=mysql-ic-0.mysql-ic-hs.order-lab.svc.cluster.local \
  -e MYSQL_PORT=3306 -e MYSQL_USER=root -e MYSQL_PASSWORD=flash-root \
  -e MYSQL_CREATE_ROUTER_USER=0 mysql/mysql-router:8.0
sleep 25 && docker cp router-bootstrap:/tmp/mysqlrouter /tmp/router-bundle && docker rm -f router-bootstrap
kubectl -n order-lab create secret generic mysql-router-secrets \
  --from-file=mysqlrouter.key=/tmp/router-bundle/mysqlrouter.key \
  --from-file=keyring=/tmp/router-bundle/data/keyring \
  --from-file=state.json=/tmp/router-bundle/data/state.json \
  --from-file=router-cert.pem=/tmp/router-bundle/data/router-cert.pem \
  --from-file=router-key.pem=/tmp/router-bundle/data/router-key.pem \
  --from-file=ca.pem=/tmp/router-bundle/data/ca.pem \
  --dry-run=client -o yaml | kubectl apply -f -
# mysqlrouter.conf 放 ConfigMap（deploy/k8s/ha/mysql-router-direct.yaml 内含）
# 注意：conf 内 router_id 每次重新生成时以 bundle 内为准更新 ConfigMap。

# 2.2 部署直接运行版 Router：
kubectl apply -f deploy/k8s/ha/mysql-router-direct.yaml
# 2.3 应用切 DSN + 旧 mysql 退役（all.yaml 已含）：
kubectl apply -f deploy/k8s/base/all.yaml
kubectl -n order-lab scale deploy/mysql --replicas=0   # 单实例退役，PVC 保留存档
# 2.4 补种数据（fresh 集群只有 P1/P2；不补则负缓存回源风暴）：
kubectl -n order-lab cp tests/seed-products.sql mysql-ic-0:/tmp/seed.sql
kubectl -n order-lab exec mysql-ic-0 -- mysql -h mysql-router.order-lab.svc.cluster.local \
  -P 6446 -uflash -pflash flash -e "SOURCE /tmp/seed.sql;"
kubectl -n order-lab exec deploy/redis -- redis-cli FLUSHALL
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/warmup.js' < tests/load/cache-warmup.js
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e SKU_COUNT=10000 /tmp/warmup.js > /tmp/warm.log 2>&1 &'
```

## 3. 场景 F1：HA 档基线（600 rps 冻结）

```bash
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=600 -e DURATION=180s /tmp/fixed-rate.js > /tmp/f1.log 2>&1 &'
# 预期：读 p99 ~77ms / 写 p99 ~337ms / 失败 ~0.001%
```

## 4. 场景 F2：主库强杀切换（核心）

```bash
# 找当前 PRIMARY（谁 read_only=0）：
for p in 0 1 2; do kubectl -n order-lab exec mysql-ic-$p -- mysql -h 127.0.0.1 -uflash -pflash -N -e "SELECT @@hostname, @@read_only;" 2>/dev/null; done
B0=<记录 orders 计数>; LAST=<记录 MAX(id)>
kubectl -n order-lab exec k6-load -- sh -c 'nohup k6 run -e RATE=600 -e DURATION=300s /tmp/fixed-rate.js > /tmp/f2.log 2>&1 &'
sleep 60
kubectl -n order-lab delete pod mysql-ic-<PRIMARY> --force --grace-period=0
# 每 8-10s 采样：成员角色（replication_group_members）+ orders 计数（经 6446）
# 预期：T+1s 内选举完成；T+30s 写满速；总失败 ~3%；0 已提交订单丢失
```

## 5. 场景 F3/F4：一致性与旧主回归

```bash
# F3：kill 前最后订单在新主可读（BEFORE_ON_PRIMARY_FAILOVER）
kubectl -n order-lab exec mysql-ic-0 -- mysql -h mysql-router... -P 6446 ... \
  -e "SELECT id, status FROM orders WHERE id=$LAST;"
# F4：观察被杀成员回归为 SECONDARY（~60-90s，auto-rejoin）
```

## 6. 场景 F5：多数派丢失（必须用暴亡注入！）

```bash
# ❌ 缩容（scale 3→1）是优雅关停：LEAVE → 组收缩 → 单成员组合法可写（不是多数派丢失）
# ✅ 暴亡（无 LEAVE）：docker kill 成员进程
for n in $(docker ps --format '{{.Names}}' | grep -E 'k8s_mysql_mysql-ic-[12]_'); do docker kill -s KILL $n; done
# 预期：T+~20s super_read_only=1；写入明确拒绝（ERROR 2003/1290），无假成功
# 全灭恢复（GR 不自动重组）：
docker run --rm --dns <CoreDNS IP> -v $PWD/tests/reboot-cluster.js:/r.js \
  --entrypoint bash mysql/mysql-operator:latest -c 'mysqlsh --file /r.js'
```

## 7. 对账模板

```sql
-- 经 Router 6446 执行
SELECT (SELECT COUNT(*) FROM outbox_events WHERE status='SENT') sent,
       (SELECT COUNT(*) FROM outbox_events WHERE status='PENDING') pending,
       (SELECT COUNT(*) FROM inbox_events) inbox,
       (SELECT COUNT(*) FROM order_notifications) notif,
       (SELECT COUNT(*) FROM idempotency WHERE order_id=0) orphan;
-- 成员状态
SELECT MEMBER_HOST, MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members;
```

## 本实验要回答的问题

1. RTO 由哪几段组成？为什么 F2b 的 77s 与 F2-final 的 30s 差在 Router 启动路径？
2. 切主瞬间新主为什么会 OOM？「故障态资源预算」怎么算？
3. BEFORE_ON_PRIMARY_FAILOVER 在日志与 F3 核验中如何证明生效？
4. 为什么缩容测不出多数派丢失？暴亡与优雅关停在 GR 里是什么区别？
5. GORM 的 OnConflict{DoNothing} 为什么在 GR 上触发 1869？与 failover 重投如何相互作用？
