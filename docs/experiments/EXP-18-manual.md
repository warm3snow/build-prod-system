# EXP-18 手工操作手册：独立备份恢复与跨组件对账

> 目标：负载中一致性备份（R1）→ 隔离原环境（R2）→ 全量恢复（R3）→
> PITR 恢复 + 隔离重放（R4）→ 原环境恢复（R5）。
> 前置：EXP-17 环境（InnoDB Cluster + Router + KRaft ×3 运行中）；
> 宿主机有 mysqlbinlog（brew mysql-client）；bin/reconcile 已构建。
> 预期总耗时：约 90 分钟。

## 0. 关键顺序约束（先读，都是实测踩坑）

```text
1. 受控恢复顺序（ADR-039）：DB 就绪 → 导入 → [binlog] → 对账 → order-api
   → relay（建隔离 topic）→ 确认 topic 存在 → consumer（新组入组）。
   consumer 先于 topic 存在入组 → range 分配 0 分区、组 Stable 但不消费、
   无报错、无自动重平衡（kafka-go 实测）——只能重启 consumer。
2. mysql:8.0 镜像不含 mysqlbinlog（精简版）；binlog 重放在宿主机做：
   mysqlbinlog 解析归档 → kubectl exec -i stdin 灌入 DR MySQL。
3. 每个恢复代用新 topic + 新消费组（R3 用 orders-dr-replay/order-consumer-dr，
   R4 删 topic 重建 + order-consumer-dr2）——旧代残留消息盲消费会造成
   Inbox 孤儿。
4. bash 脚本里变量名后不能紧跟全角字符（$TS（ 会变成变量名 "TS（"）。
5. port-forward 到 svc/mysql-router 在本环境报 1129（host cache 怪癖）；
   reconcile 一律 port-forward 到 PRIMARY pod。
```

## 1. 准备

```bash
# 工具与镜像（无代码变更，relay/consumer 复用 exp17a）：
GOTOOLCHAIN=local go build -o bin/reconcile ./cmd/reconcile
command -v mysqlbinlog   # 宿主机需要（brew install mysql-client）
kubectl -n order-lab exec -i k6-load -- sh -c 'cat > /tmp/fixed-rate.js' < tests/load/fixed-rate.js

# PRIMARY 定位与预检（mysqldump 在 GR 主库可用、dump 含 GTID_PURGED）：
kubectl -n order-lab exec mysql-ic-0 -- mysql -uroot -pflash-root -N -B -e \
  "SELECT member_host, member_role FROM performance_schema.replication_group_members"
kubectl -n order-lab exec mysql-ic-<PRIMARY> -- mysqldump -uroot -pflash-root \
  --single-transaction --source-data=2 --set-gtid-purged=ON --triggers \
  --databases flash 2>/dev/null | grep -iE "GTID_PURGED|CHANGE MASTER" | head -2
```

## 2. R1：负载 + 一致性备份（备份点 T_b）

```bash
# 2.1 预热 + 负载（LEDGER=1 逐单记账）：
kubectl -n order-lab exec k6-load -- k6 run -e SKU_COUNT=10000 /tmp/warmup.js
kubectl -n order-lab exec k6-load -- sh -c \
  'nohup k6 run -e RATE=600 -e DURATION=600s -e LEDGER=1 /tmp/fixed-rate.js > /tmp/r1.log 2>&1 &'

# 2.2 T+4min：全量备份（T_b = dump 快照点；负载持续写入中）：
deploy/backup/backup.sh full backup-exp18
#   产物：flash-full-*.sql（含 GTID_PURGED + 位点）、binlogs-*.tar、
#         gtid/master-status 元数据、SHA256SUMS（活跃 binlog 尾部撕裂警告属预期）

# 2.3 守恒基线快照（事务化；差分法与时间点无关，任意自洽时刻皆可）：
kubectl -n order-lab port-forward pod/mysql-ic-<PRIMARY> 16306:3306 &
bin/reconcile -dsn 'flash:flash@tcp(127.0.0.1:16306)/flash?parseTime=true' \
  -mode snapshot > backup-exp18/baseline-snapshot.json
```

## 3. R2：静默归档 + 账本提取 + 隔离原环境（T_FAIL，RTO 计时开始）

```bash
# 3.1 负载结束、lag=0 静默后：最终 binlog 归档（无撕裂警告才算干净）：
kubectl -n order-lab exec mysql-ic-0 -- mysql -uroot -pflash-root -N -B -e \
  "SELECT status,COUNT(*) FROM flash.outbox_events GROUP BY status"   # PENDING=0
deploy/backup/backup.sh binlogs backup-exp18

# 3.2 账本提取到宿主机（故障域外）：
deploy/backup/backup.sh ledger /tmp/r1.log backup-exp18/ledger-r1.txt

# 3.3 隔离（PVC 保留不删；kafka-i/redis/k6-load/监控保留）：
date -u +%H:%M:%S   # = T_FAIL
kubectl -n order-lab scale deploy/order-api deploy/outbox-relay deploy/consumer \
  deploy/mysql-router --replicas=0
kubectl -n order-lab scale sts mysql-ic --replicas=0
```

## 4. R3：全量备份恢复（仅 dump）

```bash
deploy/backup/restore.sh deploy                    # 新 ns order-dr、新卷、GTID ON
deploy/backup/restore.sh import backup-exp18/flash-full-*.sql
#   预期水位 = T_b 快照（含 0~2 条 PENDING：dump 时刻 relay 在途，正常态）

# 对账（port-forward 到 DR MySQL）：
kubectl -n order-dr port-forward deploy/mysql-dr 17306:3306 &
bin/reconcile -dsn 'flash:flash@tcp(127.0.0.1:17306)/flash?parseTime=true' \
  -mode check -baseline backup-exp18/baseline-snapshot.json \
  -ledger backup-exp18/ledger-r1.txt -lost-file backup-exp18/lost-full-restore.txt
#   预期：守恒 PASS；lost = (T_b,T_f] 的账本订单（RPO 实测）；mismatch=0

# 受控恢复写入 + 冒烟（RTO-full 计时终点）：
kubectl -n order-dr scale deploy/order-api --replicas=1
kubectl -n order-dr port-forward svc/order-api 18081:8080 &
curl -s -o /dev/null -w '%{http_code}\n' localhost:18081/api/products/P1     # 200
curl -s -X POST localhost:18081/api/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: exp18-smoke-1' -d '{"user_id":"exp18","sku":"P1","qty":1}'  # 201
#   再发一次同键 → 200 replayed

# 事件重放（先 relay 后 consumer！见第 0 节）：
bin/reconcile -dsn '...' -mode replay-scope        # 确定范围
kubectl -n order-dr exec deploy/mysql-dr -- mysql -uroot -pflash-root -e "
  UPDATE flash.outbox_events SET status='PENDING', attempts=0, sent_at=NULL
   WHERE status='PENDING' OR (status='SENT' AND NOT EXISTS
     (SELECT 1 FROM flash.inbox_events ib WHERE ib.event_id = flash.outbox_events.event_id));"
kubectl -n order-dr scale deploy/outbox-relay --replicas=1
kubectl -n order-dr rollout status deploy/outbox-relay --timeout=120s
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic orders-dr-replay | head -1
#   ↑ 确认 topic 存在（3 分区/RF3/minISR2），再拉 consumer：
kubectl -n order-dr scale deploy/consumer --replicas=1
sleep 20 && bin/reconcile -dsn '...' -mode check    # 预期四表 1:1:1 PASS
#   若 0 分区（--members 显示 #PARTITIONS 0）：delete pod -l app=consumer 重入组
```

## 5. R4：全量 + binlog PITR（RPO=0 档）

```bash
# 5.1 清第一代隔离 topic（残留消息属于上一恢复代）：
kubectl -n order-lab exec kafka-i-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic orders-dr-replay

# 5.2 新卷重来 → 导入 → 重放：
deploy/backup/restore.sh reset                     # 删 ns 含 PVC
deploy/backup/restore.sh deploy
deploy/backup/restore.sh import backup-exp18/flash-full-*.sql
deploy/backup/restore.sh binlog backup-exp18/binlogs-<T_f>.tar
#   宿主机 mysqlbinlog 解析全部 binlog | kubectl stdin 灌入；
#   已执行 GTID 自动跳过——预期水位 = T_f 精确值、GTID 与源库故障前一致

# 5.3 严格对账（PITR 档要求 lost=0）：
kubectl -n order-dr port-forward deploy/mysql-dr 17306:3306 &
bin/reconcile -dsn 'flash:flash@tcp(127.0.0.1:17306)/flash?parseTime=true' \
  -mode check -baseline backup-exp18/baseline-snapshot.json \
  -ledger backup-exp18/ledger-r1.txt -strict-ledger > backup-exp18/r4-final-reconcile.txt
#   预期：verified=全部账本 lost=0 mismatch=0；守恒 PASS；RESULT: PASS

# 5.4 受控顺序恢复（新代新组）：
kubectl -n order-dr scale deploy/order-api --replicas=1 && <冒烟>
kubectl -n order-dr scale deploy/outbox-relay --replicas=1
<确认 topic 存在>
kubectl -n order-dr set env deploy/consumer CONSUMER_GROUP=order-consumer-dr2
kubectl -n order-dr scale deploy/consumer --replicas=1
#   终局：四表 1:1:1（含冒烟订单）PASS
```

## 6. R5：清理与原环境恢复

```bash
kubectl delete namespace order-dr --ignore-not-found      # DR 栈整体下线

# 原环境恢复：GR 全灭重启后成员 OFFLINE，需引导（EXP-16 ADR-035）：
kubectl -n order-lab scale sts mysql-ic --replicas=3
kubectl -n order-lab rollout status sts/mysql-ic --timeout=240s
kubectl -n order-lab run mysqlsh-recovery --rm -i --restart=Never \
  --image=mysql/mysql-operator:latest -- mysqlsh --mysql \
  -h mysql-ic-1.mysql-ic-hs.order-lab.svc.cluster.local -P 3306 \
  -u root -pflash-root -e "dba.rebootClusterFromCompleteOutage()"
kubectl -n order-lab exec mysql-ic-1 -- mysql -uroot -pflash-root -N -B -e \
  "SELECT member_host, member_role, member_state FROM performance_schema.replication_group_members"

kubectl -n order-lab scale deploy/mysql-router deploy/order-api --replicas=2
kubectl -n order-lab scale deploy/outbox-relay deploy/consumer --replicas=1
# 终局对账：port-forward PRIMARY pod → reconcile -strict-ledger；lag=0
```

## 7. 对账器速查（cmd/reconcile）

```bash
bin/reconcile -dsn '...' -mode snapshot                 # 水位快照 JSON（基线）
bin/reconcile -dsn '...' -mode check [-baseline b.json] [-ledger l.txt]
                [-strict-ledger] [-lost-file f] [-v]   # 全项对账
bin/reconcile -dsn '...' -mode replay-scope            # 重放范围 + 重置 SQL
# 语义：lost=账本有 DB 无（全量档预期非零=RPO 实测；strict 时判失败）；
#       mismatch/集合违约/守恒破坏 = 必须为 0；
#       在窗未确认 = DB 有账本无（响应未知，合法状态，信息项）。
```

## 8. 产物清单（backup-exp18/）

```text
flash-full-*.sql          全量备份（GTID_PURGED + binlog 位点）
binlogs-*.tar             binlog 归档（T_b 与 T_f 两份；PITR 用 T_f 静默份）
gtid-*/master-status-*    归档时点元数据
ledger-r1.txt             客户端成功响应账本（17705）
baseline-snapshot.json    守恒基线（事务化快照）
lost-full-restore.txt     全量档丢失订单清单（11128）
r4-final-reconcile.txt    PITR 档终局对账（lost=0）
SHA256SUMS-*              产物校验和
```

## 本实验要回答的问题

1. 全量备份与全量+binlog PITR 的 RPO/RTO 差多少？各自的数字从哪里来？
2. mysqldump 三个关键参数（--single-transaction/--source-data/--set-gtid-purged）
   分别保证什么？GTID 自动跳过为什么能让重放不需要对位点？
3. 隔离 Topic + 新消费组重放为什么必须「每代新开」？复用旧组会怎样？
4. 备份在负载中的真实代价是什么？三个资源预算维度（正常/故障/备份）如何区分？
5. 「成功订单不丢」的判定性证据是什么？为什么 DB 计数不能自证？
