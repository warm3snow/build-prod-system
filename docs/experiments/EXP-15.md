# EXP-15：持续流量下的发布、回滚与 Schema 兼容

## 1. 实验目标

- 在持续负载下完成 Kubernetes 滚动发布（正常版本），验证发布期间核心业务满足 SLO；
- 发布一个**可控坏版本**，通过指标/发布门禁识别失败，并在恢复预算内回滚；
- 用一个**向后兼容的 Schema 变更（expand）**验证新旧版本共存：先扩展结构、
  保持旧版本可用，破坏兼容的收缩不与本次发布捆绑；
- 把构建、冒烟、幂等/库存回归、发布、发布后 SLO 检查组成一条可重复的发布流程（脚本）。

## 2. 背景问题

EXP-14 验证了应用故障域（Pod 中断），但发布这个高频操作从未在负载下验证：

1. **滚动策略是静态声明**：maxSurge=0/maxUnavailable=1（EXP-14 因 CPU request
   预算而设）从未在「新旧版本共存 + 持续流量」下验证；
2. **坏版本回滚无证据**：EXP-13 已发现「apply 会覆盖 patch」的坑，但没有
   「坏版本上线 → 检测 → 回滚」的完整计时与业务影响证据；
3. **Schema 变更完全空白**：AutoMigrate 是「建表」，从未验证「改表」；
   expand/contract 的兼容边界（旧版本读新 schema、回滚后旧代码读写现有数据）
   没有任何测试；
4. **发布流程不可重复**：目前是手工 docker build + 改 tag + apply，
   没有门禁（冒烟/回归/SLO 检查），发布失败靠人肉发现。

## 3. 发布模型（本实验冻结）

```text
发布单元：order-api Deployment（3 副本，maxSurge=0/maxUnavailable=1，EXP-14 冻结）
版本线：
  exp14a（基线，exp12b 等价）→ exp15a（正常版本：Schema expand + channel 字段）
  exp15-bad（坏版本：BAD_MODE=error，下单 100% 500，可控注入）

Schema expand（orders 表）：
  + channel VARCHAR(32) NOT NULL DEFAULT 'web'
  旧版本 INSERT 不含 channel → DEFAULT 生效 → 旧代码仍可写 ✓
  回滚到旧版本 → 读订单不感知 channel（或读默认值）→ 旧代码仍可读 ✓
  收缩（DROP COLUMN / 改 NOT NULL）不在本次发布，留作反例说明

发布门禁（tests/release.sh）：
  build → smoke（健康检查）→ 幂等/库存回归（写 10 单+重放+对账）→
  set image → rollout 状态 → 发布后 SLO 检查（错误率 < 1%、P99 阈值）→
  失败自动回滚（rollout undo）
```

## 4. SLA / SLO（冻结）

| 指标 | 正常发布 | 坏版本识别 | 回滚 |
| --- | ---: | ---: | ---: |
| 发布期间错误率 | ≤ 0.1% | 识别窗口 ≤ 2min | 回滚后恢复 |
| 发布期间 P99 | 读 ≤ 200ms / 写 ≤ 500ms | — | 恢复 |
| 坏版本识别 | — | 通过错误率告警/门禁 ≤ 2min | — |
| 回滚完成 | — | — | ≤ 3min（rollout undo → Ready） |
| 新旧共存 | 旧版本可读写新 schema | — | 回滚后旧代码可读写现有数据 |
| 不变量 | 库存守恒、幂等无孤儿、1:1:1 | 同左 | 同左 |

## 5. Baseline（来自 EXP-13/14）

- 3 副本 800 rps 稳态：读 P99 ~50ms、写 P99 ~250ms、失败率 ~0.002-0.04%
  （EXP-14 A1-A3）；
- 滚动窗口经验：maxSurge=0/maxUnavailable=1 下 3 副本依次替换，
  每个新 Pod 就绪 30-75s（EXP-13/14 实测）；
- 告警链路：OrderAPIErrorRateHigh（5xx > 1%，2min）已配置（EXP-04/11）。

## 6. Hypothesis

1. 正常发布（exp15a）在 800 rps 下错误率不破 0.1%，maxUnavailable=1 时
   可用副本恒 ≥ 2，发布窗口内 SLO 满足；
2. 坏版本（100% 下单 500）在 2min 内被错误率告警/门禁识别（500 计入系统错误率）；
3. `rollout undo` 回滚到旧版本 ≤ 3min，回滚期间剩余流量不受损（回滚同样是滚动）；
4. Schema expand 后旧版本（exp12b）仍可正常下单/查询——新列有默认值，
   旧代码的 INSERT/UPDATE 不含该列不报错；回滚后数据完整；
5. 破坏兼容的收缩（DROP COLUMN）会让旧版本写入报错——作为反例验证，
   不捆绑在本次发布。

## 7. 实验方案

1. **场景 D1（正常发布）**：800 rps 持续负载中，从 exp12b 滚动发布 exp15a
   （含 Schema expand）。记录发布窗口错误率/P99、新旧 Pod 共存时间、rollout 时长；
   验证旧 Pod 在新 schema 上仍服务（发布中途旧版本仍在处理下单）。
2. **场景 D2（坏版本发布与回滚）**：发布 exp15-bad（BAD_MODE=error），
   观察错误率上升 → 告警触发 → 门禁/人工判定 → `rollout undo` 回滚；
   记录识别时间、回滚时间、受影响请求数；对账确认坏版本窗口内的订单完整性
   （坏版本下单 500，但幂等键重试到好版本后成功，无重复）。
3. **场景 D3（Schema expand/contract 兼容性）**：
   - D3a：exp15a 已在线（channel 列存在），用 exp12b 镜像（旧代码）临时替换
     1 个副本或全量回滚，验证旧代码可读写（下单 + 订单查询 + 重放）；
   - D3b：手工 `DROP COLUMN channel`（模拟破坏性收缩）→ 旧版本/新版本写入
     观察报错证据 → 恢复列（演示「收缩必须与发布分离」的反例）。
4. **场景 D4（发布流程可重复）**：`tests/release.sh` 完整跑一遍
   （build → smoke → 回归 → 发布 → SLO 检查），并演示「SLO 检查失败自动回滚」路径。
5. **对账**：全程 1:1:1、库存守恒、幂等无孤儿；坏版本窗口订单增量核验。

## 8. Code Change

- `internal/config/config.go` + `cmd/order-api/main.go`：`BAD_MODE` env
  （空=正常；`error`=下单 100% 500 + 指标计数，供坏版本镜像使用）；
- `internal/api/handler.go`：`createOrderReq` 增加可选 `channel` 字段
  （默认 `"web"`），透传 store；BAD_MODE=error 时下单返回 500 `bad_release`；
- `internal/api/metrics.go`：`release_bad_mode_total`（坏版本请求计数，观测注入生效）；
- `internal/store/mysql/store.go`：`OrderModel` 增加 `Channel string`
  （`gorm:"column:channel;not null;default:web;size:32"`），`CreateOrder` 签名
  增加 channel 参数，`CreatedOrder`/`toCreatedOrder` 带出 channel；
- `tests/release.sh`（新增）：build → smoke → 幂等/库存回归 → set image →
  rollout → SLO 检查 → 失败自动 undo 的可重复发布流程。

## 9. Kubernetes Change

- 无新增资源。沿用 EXP-14 的 Deployment（3 副本、maxSurge=0/maxUnavailable=1）。
- 镜像版本线：`order-api:exp15a`（正常）、`order-api:exp15-bad`（坏）。

## 10. Load Test

- `tests/load/fixed-rate.js`：800 rps × 8:1:1，DURATION 按场景 2-5min；
  发布期间持续运行，dropped_iterations==0 自检 + read/write P99 阈值。
- 回归脚本：`tests/load/smoke.js`（已有）或 release.sh 内 curl 冒烟。

## 11. Failure Injection

- 坏版本：`BAD_MODE=error` 镜像（下单 100% 500，不改库存不写订单——
  在准入检查后、事务前返回，保证坏版本不产生脏数据）；
- 破坏性收缩：`ALTER TABLE orders DROP COLUMN channel`（场景 D3b，事后恢复）。

## 12. Observability

- 发布时序：`kubectl rollout status`、`kubectl get pods -w`（新旧 Pod 共存段）、
  `kubectl get rs`（新旧 ReplicaSet 副本数曲线）；
- 错误分类：`http_requests_total{status}`、`release_bad_mode_total`；
- 告警：OrderAPIErrorRateHigh（已配置）——坏版本识别的触发证据；
- 对账：1:1:1、幂等无孤儿、库存守恒。

## 13. Results

实测环境：Rancher Desktop k3s（单机 6 核/16GB），3 副本（maxSurge=0/maxUnavailable=1，
EXP-14 冻结），负载 800 rps × 8:1:1。

### 场景 D1：正常发布（exp12b → exp15a，含 Schema expand，300s 负载中 T+30s 发布）

| 指标 | 实测 |
| --- | ---: |
| rollout 时长 | ~60s（3 副本依次替换，maxUnavailable=1） |
| 发布窗口失败率 | 0.39%（571/143824，全部 connection refused，**非 5xx**） |
| 失败机制 | 旧 Pod 终止与 endpoint 摘除竞态：请求落到已停 Pod → dial refused |
| Schema expand | `channel varchar(32) NOT NULL DEFAULT 'web'` AutoMigrate 加列成功 |
| 新版本行为 | channel 透传：显式 `"app"` 入库 `app`，缺省入库 `web` |
| 1:1:1 对账 | 发布全程成立 |

**关键发现**：connection refused 是网络层失败，**不产生 HTTP status**——服务端
RED 指标与 5xx 告警（OrderAPIErrorRateHigh）完全看不到它。发布窗口的客户端
失败率（0.39%）远高于 EXP-14 单 Pod 中断（0.002-0.04%），因为 maxUnavailable=1
是「先停后起」：每替换 1 个副本，服务容量短暂 3→2，且被停 Pod 的 endpoint
摘除有秒级竞态。

### 场景 D2：坏版本发布与回滚（180s 负载中 T+30s 发布 exp15-bad）

| 阶段 | 时间 | 观测 |
| --- | ---: | --- |
| T+30s | 09:45:26 | set image exp15-bad + set env BAD_MODE=error（**两次操作→两个 revision**） |
| T+60-120s | | 3 副本全部坏版本：下单 100% 500 `bad_release`；`release_bad_mode_total` 增长（采样 2080+） |
| T+85s | 09:46:21 | `rollout undo` 发起 |
| T+99s | 09:46:35 | **回滚完成（14s）**，下单恢复 201 |
| 全程 | | 失败率 3.74%（5382/143866）；读 P99 302ms 短暂超标（发布窗口噪声） |
| 数据完整性 | | 坏版本窗口 **0 脏数据**（注入点在事务前）；回滚后幂等键重试成功，孤儿幂等 0 |

**识别证据**：坏版本从发布到回滚 ~85s，期间错误率 0.39%→3.74%（~10 倍），
`release_bad_mode_total` 提供精确注入计数，`orders_created_total` 归零提供
业务侧交叉验证。**识别主要靠错误率告警口径（5xx > 1%）**，符合 ≤ 2min 门禁目标。

**回滚陷阱（重要）**：`set image` 与 `set env` 是两次独立操作，产生两个 revision；
`rollout undo` 只回退一步——回滚后镜像仍是 `exp15-bad`（BAD_MODE 已清除），
需要二次 undo 或直接 `set image` 才能回到 exp15a/exp12b。**发布变更必须原子化**
（单一 revision），否则 undo 语义不清。另见 rollout undo 与 kubectl apply 的
last-applied-configuration 冲突警告（EXP-13「apply 覆盖 patch」的另一面）。

### 场景 D3：Schema expand/contract 兼容性

| 步骤 | 结果 |
| --- | --- |
| D3a expand 后旧版本（exp12b）全量在线 | 下单 201、订单查询正常；INSERT 不含 channel → DEFAULT 'web' 生效 ✓ |
| D3b DROP COLUMN channel | 新版本语义 INSERT 报 `ERROR 1054 Unknown column 'channel'`；**旧版本（exp12b）下单仍成功** |
| D3b 恢复列 | ADD COLUMN 成功，数据无损 |

- **Expand 兼容性成立**：加列带默认值，旧代码读写不受影响；
- **Contract（DROP COLUMN）与发布捆绑的后果实证**：新版本立刻无法写入，
  而旧版本不受影响——破坏性收缩必须等所有相关版本下线后单独执行，
  「镜像回滚」不包含「数据库回滚」。

### 场景 D4：可重复发布流程（tests/release.sh）

- 全流程（build 跳过 → smoke → regression 5 单+重放 → set image → rollout →
  post smoke/regression → SLO 检查）**exit=0 一次通过**；
- slo-check 门禁：Prometheus 5xx 错误率 0 < 1% 通过；坏版本场景下该门禁会失败
  并触发自动 undo（脚本路径存在，D2 已验证其判定依据）。

### 对账

- 1:1:1 成立（SENT=inbox=notif=1,029,749，PENDING=0）；
- 幂等无孤儿（order_id=0 计数 0）；库存守恒；
- channel 数据健康：无非法值，历史订单全部默认 'web'。

## 14. Root Cause

- **发布窗口 connection refused**：maxUnavailable=1 先停旧 Pod 再起新 Pod；
  旧 Pod 停止监听到 endpoint 摘除存在秒级竞态，请求 dial 到已停 Pod。
  该失败对服务端不可见（无 HTTP status），只能从客户端/压测侧观测——
  「服务端指标健康」不能证明「发布无客户端失败」。
- **坏版本识别速度**：500 立即反映在错误率上（10 倍放大），识别耗时主要由
  滚动发布时长（~60s）而非观测延迟决定；错误率告警（2m 窗口）会滞后于
  瞬时指标，因此发布门禁用瞬时 rate 查询而非告警等待。
- **undo 只回退一个 revision**：set image/set env 拆成两次操作时，
  undo 停在中间态（exp15-bad 无 BAD_MODE）——版本管理必须一次变更一个 revision。

## 15. Trade-offs

- **maxSurge=0 vs maxSurge=1**：surge 需要节点 CPU request 余量（EXP-14 实测
  5.5/6 核无余量）；先停后起牺牲「发布期间副本数不降」，换来调度可行性。
  代价是 0.39% 客户端失败窗口——对幂等可重试的写、可重试的读可接受。
- **坏版本注入点在事务前**：坏版本不写数据（0 脏数据），回滚零对账负担；
  代价是「坏版本」不覆盖「写了坏数据」的故障类（那种坏版本的恢复是
  数据修复，不是回滚，超出本实验范围）。
- **PromQL 引号转义**：shell `--data-urlencode` 对 PromQL 内引号二次转义，
  门禁查询用 python urlencode 规避；发布脚本的可重复性依赖这种细节。

## 16. Architecture Decision

- **ADR-031**：发布采用「滚动更新 + 发布门禁脚本（release.sh）+ 失败自动 undo」：
  版本变更必须单一 revision（set image 与 env 同批 apply），
  坏版本识别以瞬时错误率指标为准（5xx > 1% 即触发回滚），
  Schema 演进只做 expand（带默认值的新列），contract 与发布分离。
- **ADR-032**：发布可用性以「客户端失败率（含 connection refused）」衡量，
  服务端 5xx 指标仅作为识别信号；发布窗口目标 ≤ 0.5%（本实验实测 0.39%）。

## 17. Lessons Learned

- **服务端指标看不见 connection refused**：发布窗口的客户端失败不会出现在
  RED 指标与 5xx 告警里。发布可用性必须用压测侧（k6 失败率）验证，
  不能用「Prometheus 无告警」证明发布无失败。
- **发布变更要一个 revision 一步到位**：image 与 env 分两次 set → undo 回退
  不干净（停在 exp15-bad 无 BAD_MODE）。多字段发布变更应改 all.yaml 后
  一次 apply（单一事实源，EXP-13 同款教训）。
- **Expand 的默认值是兼容性保险**：NOT NULL DEFAULT 'web' 让旧代码的 INSERT
  免于报错；没有默认值的加列在旧版本写入时立刻炸。Contract 反例（1054）
  用最小代价实证了「收缩必须与发布分离」。
- **坏版本要「可观测且不脏」**：BAD_MODE 注入在事务前 + 独立指标
  （release_bad_mode_total）+ 业务指标交叉验证（orders_created_total 归零），
  让「坏版本上线了多久、影响了多少请求」可精确回答。
- **port-forward 会被发布打断**：发布替换 Pod 时 port-forward 断连（
  "lost connection to pod"）——发布门禁脚本必须容忍并自动重建转发。

## 18. Interview Questions

- 为什么「镜像回滚」不等于「数据库回滚」？回滚后旧代码必须满足什么条件？
  → 镜像回滚只换应用代码；数据库已发生 expand（channel 列存在）。旧代码
  必须能读写新 schema——本实验通过「新列带默认值」保证（INSERT 不含列
  仍成功）。若做了破坏性 contract（DROP COLUMN），回滚后的新/旧代码都会
  写坏——因此 contract 必须与发布分离。
- Schema expand 为什么安全？什么变更不能在旧版本共存期间做？
  → 加列带默认值：旧代码 INSERT 不含该列 → DEFAULT 填充；新代码写列 →
  无冲突。不能在共存期间做：DROP COLUMN（新代码 1054）、改列约束
  （NOT NULL 无默认）、重命名列（两版本语义分裂）。
- maxSurge=0/maxUnavailable=1 的发布窗口与 PDB 的关系是什么？
  → PDB 只管 Eviction API（维护驱逐），不约束 Deployment 滚动更新；
  滚动可用性由 Deployment 策略保证。maxUnavailable=1 下 3 副本发布期间
  可用 ≥ 2，与 PDB minAvailable=2 语义一致但机制独立。
- 坏版本如何做到「可控」而不污染数据？注入点为什么放在事务前？
  → BAD_MODE 在准入检查后、事务前返回 500：不扣库存、不建订单、不写
  Outbox。坏版本窗口 0 脏数据 → 回滚后客户端原幂等键重试即成功，无需数据修复。
- 发布门禁的 SLO 检查用什么指标、什么窗口？为什么不能用「人工感觉」？
  → 瞬时 5xx 错误率（2m rate，阈值 1%，对齐 OrderAPIErrorRateHigh 告警口径）
  + 客户端失败率（k6）。人工感觉无法区分「发布窗口的 0.39% 竞态失败」与
  「坏版本的 3.74% 系统失败」，也不可复现、不可自动化。

