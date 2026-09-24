# EXP-14：应用 Pod/Node 故障与维护中断

## 1. 实验目标

- 在持续流量下区分**优雅退出**（SIGTERM，摘流+排空）与**进程突然中断**（SIGKILL/崩溃），
  分别量化请求级错误窗口、Pod 恢复时间与已提交业务完整性；
- 通过 **Eviction API** 验证 PDB 对自愿中断（节点维护驱逐）的约束；
- 模拟节点维护（cordon+drain）与节点硬故障，验证副本分散与剩余容量；
- 复用成功响应账本与 1:1:1 对账，逐单核验订单不变量。

## 2. 背景问题

EXP-13 回答了「扩容有效吗」，但从未回答「副本故障时服务是否仍可用」：

1. **优雅退出已实现但未在负载下验证**：SIGTERM → 摘流 → 排空 → 退出链路
   在 EXP-02 只做了静态检查；端点摘除竞态（EXP-13 实测 ~2.5s 失败窗口）
   与 15s grace 期在负载下的实际表现没有证据；
2. **突然中断没有任何防护证据**：SIGKILL/崩溃下请求直接落在已死 Pod 上，
   恢复窗口 = readiness 探测 + 重调度 + 启动，从未计时；
3. **PDB 存在（minAvailable 1）但从未触发过**：EXP-13 的 PDB 是静态声明，
   驱逐行为、拒绝语义、与滚动更新的关系都是空白；
4. **入口单点未声明**：实验环境无独立入口控制器（k6 直连 ClusterIP Service），
   单节点数据面本身是单点——必须如实标注，不能装作已解决。

## 3. 故障域模型（本实验冻结）

```text
应用故障域（本实验范围）：
  order-api × 3 副本（固定，删除 HPA——EXP-13 证明 CPU HPA 在 DB 瓶颈下误扩，
  本实验聚焦故障行为，不引入扩缩容变量）
  PDB minAvailable=2（允许 1 个自愿中断；不阻止硬故障与直接删除）
  topologySpreadConstraints：maxSkew=1, whenUnsatisfiable=ScheduleAnyway
    （单节点无实际分散作用，声明存在、效果留待多节点 HA 档验证）

明确排除（范围约束，roadmap 规定）：
  MySQL/Kafka 仍为单实例——数据组件不属于本实验故障目标
  节点级故障在单机环境只能「模拟」，结果标注为模拟档
  入口（Service 数据面）为单点：无独立 Ingress，如实记录
```

## 4. SLA / SLO（冻结）

| 指标 | 目标 | 口径 |
| --- | ---: | --- |
| 故障窗口外错误率 | ≤ 0.1% | 3 副本 800 rps 混合稳态 |
| 优雅退出 | 0 已提交业务丢失 | SIGTERM 场景逐单核验 |
| 优雅退出失败窗口 | ≈ 端点摘除竞态（EXP-13 实测 ~2.5s） | 请求级 5xx 时间窗 |
| 突然中断服务级影响 | ≈ 0（其余 2 副本承接） | 集群 5xx 率 |
| 突然中断 Pod 恢复 | ≤ 90s（探测+重调度+启动） | Pod Ready 时间戳 |
| PDB 约束 | 驱逐第 1 个放行、第 2 个 429 拒绝 | minAvailable=2 |
| 不变量 | 库存守恒、幂等无孤儿、1:1:1 | 全程对账 |

## 5. Baseline（来自 EXP-13）

- 缩容稳定性（3→1 副本，800 rps 中）：97.17% 通过、失败集中在端点摘除窗口 ~2.5s、
  28496 单全部 SENT、0 已提交业务丢失——EXP-14 的对照锚点；
- 3 副本 800 rps 稳态：EXP-13 场景 E4 前段实测，读 P99 ~50ms、写 P99 ~250ms；
- Pod 重调度+启动：EXP-13 实测 30-75s（调度+启动+就绪）。

## 6. Hypothesis

1. 优雅退出（SIGTERM）在负载下失败窗口 ≈ 端点摘除竞态（~2.5s），
   窗口内请求失败但**不产生已提交业务丢失**（在途请求在 grace 期完成）；
2. 突然中断（--force --grace-period=0）的失败窗口更长（含探测/调度/启动），
   但服务级影响被剩余 2 副本吸收，5xx 率不破 0.1% 的倍数级放大；
3. PDB minAvailable=2 时 Eviction API 驱逐第 1 个 Pod 成功、第 2 个被 429 拒绝；
4. 单节点下 drain（节点维护模拟）与硬故障模拟只能「停摆」——Pod 无处可调度，
   如实记录为模拟档限制，不伪装成「已通过节点故障验证」；
5. 全程对账 1:1:1 成立：故障窗口内 k6 收到失败但服务端已提交的订单，
   可由幂等键重试收敛，无孤儿。

## 7. 实验方案

1. **场景 A1（优雅退出）**：3 副本、800 rps × 2min，T+30s `kubectl delete pod`
   （SIGTERM）。记录：k6 失败时间窗、Pod 状态迁移时间戳、旧 Pod 排空日志、
   新 Pod Ready 时间；逐单核验。
2. **场景 A2（突然中断）**：同负载，T+30s `kubectl delete pod --force --grace-period=0`
   （SIGKILL）。同指标对照 A1。
3. **场景 A3（进程崩溃）**：T+30s `kubectl exec ... -- kill -9 1`（容器内崩溃，
   Pod 不删除）。观察 restartPolicy 重启行为与 A2 差异（重启 60-90s vs 删除重建）。
4. **场景 B（Eviction API + PDB）**：直接对 2 个 Pod 依次 POST `/eviction`，
   验证第 1 个放行、第 2 个 429；记录 PDB status 与事件。
5. **场景 C（节点维护/硬故障模拟）**：cordon 节点 → drain（预期 Pod 驱逐后
   Pending，无第二节点）→ uncordon 恢复；再做一次 cordon + force delete 全部
   order-api Pod 模拟硬故障 → 全部 Pending → uncordon。全程标注模拟档。
6. **对账**：outbox/inbox/notif 1:1:1；订单数==库存扣减；幂等无孤儿；
   成功响应账本（k6 201/200 计数）与 DB 订单数核验。

## 8. Code Change

- 无 Go 代码变更。EXP-02 的优雅退出（SIGTERM → `srv.Shutdown`，15s grace）
  与 EXP-11 的客户端重试口径直接复用；本实验的变更全部在 Kubernetes 层。

## 9. Kubernetes Change

- `deploy/k8s/base/all.yaml`：
  - order-api `replicas: 3`（固定副本，实验期间删除 HPA，避免误扩干扰）；
  - PDB `minAvailable: 1 → 2`（3 副本允许 1 个自愿中断，EXP-13 为 1 副本时代的保守值）；
  - order-api 增加 `topologySpreadConstraints`（maxSkew 1 / ScheduleAnyway，
    单节点声明式存在，多节点档生效）。

## 10. Load Test

- `tests/load/fixed-rate.js`：constant-arrival-rate 800 rps、8:1:1 混合、
  2min/场景；dropped_iterations==0 自检；read P99 < 200ms / write P99 < 500ms
  阈值编码 SLO（稳态判定）。

## 11. Failure Injection

| 注入 | 命令 | 语义 |
| --- | --- | --- |
| SIGTERM | `kubectl delete pod <p>` | 优雅退出（摘流+排空） |
| SIGKILL | `kubectl delete pod <p> --force --grace-period=0` | 突然中断（无排空） |
| 进程崩溃 | `kubectl exec <p> -- kill -9 1` | 容器内崩溃（Pod 保留，restartPolicy 重启） |
| Eviction | `curl -X POST /api/v1/namespaces/order-lab/pods/<p>/eviction` | 节点维护驱逐（PDB 约束） |
| 节点维护 | `kubectl cordon && kubectl drain` | 模拟（单节点无第二调度目标） |
| 节点硬故障 | cordon + force delete 全部 order-api Pod | 模拟（同上） |

## 12. Observability

- 时序：`kubectl get pods -w` 时间戳 + `kubectl get events --watch`
  （Scheduled/Pulled/Started/Ready 各阶段耗时）；
- 请求级：k6 失败时间窗 + `http_requests_total{status}`（故障前/中/后分段）；
- 复用：RED、`orders_created_total`、1:1:1 对账、成功响应账本；
- 恢复计时：故障注入时刻 → Pod Ready 时刻（`kubectl get pod -o json` 的
  `status.conditions` lastTransitionTime）。

## 13. Results

实测环境：Rancher Desktop k3s（单机 6 核/16GB，单 Node），order-api exp12b × 3 副本，
PDB minAvailable=2，HPA 已删除。负载：800 rps × 8:1:1（constant-arrival-rate）。

### 场景 A1：优雅退出（SIGTERM，T+30s）

| 指标 | 实测 |
| --- | ---: |
| 请求总数 / 失败 | 96000 / **2**（0.002%） |
| 失败窗口 | ~2s（端点摘除竞态，与 EXP-13 缩容窗口一致） |
| Pod 恢复 | 删除→新 Pod Ready 约 60s（调度+启动+就绪，10s grace 排空无阻塞） |
| 订单增量 vs SENT 增量 | +5028 == +5028，1:1:1 成立 |

- **优雅退出失败窗口极小**：SIGTERM 后新请求已被路由到剩余 2 副本，
  仅端点摘除竞态窗口内的请求失败（2 个，全部可由幂等键重试收敛）。

### 场景 A2：突然中断（SIGKILL --force --grace-period=0，T+30s）

| 指标 | 实测 |
| --- | ---: |
| 请求总数 / 失败 | 96001 / **14**（0.01%） |
| 失败窗口 | ~2-3s（无排空，稍大于 A1） |
| Pod 恢复 | 删除→新 Pod Ready 约 105s（无 grace 排空，重调度+启动） |
| 订单增量 vs SENT 增量 | +4484 == +4484，1:1:1 成立 |

- **SIGKILL 的服务级影响被剩余副本吸收**：失败率 0.01%，不破 SLO 数量级；
  与 A1 的差异只有 12 个请求——突然中断在「多副本+无状态」下并非灾难，
  代价是窗口内请求直接失败（无排空机会）。

### 场景 A3：容器内崩溃（SIGKILL 主进程，Pod 保留）

| 指标 | 实测 |
| --- | ---: |
| 请求总数 / 失败 | 96001 / **44**（0.04%） |
| 恢复路径 | restartPolicy 重启：RESTARTS 0→1，约 40-60s 恢复（**无重调度，IP 不变**） |
| 订单增量 vs SENT 增量 | +7727 == +7727，1:1:1 成立 |

- **崩溃路径与删除重建不同**：容器内 kill 主进程后 Pod 仍在（RESTARTS+1），
  readiness 探针要等 period 5s 才发现并摘流，失败窗口比 A2 大（44 vs 14）；
  但无重调度开销。**注意**：distroless 镜像内无 shell/kill，注入需经宿主机
  docker（`docker kill -s KILL <容器>`），且该路径绕过 kubelet 感知。

### 场景 B：Eviction API + PDB（minAvailable=2）

| 步骤 | 结果 |
| --- | --- |
| 第 1 次驱逐（3→2 healthy） | **201 放行**，Pod 优雅终止后重建 |
| 立即第 2 次驱逐（2→1 会低于 minAvailable） | **429 TooManyRequests**，PDB 状态 `DisruptionAllowed=False, disruptionsAllowed=0` |
| 新副本就绪后（healthy 恢复 3） | `disruptionsAllowed=1`，驱逐恢复允许 |

- **PDB 语义完整验证**：自愿中断被闸门在 minAvailable；恢复后自动放行。
  PDB status 三态（currentHealthy/desiredHealthy/disruptionsAllowed）全程可观测。

### 场景 C：节点维护与硬故障（单节点，模拟档）

| 步骤 | 结果 |
| --- | --- |
| C1 cordon + drain | 全部 Pod（含 MySQL/Kafka）被驱逐后 **Pending**——单节点无第二调度目标；`k6-load` 裸 Pod 需 `--force`（无控制器）；drain 超时 90s 报 global timeout |
| C1 uncordon 恢复 | 60-90s 内全部组件恢复 Running（MySQL/Kafka 等持久化组件数据完好） |
| C2 cordon + force delete 全部 order-api | 3 副本全部 **Pending**（调度域为空）→ **应用全停**，PDB `currentHealthy=0` |
| C2 uncordon 恢复 | 30-60s 内 3 副本 Ready |

- **如实结论**：单节点下「节点故障」=「应用全停 + 数据组件全停」，
  无法验证剩余容量与副本分散收益——**只能标注为模拟档**。
  本实验真正验证的节点级结论是：cordon/drain 的驱逐-恢复闭环可执行、
  持久化数据不受影响。

### 对账

- 全程 SENT == inbox == notif（1:1:1）；PENDING=0；孤儿幂等=0；
- A1/A2/A3 各场景订单增量与 SENT 增量严格一致（+5028/+4484/+7727）。

## 14. Root Cause

- **A1 失败窗口的来源**：endpoint slice 传播与 Pod 终止的秒级竞态
  （EXP-13 缩容同源）——新请求落入已终止 Pod，连接拒绝。10s grace 排空
  让在途请求完成，但竞态窗口无法归零。
- **A3 失败窗口比 A2 大的机制**：容器崩溃后 Pod 对象仍在，endpoint 不摘除；
  只有 readiness 探针（period 5s）发现失败并摘流，期间请求持续落入已死容器。
  A2 的 force delete 直接删除 Pod → endpoint 立即摘除（仍留秒级竞态），
  所以窗口更小。**崩溃恢复的摘流延迟由探针周期决定**。
- **drain 卡住的机制**：驱逐第 1 个 Pod 后 PDB `disruptionsAllowed=0`，
  drain 后续驱逐被 429 拒绝直至超时（90s global timeout）——这是 PDB
  与 drain 的预期交互，但单节点上被驱逐的 Pod 无处调度放大了混乱。
- **单节点 Pending 的本质**：cordon 后调度域为空，无「剩余节点容量」概念；
  硬故障模拟的结果不是「降级可用」而是「全停」，与多节点语义完全不同。

## 15. Trade-offs

- **maxSurge=0/maxUnavailable=1**：峰值副本数不超 3（节点 CPU request 预算
  5.5/6 核不允许 surge），代价是滚动期间可用副本=2（PDB 下限）。发布时
  容量余量由「副本数-1」提供，EXP-15 复用该基线。
- **优雅退出不能消除竞态窗口**：endpoint 摘除是异步传播，客户端重试
  （EXP-11 口径）是吸收窗口的务实选择；追求零失败窗口需要连接排空/延迟摘流，
  复杂度不成比例（EXP-13 已有同结论）。
- **distroless 镜像的注入成本**：无 shell 意味着故障注入只能经宿主机
  docker 绕过 kubelet——这恰好暴露了「容器内崩溃」与「Pod 删除」的监控盲区
  差异；真实环境的崩溃注入建议用 debug 镜像或 chaos 工具（选做）。

## 16. Architecture Decision

- **ADR-029**：order-api 采用固定 3 副本 + 滚动策略 maxSurge=0/maxUnavailable=1
  + PDB minAvailable=2。理由：节点 CPU request 预算（5.5/6 核）不支持 surge；
  PDB 不约束 Deployment 自身滚动更新，发布可用性由滚动策略保证；
  HPA 停用（EXP-13：CPU HPA 在 DB 瓶颈下误扩）至写路径瓶颈显式化。
- **ADR-030**：节点级故障实验在单机环境只做「驱逐-恢复闭环」验证，
  结论标注模拟档；节点级 HA 结论（剩余容量、副本分散）推迟到多节点
  HA 档（EXP-16/17 环境或独立多节点集群）。

## 17. Lessons Learned

- **三种中断是三条不同恢复路径**：优雅退出（SIGTERM，端点竞态 ~2s）、
  突然中断（force delete，竞态稍大）、容器崩溃（探针摘流延迟 5s×n + 重启，
  Pod 对象保留）。生产告警不能只看「Pod 重启」——容器崩溃时 Pod 看似
  Running（RESTARTS+1），只有探针发现后才会摘流。
- **PDB 的边界要用实验钉死**：minAvailable=2 下「驱逐第 1 个放行、
  第 2 个 429、恢复后自动放行」三态齐全；PDB 不阻止 force delete 与
  节点硬故障——可用性保护是「PDB + 滚动策略 + 客户端重试」的组合。
- **drain 会连数据组件一起驱逐**：单节点上 cordon/drain 是核弹级操作
  （MySQL/Kafka 全部 Pending）。EXP-14 范围约束「数据组件不得落在目标节点」
  在单节点上无法满足——这是模拟档与实证档的根本差距，必须写进结论边界。
- **CPU request 预算是滚动更新的隐形约束**：默认 maxSurge 25% 需要 4 个
  1 核副本（4 核），节点仅剩 0.5 核 → 新 RS 全部 Pending 卡死发布。
  requests 反映真实用量（EXP-13）后，任何「峰值副本」假设都要过一遍
  节点可分配预算。
- **裸 Pod 会被 drain --force 直接删除**：k6-load 无控制器，drain 时被删
  导致压测中断。压测工具应至少用 Deployment 或接受「实验前重建」的代价。


## 18. Interview Questions

- 优雅退出与突然中断的失败窗口分别由什么决定？哪个环节主导恢复时间？
  → 优雅退出=端点摘除竞态（~2s）；突然中断=竞态+无排空（略大）；
  容器崩溃=探针摘流延迟（5s×threshold）+重启（20-40s）主导，与删除重建不同路径。
- PDB 保护什么、不保护什么？为什么 PDB 不阻止 `delete --force`？
  → PDB 只闸门 Eviction API（自愿中断，如 drain）；直接删除与节点硬故障
  绕过 PDB。发布可用性由 Deployment 滚动策略保证。
- 单节点环境的「节点故障实验」为什么只能算模拟？要声称节点级 HA 需要什么条件？
  → 单节点 cordon 后调度域为空，Pod 全体 Pending=应用全停，没有「剩余容量」
  可验证；需要多节点 + 副本真实分散 + 入口不在目标节点（HA 实证档）。
- 故障窗口内客户端收到 5xx 但服务端已提交的订单如何收敛？
  → 客户端按原幂等键重试/查询（EXP-03/11 口径），A1/A2/A3 对账证明
  窗口内订单全部已提交且无重复。
