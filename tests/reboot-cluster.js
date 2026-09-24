// EXP-16：从完全停摆中重组集群 + 开启自动重入。
// rebootClusterFromCompleteOutage 自动选择 GTID 最超前的成员为新主并重入其余成员。
shell.connect('root:flash-root@mysql-ic-1.mysql-ic-hs.order-lab.svc.cluster.local:3306');
print('connected\n');
var cluster = dba.rebootClusterFromCompleteOutage('ordercluster');
print('rebooted\n');
// 成员重启/被驱逐后自动重入（STS 滚动更新、Pod 重建后无需人工 rejoin）
try {
  cluster.setOption('cluster.autoRejoinTries', 10);
  print('autoRejoinTries=10\n');
} catch (e) { print('autoRejoinTries unsupported: ' + e + '\n'); }
print(cluster.status());
