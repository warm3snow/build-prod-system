// EXP-16：InnoDB Cluster 引导脚本（宿主机 mysqlsh 经 VM 容器执行，DNS=CoreDNS）。
// 前提：mysql-ic-0/1/2 已以标准参数运行（GR 插件已加载、report_host 已设）。
for (var h of ['mysql-ic-0', 'mysql-ic-1', 'mysql-ic-2']) {
  var uri = 'root:flash-root@' + h + '.mysql-ic-hs.order-lab.svc.cluster.local:3306';
  dba.configureInstance(uri, {interactive: false});
  print(h + ' configured\n');
}
shell.connect('root:flash-root@mysql-ic-0.mysql-ic-hs.order-lab.svc.cluster.local:3306');
print('connected to ic-0\n');
var cluster = dba.createCluster('ordercluster', {memberSslMode: 'DISABLED'});
print('cluster created\n');
// 切主一致性等待：新主就绪前先应用完积压事务（防陈旧读）。
cluster.setOption('cluster.replicationConsistency', 'BEFORE_ON_PRIMARY_FAILOVER');
print('consistency set\n');
cluster.addInstance('root:flash-root@mysql-ic-1.mysql-ic-hs.order-lab.svc.cluster.local:3306', {recoveryMethod: 'clone'});
print('ic-1 added\n');
cluster.addInstance('root:flash-root@mysql-ic-2.mysql-ic-hs.order-lab.svc.cluster.local:3306', {recoveryMethod: 'clone'});
print('ic-2 added\n');
print(cluster.status());
