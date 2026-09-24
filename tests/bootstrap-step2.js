// EXP-16 引导第二步：SET PERSIST 一致性 + addInstance（clone）。
for (var i = 0; i < 3; i++) {
  var h = 'mysql-ic-' + i + '.mysql-ic-hs.order-lab.svc.cluster.local';
  shell.connect('root:flash-root@' + h + ':3306');
  var s = session.runSql("SET PERSIST group_replication_consistency='BEFORE_ON_PRIMARY_FAILOVER'");
  print(h + ' consistency persisted\n');
}
shell.connect('root:flash-root@mysql-ic-0.mysql-ic-hs.order-lab.svc.cluster.local:3306');
var cluster = dba.getCluster('ordercluster');
print('got cluster\n');
cluster.addInstance('root:flash-root@mysql-ic-1.mysql-ic-hs.order-lab.svc.cluster.local:3306', {recoveryMethod: 'clone'});
print('ic-1 added\n');
cluster.addInstance('root:flash-root@mysql-ic-2.mysql-ic-hs.order-lab.svc.cluster.local:3306', {recoveryMethod: 'clone'});
print('ic-2 added\n');
print(cluster.status());
