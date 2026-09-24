shell.connect('root:flash-root@mysql-ic-1.mysql-ic-hs.order-lab.svc.cluster.local:3306');
var cluster = dba.getCluster('ordercluster');
cluster.rejoinInstance('root:flash-root@mysql-ic-2.mysql-ic-hs.order-lab.svc.cluster.local:3306', {interactive: false});
print('rejoin issued\n');
