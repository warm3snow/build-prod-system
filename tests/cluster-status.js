shell.connect('root:flash-root@mysql-ic-0.mysql-ic-hs.order-lab.svc.cluster.local:3306');
var c = dba.getCluster('ordercluster');
print(c.status());
