sudo mkdir /tmp/test-etcd
sudo tar xzvf ~/Desktop/etcd-v3.3.12-linux-amd64.tar.gz -C /tmp/test-etcd --strip-components=1
ETCDCTL_API=3 /tmp/test-etcd/etcdctl version