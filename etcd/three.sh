/tmp/test-etcd/etcd --name s3 \
  --data-dir /tmp/etcd/s3 \
  --listen-client-urls http://localhost:32379 \
  --advertise-client-urls http://localhost:32379 \
  --listen-peer-urls http://localhost:32380 \
  --initial-advertise-peer-urls http://localhost:32380 \
  --initial-cluster s1=http://localhost:2380,s2=http://localhost:22380,s3=http://localhost:32380 \
  --initial-cluster-token tkn \
  --initial-cluster-state new