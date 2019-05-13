GODEBUG=http2debug=1   # enable verbose HTTP/2 debug logs
GODEBUG=http2debug=2   # ... even more verbose, with frame dumps

ETCDCTL_API=3 /tmp/test-etcd/etcdctl del "" --from-key=true
go run ../physical/pm.go