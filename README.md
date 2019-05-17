# How to run source code
## Install Dependencies
1. Install gRPC
```golang
go get -v -insecure google.golang.org/grpc
```
2. Install proto
```golang
go get -u -v -insecure github.com/golang/protobuf/proto
```

3. Install proto compiler and change the path variable
```golang
go get -u -v -insecure  github.com/golang/protobuf/protoc-gen-go
```
```bash
export PATH=$PATH:$GOPATH/bin
```
4. Install etcd client
```golang
go get -insecure -v go.etcd.io/etcd/clientv3
```
5. Install zap
```golang
go get -v -u -insecure go.uber.org/zap
```
6. Install golp
```golang
go get -d -u -v -insecure github.com/draffensperger/golp
```

```bash
echo $GOPATH
LP_URL=http://sourceforge.net/projects/lpsolve/files/lpsolve/5.5.2.0/lp_solve_5.5.2.0_dev_ux64.tar.gz
LP_DIR=$GOPATH/src/github.com/draffensperger/golp/lpsolve
mkdir -p $LP_DIR
curl -L $LP_URL | tar xvz -C $LP_DIR
```

7. Install go.matrix
```golang
go get -insecure -u -v github.com/skelterjohn/go.matrix
```
8. For the source code to run, install [etcd datastore](https://github.com/etcd-io/etcd/releases) and for installation guides follow [play.etcd.io](http://play.etcd.io/install)

