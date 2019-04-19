package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// Timeout for context
	contextTimeout   = time.Second
	dialTimeout      = 5 * time.Second
	minimumLeaseTime = int64(5)
)

// TakeLease: Provides Lease
func TakeLease(ctx context.Context, cli *clientv3.Client, duration int64) (lease *clientv3.LeaseGrantResponse) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	lease, err := cli.Grant(ctx, duration)
	if err != nil {
		zap.L().Error("Lease Error", zap.Error(err))
	}
	zap.L().Info("Lease Granted")
	return
}

// InsertKeyWithLease: Inserts key with a lease
func InsertKeyWithLease(ctx context.Context, cli *clientv3.Client, key, value string, lease *clientv3.LeaseGrantResponse) (resp *clientv3.PutResponse) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	resp, err := cli.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		zap.L().Error("Insert Error", zap.Error(err))
	}
	zap.L().Info("Key is inserted")
	return
}

// KeepAlive: Keeps Alive the lease
func KeepAlive(ctx context.Context, cli *clientv3.Client, lease *clientv3.LeaseGrantResponse) {
	for {
		ctx = clientv3.WithRequireLeader(ctx)
		_, err := cli.KeepAliveOnce(ctx, lease.ID)
		if err != nil {
			zap.L().Error("Keep Alive Error", zap.Error(err))
		}
		sleepTime := time.Duration(minimumLeaseTime-2) * time.Second
		time.Sleep(sleepTime)
	}
}

// masterInsert inserts the master_$(hostname) key into etcd store
// periodically with a lease. This will tell which master hosts are
// currently online, so the leader can decide the partition of the
// PM's to the masters.
func MasterInsert(cli *clientv3.Client, hostName, ipaddress string) {

	leaseTime := minimumLeaseTime

	// First Take a Lease to insert a key
	lease := TakeLease(context.Background(), cli, leaseTime)
	zap.L().Info("Lease Granted for inserting master key into etcd store",
		zap.String("Key", hostName),
		zap.Int64("Duration", leaseTime),
	)

	// Now insert the master_$(hostName) key with value of ipadress
	key := "master_" + hostName
	resp := InsertKeyWithLease(context.Background(), cli, key, ipaddress, lease)
	zap.L().Debug("The returned response is", zap.Any("Response", resp))
	zap.L().Info("Key is inserted",
		zap.String("Key", key),
		zap.Any("LeaseId", lease.ID),
	)

	// Now keep alive
	go KeepAlive(context.Background(), cli, lease)

}

func main() {

	// TODO: This values should also be received from config file
	hostName := "praneeth"
	ipaddress := "localhost:8888"

	// Created logger and made the logger pacakge global
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Logger Error")
	}
	zap.ReplaceGlobals(logger)
	defer zap.L().Sync()

	// Trying to connect with the etcd data store
	// TODO: Change the endpoints so it can be accesible from config file
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379", "http://localhost:22379", "http://localhost:32379"},
		DialTimeout: dialTimeout,
	})
	if err != nil {
		zap.L().Error("Etcd Connect Error", zap.Error(err))
	}
	defer cli.Close()

	MasterInsert(cli, hostName, ipaddress)
	var s string
	fmt.Scanln(&s)
}
