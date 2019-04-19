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
	ContextTimeout   = time.Second
	dialTimeout      = 5 * time.Second
	MinimumLeaseTime = int64(5)
	LeaderKey        = "/elected"
)

// TakeLease: Provides Lease
func TakeLease(ctx context.Context, cli *clientv3.Client, duration int64) (lease *clientv3.LeaseGrantResponse) {
	ctx, cancel := context.WithTimeout(ctx, ContextTimeout)
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
	ctx, cancel := context.WithTimeout(ctx, ContextTimeout)
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
		sleepTime := time.Duration(MinimumLeaseTime-2) * time.Second
		time.Sleep(sleepTime)
	}
}

// masterInsert inserts the master_$(hostname) key into etcd store
// periodically with a lease. This will tell which master hosts are
// currently online, so the leader can decide the partition of the
// PM's to the masters.
func MasterInsert(cli *clientv3.Client, hostName, ipaddress string) {

	leaseTime := MinimumLeaseTime

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

func WatchLeaderElection(cli *clientv3.Client, hostName string, pipe chan bool) {
	ctx := clientv3.WithRequireLeader(context.Background())
	watchChan := cli.Watch(ctx, LeaderKey, clientv3.WithPrevKV(), clientv3.WithFilterPut())
	for watchResp := range watchChan {
		if watchResp.Canceled == true {
			err := watchResp.Err()
			zap.L().Error("Error in Watch Stream",
				zap.Error(err),
			)
			break
		}
		pipe <- true
		for _, event := range watchResp.Events {
			zap.L().Info("There is a Leader Event",
				zap.ByteString("New Leader", event.Kv.Value),
				zap.ByteString("Old Leader", event.PrevKv.Value),
			)
		}
	}
}

func SelectLeader(cli *clientv3.Client, hostName string, pipe chan bool) {

	for {
		zap.L().Info("Starting Leader Election Process")
		leaseTime := MinimumLeaseTime

		// First Take a Lease to insert a key
		lease := TakeLease(context.Background(), cli, leaseTime)
		zap.L().Info("Lease Granted for inserting master key into etcd store",
			zap.String("Key", hostName),
			zap.Int64("Duration", leaseTime),
		)

		// Performing an atomic transaction
		ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
		defer cancel()
		ctx = clientv3.WithRequireLeader(ctx)
		resp, err := cli.Txn(ctx).If(
			clientv3.Compare(clientv3.Version(LeaderKey), "=", 0),
		).Then(
			clientv3.OpPut(LeaderKey, hostName, clientv3.WithLease(lease.ID)),
		).Commit()
		if err != nil {
			zap.L().Error("TXN Error", zap.Error(err))
		}

		if resp.Succeeded == true {
			zap.L().Info("Leader is Selected",
				zap.String("Leader Name", hostName),
			)
			// Now keep alive
			go KeepAlive(context.Background(), cli, lease)
			//
			// TODO:
			// StartLeaderProcess()
			var s string
			fmt.Scanln(&s)
		} else {
			zap.L().Info("Leader is Selected, but this is not the selected leader",
				zap.String("Leader Name", hostName),
			)
			_ = <-pipe
		}
	}
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

	// Will provide a list of available masters
	MasterInsert(cli, hostName, ipaddress)

	pipe := make(chan bool)
	go WatchLeaderElection(cli, hostName, pipe)

	go SelectLeader(cli, hostName, pipe)

	var s string
	fmt.Scanln(&s)
}
