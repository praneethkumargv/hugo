package main

import (
	"context"
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
	ControllerPort   = 8080
	LeaderPort       = 8081
)

// masterInsert inserts the master_$(hostname) key into etcd store
// periodically with a lease. This will tell which master hosts are
// currently online, so the leader can decide the partition of the
// PM's to the masters.
// KEY: master_$(hostName) VALUE: ipaddress
func MasterInsert(cli *clientv3.Client, hostName, ipaddress string) {

	zap.L().Debug("Trying to insert master_$(hostname) key in the etcd data store")
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
	zap.L().Info("Master Key is inserted and it's lease will be updated periodically")
}

func WatchLeaderElection(cli *clientv3.Client, hostName string, pipe, mast, lead chan bool) {
	zap.L().Debug("Trying to watch Leader Election")
	ctx := clientv3.WithRequireLeader(context.Background())
	zap.L().Debug("Sending Watch request for watching the Leader Key",
		zap.String("LeaderKey", LeaderKey),
	)
	watchChan := cli.Watch(ctx, LeaderKey, clientv3.WithPrevKV(), clientv3.WithFilterPut())
	for watchResp := range watchChan {
		zap.L().Debug("Observed a watch event for leader key",
			zap.String("LeaderKey", LeaderKey),
		)
		if watchResp.Canceled == true {
			err := watchResp.Err()
			zap.L().Error("Error in Watch Stream",
				zap.Error(err),
			)
			break
		}
		pipe <- true
		mast <- true
		lead <- true
		for _, event := range watchResp.Events {
			zap.L().Info("There is a Leader Event",
				zap.ByteString("New Leader", event.Kv.Value),
				zap.ByteString("Old Leader", event.PrevKv.Value),
			)
		}
	}
}

// Leader KEY: /elected VALUE: hostName
func SelectLeader(cli *clientv3.Client, hostName string, pipe, lead chan bool) {
	zap.L().Debug("Master started trying to become Leader")
	for {
		zap.L().Info("Starting Leader Election Process")
		leaseTime := MinimumLeaseTime

		zap.L().Debug("Trying to generate a lease id")
		// First Take a Lease to insert a key
		lease := TakeLease(context.Background(), cli, leaseTime)
		zap.L().Info("Lease Granted for inserting master key into etcd store",
			zap.String("Key", hostName),
			zap.Int64("Duration", leaseTime),
		)

		zap.L().Debug("Trying to perform an atomic transaction for becoming leader")
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
		zap.L().Debug("Atomic Transaction Performed")

		if resp.Succeeded == true {
			zap.L().Info("Leader is Selected",
				zap.String("Leader Name", hostName),
			)
			// Now keep alive
			go KeepAlive(context.Background(), cli, lease)
			//
			// TODO:
			StartLeaderProcess(cli, lead, hostName)
		} else {
			zap.L().Info("Leader is Selected, but this is not the selected leader",
				zap.String("Host Name", hostName),
			)
			_ = <-pipe
			_ = <-lead
		}
	}
}

func main() {

	// TODO: This values should also be received from config file
	const hostName = "praneeth"
	const ipaddress = "localhost"

	// Created logger and made the logger pacakge global
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Logger Error")
	}
	zap.ReplaceGlobals(logger)
	defer zap.L().Sync()

	zap.L().Debug("Given HostName and IPaddress",
		zap.String("Host Name", hostName),
		zap.String("IP Address", ipaddress),
	)

	zap.L().Debug("Trying to connect with the etcd data store")
	zap.L().Debug("The given endpoints of the etcd data store are")
	clientendpoints := []string{"http://localhost:2379", "http://localhost:22379", "http://localhost:32379"}
	for _, endpoint := range clientendpoints {
		zap.L().Debug("Endpoint", zap.String("Endpoint", endpoint))
	}
	// Trying to connect with the etcd data store
	// TODO: Change the endpoints so it can be accesible from config file
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   clientendpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		zap.L().Error("Etcd Connect Error", zap.Error(err))
	}
	defer cli.Close()
	zap.L().Debug("Connected with Etcd Data store")

	// Will provide a list of available masters
	MasterInsert(cli, hostName, ipaddress)
	// For Leader Selection
	pipe := make(chan bool)
	// For Leader Updation
	mast := make(chan bool)
	// For Leader Termination
	lead := make(chan bool)

	//Will see if there is any change in leader status
	go WatchLeaderElection(cli, hostName, pipe, mast, lead)
	//Will select the leader if there is no new leader
	go SelectLeader(cli, hostName, pipe, lead)
	// For Partition Updates periodically
	go MasterUpdation(mast)

	//Will be the endpoint for the client to make VM create and delete requests
	Controller(cli, mast, hostName)
}
