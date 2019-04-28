package main

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TakeLease: Provides Lease
func TakeLease(ctx context.Context, cli *clientv3.Client, duration int64) (lease *clientv3.LeaseGrantResponse) {
	zap.L().Debug("Trying to take Lease")
	ctx, cancel := context.WithTimeout(ctx, ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	lease, err := cli.Grant(ctx, duration)
	if err != nil {
		zap.L().Error("Lease Error", zap.Error(err))
	}
	zap.L().Info("Lease Granted",
		zap.Any("Lease ID", lease.ID),
	)
	return
}

// InsertKeyWithLease: Inserts key with a lease
func InsertKeyWithLease(ctx context.Context, cli *clientv3.Client, key, value string, lease *clientv3.LeaseGrantResponse) (resp *clientv3.PutResponse) {
	zap.L().Debug("Trying to insert key with lease",
		zap.String("Key", key),
		zap.String("Value", value),
	)
	ctx, cancel := context.WithTimeout(ctx, ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	resp, err := cli.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		zap.L().Error("Insert Error", zap.Error(err))
	}
	zap.L().Info("Key is inserted with Lease",
		zap.String("Key", key),
		zap.String("Value", value),
	)
	return
}

// Inserts Key
func InsertKey(cli *clientv3.Client, key, value string) (resp *clientv3.PutResponse) {
	zap.L().Debug("Trying to insert key",
		zap.String("Key", key),
		zap.String("Value", value),
	)
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		zap.L().Error("Insert Error", zap.Error(err))
	}
	zap.L().Info("Key is inserted",
		zap.String("Key", key),
		zap.String("Value", value),
	)
	return
}

// Get the Resp for a given Key value
func GetKeyResp(ctx context.Context, cli *clientv3.Client, key string) (resp *clientv3.GetResponse) {
	zap.L().Debug("Getting Information about a key",
		zap.String("Key", key),
	)
	ctx, cancel := context.WithTimeout(ctx, ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		zap.L().Error("Get Error", zap.Error(err))
	}
	zap.L().Info("GET Response for a given key is returned",
		zap.String("Key", key),
		// zap.String("")
	)
	return
}

// KeepAlive: Keeps Alive the lease
func KeepAlive(ctx context.Context, cli *clientv3.Client, lease *clientv3.LeaseGrantResponse) {
	for {
		zap.L().Debug("Starting to increase a lease")
		ctx = clientv3.WithRequireLeader(ctx)
		_, err := cli.KeepAliveOnce(ctx, lease.ID)
		if err != nil {
			zap.L().Error("Keep Alive Error", zap.Error(err))
		}
		sleepTime := time.Duration(MinimumLeaseTime-2) * time.Second
		zap.L().Info("Keeping Alive Lease",
			zap.Any("Lease Id", lease.ID),
		)
		time.Sleep(sleepTime)
	}
}

func InsertKeyIfKeyNotPresent(cli *clientv3.Client, key, value string) bool {
	zap.L().Debug("Trying to insert key if Key is not present",
		zap.String("Key", key),
		zap.String("Value", value),
	)
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	zap.L().Debug("Trying to perform InsertKeyIfKeyNotPresent operation atomically")
	txnResp, err := cli.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(key), "=", 0),
	).Then(
		clientv3.OpPut(key, value),
	).Commit()
	if err != nil {
		zap.L().Error("TXN Error", zap.Error(err))
	}
	if txnResp.Succeeded == true {
		zap.L().Info("Key is Inserted",
			zap.String("Key Name", key),
			zap.String("Value", value),
		)
		return true
	}
	zap.L().Info("Key is not Inserted",
		zap.String("Key Name", key),
		zap.String("Value", value),
	)
	return false
}

func AtomicKeyInsertion(cli *clientv3.Client, cs clientv3.Cmp, opts ...clientv3.Op) bool {
	zap.L().Debug("Trying to insert so many keys at a time Atomically")
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	// TODO:
	txnResp, err := cli.Txn(ctx).If(cs).Then(opts...).Commit()
	if err != nil {
		zap.L().Error("TXN Error", zap.Error(err))
	}
	if txnResp.Succeeded == true {
		zap.L().Info("Keys are Inserted")
		return true
	}
	zap.L().Info("Keys are not Inserted")
	return false
}

func GetKeyRangeResp(cli *clientv3.Client, key, end string) (resp *clientv3.GetResponse) {
	zap.L().Debug("Getting Key Values in a range",
		zap.String("Range Starting", key),
		zap.String("Range End", end),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*ContextTimeout)
	defer cancel()
	ctx = clientv3.WithRequireLeader(ctx)
	resp, err := cli.Get(ctx, key, clientv3.WithLimit(0), clientv3.WithRange(end))
	if err != nil {
		zap.L().Error("Get Error", zap.Error(err))
	}
	zap.L().Info("GET Response for a given key is returned")
	return
}
