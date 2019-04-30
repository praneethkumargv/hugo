package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pbt "napoleon/types"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// Timeout for context
	ContextTimeout   = time.Second
	dialTimeout      = 5 * time.Second
	MinimumLeaseTime = int64(5)
	LeaderKey        = "/elected"
)

func main() {
	// Created logger and made the logger pacakge global
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Logger Error")
	}
	zap.ReplaceGlobals(logger)
	defer zap.L().Sync()

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

	for i := 10; i < 12; i++ {
		pm := &pbt.PM{
			PMName:    fmt.Sprintf("%d", i+1),
			Vcpu:      25000,
			Memory:    25000,
			Ipaddress: "localhost:" + fmt.Sprintf("%d", 10000+i),
			State:     true,
		}
		// var pmt pbt.PM
		out, err := proto.Marshal(pm)
		if err != nil {
			zap.L().Error("Marshalling Error", zap.Error(err))
		}

		key := fmt.Sprintf("pm_%d", i+1)
		zap.L().Debug("Trying to insert 'pm key'",
			zap.String("PM Key", key),
		)
		value := string(out)
		resp := InsertKey(cli, key, value)
		zap.L().Debug("The returned response is",
			zap.Any("Response", resp),
		)
		zap.L().Info("Key is inserted",
			zap.String("Key", key),
		)
	}
	// value := getKeyValue(GetKeyResp(context.Background(), cli, "partition_master_s1"))
	// // var temp pbtype.Partition
	// // err := proto.Unarshal(value, &temp)
	// fmt.Println(value)
}

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
