package main

import (
	"context"
	"time"

	pbc "napoleon/controller"
	pbl "napoleon/leader"
	pb "napoleon/napolet"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

//TODO:
// napolet client
// leader client
// etcd client

const (
	napoletport = 8799
)

func ReadRPC(cli *clientv3.Client, squeue chan string) {
	tunnel := make(chan string)
	for i := 0; i < noOfReadRPCs; i++ {
		go TalkToNapolet(cli, tunnel, squeue)
	}
	for {
		mu.Lock()
		length := len(partition.Ipaddress)
		mu.Unlock()
		for i := 0; i < length; i++ {
			mu.Lock()
			if len(partition.Ipaddress) < i {
				break
			}
			tunnel <- partition.Ipaddress[i]

			mu.Unlock()
		}
	}

}

func GetStat(client pb.PingClient) *pb.Stat {
	zap.L().Debug("Getting Stat from napolet")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	stat, err := client.GetStat(ctx, &pb.Dummy{})
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	return stat
}

func CreateConnectionSlave(cli *clientv3.Client, ipaddress string) *pb.Stat {
	conn, err := grpc.Dial(ipaddress+":"+string(napoletport), grpc.WithInsecure())
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pb.NewPingClient(conn)
	stat := GetStat(client)
	return stat
}

func SendStateUpdate(client pbl.LeaderClient, pmid string, smem, scpu uint32) {
	zap.L().Debug("Sending Stat to Leader")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := client.SendStateUpdate(ctx, &pbl.StateUpdateRequest{PMId: pmid, SlackCpu: scpu, SlackMemory: smem})
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
}

func CreateConnectionLeader(pmid string, smem, scpu uint32) {
	mu.Lock()
	conn, err := grpc.Dial(leader+":"+string(LeaderPort), grpc.WithInsecure())
	mu.Unlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pbl.NewLeaderClient(conn)
	SendStateUpdate(client, pmid, smem, scpu)
}

func InformLeader(stat *pb.Stat, squeue chan string) {
	var smem, scpu uint32
	if stat.PM.SlackMemory == 0 || stat.PM.SlackCpu == 0 {
		smem = 0
		scpu = 0
	} else {
		smem = stat.PM.SlackMemory
		scpu = stat.PM.SlackCpu
	}
	CreateConnectionLeader(stat.PM.PMId, smem, scpu)
	if smem == 0 || scpu == 0 {
		sched.Lock()
		squeue <- string(3)
		value, error := proto.Marshal(stat)
		zap.L().Error("Error in Marshalling", zap.Error(error))
		squeue <- string(value)
		sched.Unlock()
	}
}

func TalkToNapolet(cli *clientv3.Client, tunnel, squeue chan string) {
	for {
		ipaddress := <-tunnel
		stat := CreateConnectionSlave(cli, ipaddress)
		for _, vm := range stat.VMS {
			if vm.State == "Created" {
				changeStateOfVM(cli, vm.VMId, pbc.VMStatusResponse_CREATED)
			} else if vm.State == "Suspended" {
				changeStateOfVM(cli, vm.VMId, pbc.VMStatusResponse_SUSPENDED)
			}
		}
		InformLeader(stat, squeue)
	}
}
