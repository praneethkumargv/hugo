package main

import (
	"context"
	"math/rand"
	pbc "napoleon/controller"
	pbl "napoleon/leader"
	pbn "napoleon/napolet"
	pbt "napoleon/types"
	"time"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TODO:
// LP Solver
// Leader Client
// napolet Client
// Retrieve Top K elements

func Scheduler(cli *clientv3.Client, squeue chan string) {
	for {
		reqtype := <-squeue
		reqparam := <-squeue
		var done bool
		for i := 0; i < 10; i++ {
			if reqtype == "1" {
				done = ScheduleCreateVM(reqparam, cli)
			} else if reqtype == "2" {
				done = ScheduleDeleteVM(reqparam, cli)
			} else if reqtype == "3" {
				done = ScheduleMigrateVM(reqparam)
			}
			if done == true {
				break
			}
			time.Sleep(time.Duration(rand.Int31n(2000)) * time.Millisecond)
		}
		if done == false {
			zap.L().Error("There is a Scheduling operation that is taking so much time, so leaving it")
		}
	}
}

func RetrievePM(client pbl.LeaderClient) *pbl.StateResponse {
	zap.L().Debug("Getting PM's from Leader")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	pms, err := client.RetrieveStateChanges(ctx, &pbl.StateRequest{NumOn: 15, NumOff: 5})
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	return pms
}

func createVM(client pbl.LeaderClient, req *pbl.CreateNewVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.CreateNewVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	return
}

func ScheduleCreateVM(reqparam string, cli *clientv3.Client) (done bool) {
	var value = new(pbc.VMStatusResponse)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	mu.Lock()
	conn, err := grpc.Dial(leader+":"+string(LeaderPort), grpc.WithInsecure())
	mu.Unlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pbl.NewLeaderClient(conn)
	pms := RetrievePM(client)
	var pmid string
	for _, pm := range pms.Pm {
		if pm.SlackCpu > value.Vm.Vcpus && pm.SlackMemory > value.Vm.Memory {
			req := &pbl.CreateNewVMRequest{
				VMId:    value.VMId,
				PMId:    pm.PMId,
				Pcpu:    value.Vm.Vcpus,
				Pmemory: value.Vm.Memory,
			}
			done = createVM(client, req)
			if done == true {
				pmid = pm.PMId
				break
			}
		}
	}
	if done == true {
		//TODO: send request to borglet
		keyvalue := getKeyValue(GetKeyResp(context.Background(), cli, pmid))
		var temp pbt.PM
		error := proto.Unmarshal([]byte(keyvalue), &temp)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}
		conn, err := grpc.Dial(temp.Ipaddress+":"+string(napoletport), grpc.WithInsecure())
		if err != nil {
			zap.L().Error("Failed to dial", zap.Error(err))
		}
		defer conn.Close()
		client := pbn.NewPingClient(conn)
		req := &pbn.CreateVMRequest{
			VMName:    value.Vm.VMName,
			Vcpus:     value.Vm.Vcpus,
			Memory:    value.Vm.Memory,
			Storage:   value.Vm.Storage,
			ImageName: value.Vm.ImageName,
			VMId:      value.VMId,
			PMId:      pmid,
		}
		done = SendCreateReqToNapolet(client, req)
	}
	return
}

func SendCreateReqToNapolet(client pbn.PingClient, req *pbn.CreateVMRequest) (done bool) {
	zap.L().Debug("Contacting napolet For Conformation")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.CreateVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	return
}

func deleteVM(client pbl.LeaderClient, req *pbl.DeleteVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation Delete")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.DeleteVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	return
}

func ScheduleDeleteVM(reqparam string, cli *clientv3.Client) (done bool) {
	var value = new(pbc.VMStatusResponse)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	mu.Lock()
	conn, err := grpc.Dial(leader+":"+string(LeaderPort), grpc.WithInsecure())
	mu.Unlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pbl.NewLeaderClient(conn)
	// TODO:
	req := &pbl.DeleteVMRequest{VMId: value.VMId}
	done = deleteVM(client, req)
	if done == true {
		keyvalue := getKeyValue(GetKeyResp(context.Background(), cli, value.VMId+"_on"))
		keyvalue = getKeyValue(GetKeyResp(context.Background(), cli, keyvalue))
		var temp pbt.PM
		error := proto.Unmarshal([]byte(keyvalue), &temp)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}
		conn, err := grpc.Dial(temp.Ipaddress+":"+string(napoletport), grpc.WithInsecure())
		if err != nil {
			zap.L().Error("Failed to dial", zap.Error(err))
		}
		defer conn.Close()
		client := pbn.NewPingClient(conn)
		req := &pbn.DeleteVMRequest{
			VMId: value.VMId,
		}
		done = SendDeleteReqToNapolet(client, req)
	}
	return
}

func SendDeleteReqToNapolet(client pbn.PingClient, req *pbn.DeleteVMRequest) (done bool) {
	zap.L().Debug("Contacting napolet For Conformation Delete")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.DeleteVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	return
}

func ScheduleMigrateVM(reqparam string) (done bool) {
	var value = new(pbn.Stat)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	mu.Lock()
	conn, err := grpc.Dial(leader+":"+string(LeaderPort), grpc.WithInsecure())
	mu.Unlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pbl.NewLeaderClient(conn)
	// TODO:
	// what if no of pms are less than asked
	pms := RetrievePM(client)
	var pcpu, pmemory []uint32
	var ccpu, cmemory uint32
	var scpu, smemory []uint32
	vmmap := make(map[int]*pbn.VMStat)
	pmmap := make(map[int]*pbl.PMInformation)
	ccpu = value.PM.SlackCpu
	cmemory = value.PM.SlackMemory
	for i, vm := range value.VMS {
		vmmap[i] = vm
		pcpu = append(pcpu, vm.PredictedCpu)
		pmemory = append(pmemory, vm.PredictedMemory)
	}
	for i, pm := range pms.Pm {
		pmmap[i] = pm
		scpu = append(scpu, pm.SlackCpu)
		smemory = append(smemory, pm.SlackMemory)
	}
	vmtopm := Solve(pcpu, pmemory, ccpu, cmemory, scpu, smemory)
	var req pbl.MigrateVMRequest
	for vmno, pmno := range vmtopm {
		ele := &pbl.CreateNewVMRequest{
			VMId:    vmmap[vmno].VMId,
			PMId:    pmmap[pmno].PMId,
			Pcpu:    vmmap[vmno].PredictedCpu,
			Pmemory: vmmap[vmno].PredictedMemory,
		}
		req.Assigned = append(req.Assigned, ele)
	}
	done = SendReqForMigration(client, &req)
	return
}

func SendReqForMigration(client pbl.LeaderClient, req *pbl.MigrateVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation Migration")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	resp, err := client.MigrateVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	return
}
