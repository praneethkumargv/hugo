package main

import (
	"context"
	"fmt"
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

// TODO: When request for migration scheduler sees if it is in it's partition

func Scheduler(cli *clientv3.Client, squeue chan Sched) {
	for {
		req := <-squeue
		zap.L().Debug("Got a VM request and need to execute")
		reqtype := fmt.Sprintf("%d", req.types)
		reqparam := req.method
		var done bool
		for i := 0; i < 10; i++ {
			if reqtype == "1" {
				zap.L().Debug("Create VM Request")
				done = ScheduleCreateVM(reqparam, cli)
				if done == true {
					zap.L().Debug("VM is Created")
				}
			} else if reqtype == "2" {
				zap.L().Debug("Delete VM Request")
				done = ScheduleDeleteVM(reqparam, cli)
				if done == true {
					zap.L().Debug("VM is Deleted")
				}
			} else if reqtype == "3" {
				zap.L().Debug("Migrate VM Request")
				done = ScheduleMigrateVM(reqparam, cli)
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
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	pms, err := client.RetrieveStateChanges(ctx, &pbl.StateRequest{NumOn: 15, NumOff: 5})
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	for _, pm := range pms.Pm {
		zap.L().Debug("The retrieved PM's are")
		zap.L().Debug("",
			zap.String("PMId", pm.PMId),
			zap.Uint32("Capacity CPU", pm.CapacityCpu),
			zap.Uint32("Capacity Memory", pm.CapacityMemory),
			zap.Uint32("SlackCpu", pm.SlackCpu),
			zap.Uint32("SlackMemory", pm.SlackMemory),
			zap.Uint32("Number in retrieved pm", pm.Number),
			zap.Bool("State of PM", pm.State),
		)
	}
	return pms
}

func createVM(client pbl.LeaderClient, req *pbl.CreateNewVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation",
		zap.String("Virtual Machine", req.VMId),
		zap.String("Physical Machine", req.PMId),
	)
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.CreateNewVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Create Request Accepted by leader",
			zap.String("Virtual Machine", req.VMId),
		)
	}
	return
}

func ScheduleCreateVM(reqparam string, cli *clientv3.Client) (done bool) {
	var value = new(pbc.VMStatusResponse)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	// TODO: READERS AND WRITERS LOCK FOR LEADER
	// mu.RLock()
	zap.L().Debug("Making connection with Leader")
	conn, err := grpc.Dial(leader+":"+fmt.Sprintf("%d", LeaderPort), grpc.WithInsecure())
	// mu.RUnlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	zap.L().Debug("Leader Connected")
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
		zap.L().Debug("Send request for the Napolet for creation of VM")
		keyvalue := getKeyValue(GetKeyResp(context.Background(), cli, pmid))
		var temp pbt.PM
		error := proto.Unmarshal([]byte(keyvalue), &temp)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}
		zap.L().Debug("Trying to connect with napolet")
		conn, err := grpc.Dial(temp.Ipaddress, grpc.WithInsecure())
		if err != nil {
			zap.L().Error("Failed to dial", zap.Error(err))
		}
		zap.L().Debug("Connected with napolet")
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
	zap.L().Debug("Contacting napolet For Conformation",
		zap.String("Virtual Machine Name", req.VMName),
		zap.String("Virtual machine Id", req.VMId),
		zap.String("Physical machine Id", req.PMId),
	)
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.CreateVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Virtual Machine Created", zap.String("VMId", req.VMId))
	} else {
		zap.L().Debug("Virtual Machine Not Created", zap.String("VMId", req.VMId))
	}
	return
}

func deleteVM(client pbl.LeaderClient, req *pbl.DeleteVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation Delete",
		zap.String("Virtual Machine Id", req.VMId),
	)
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.DeleteVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Delete Request Accepted by leader",
			zap.String("Virtual Machine", req.VMId),
		)
	}
	return
}

func ScheduleDeleteVM(reqparam string, cli *clientv3.Client) (done bool) {
	var value = new(pbc.VMStatusResponse)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	// mu.RLock()
	zap.L().Debug("Making connection with Leader")
	conn, err := grpc.Dial(leader+":"+fmt.Sprintf("%d", LeaderPort), grpc.WithInsecure())
	// mu.RUnlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	zap.L().Debug("Leader Connected")
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
		zap.L().Debug("Trying to connect with napolet")
		conn, err := grpc.Dial(temp.Ipaddress, grpc.WithInsecure())
		if err != nil {
			zap.L().Error("Failed to dial", zap.Error(err))
		}
		zap.L().Debug("Connected with napolet")
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
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.DeleteVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Delete Request Accepted by Napolet",
			zap.String("Virtual Machine", req.VMId),
		)
	} else {
		zap.L().Debug("Delete Request Not Accepted by Napolet",
			zap.String("Virtual Machine", req.VMId),
		)
	}
	return
}

func ScheduleMigrateVM(reqparam string, cli *clientv3.Client) (done bool) {
	var value = new(pbn.Stat)
	error := proto.Unmarshal([]byte(reqparam), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	zap.L().Debug("Scheduled to Migrate VM's in PM",
		zap.String("Physical Machine Id", value.PM.PMId),
	)
	srcpmid := value.PM.PMId
	// mu.RLock()
	zap.L().Debug("Making connection with Leader")
	conn, err := grpc.Dial(leader+":"+fmt.Sprintf("%d", LeaderPort), grpc.WithInsecure())
	// mu.RUnlock()
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	zap.L().Debug("Leader Connected")
	defer conn.Close()
	client := pbl.NewLeaderClient(conn)
	pms := RetrievePM(client)
	var pcpu, pmemory []uint32
	var ccpu, cmemory uint32
	var scpu, smemory []uint32
	vmmap := make(map[int]*pbn.VMStat)
	pmmap := make(map[int]*pbl.PMInformation)
	ccpu = value.PM.TotalCpu
	cmemory = value.PM.TotalMemory
	zap.L().Debug("", zap.Uint32("Capacity CPU", ccpu))
	zap.L().Debug("", zap.Uint32("Capacity Memory", cmemory))

	zap.L().Debug("The VM's on the physical machine are")
	for i, vm := range value.VMS {
		zap.L().Debug("", zap.String("VMId", vm.VMId))
		zap.L().Debug("", zap.Uint32("Predicted CPU", vm.PredictedCpu))
		zap.L().Debug("", zap.Uint32("Predicted Memory", vm.PredictedMemory))
		vmmap[i] = vm
		pcpu = append(pcpu, vm.PredictedCpu)
		pmemory = append(pmemory, vm.PredictedMemory)
	}
	zap.L().Debug("The retrieved PM's are")
	noofonpms := 0
	noofoffpms := 0
	for i, pm := range pms.Pm {
		zap.L().Debug("", zap.String("PMId", pm.PMId))
		zap.L().Debug("", zap.Uint32("Slack CPU", pm.SlackCpu))
		zap.L().Debug("", zap.Uint32("Slack Memory", pm.SlackMemory))
		pmmap[i] = pm
		scpu = append(scpu, pm.SlackCpu)
		smemory = append(smemory, pm.SlackMemory)
		if pm.State == true {
			noofonpms++
		} else if pm.State == false {
			noofoffpms++
		}
	}
	zap.L().Debug("Trying to solving the problem with the given heuristic")
	start := time.Now()
	vmtopm := Solve(pcpu, pmemory, ccpu, cmemory, scpu, smemory, noofonpms, noofoffpms)
	elapsed := time.Since(start)
	zap.L().Info("Time elapsed for algorithm is ",
		zap.Duration("Time for Heuristic to run", elapsed),
	)
	var req pbl.MigrateVMRequest
	// zap.L().Debug("", zap.Uint32("", len(vmtopm)))
	for vmno, pmno := range vmtopm {

		// send request of only vm that need to migrate to another pm
		if pmno == 0 {
			zap.L().Debug("",
				zap.String("VMId", vmmap[vmno].VMId),
				zap.String("PMId", value.PM.PMId),
			)
		} else {
			zap.L().Debug("",
				zap.String("VMId", vmmap[vmno].VMId),
				zap.String("PMId", pmmap[pmno-1].PMId),
			)
			ele := &pbl.CreateNewVMRequest{
				VMId:    vmmap[vmno].VMId,
				PMId:    pmmap[pmno-1].PMId,
				Pcpu:    vmmap[vmno].PredictedCpu,
				Pmemory: vmmap[vmno].PredictedMemory,
			}
			req.Assigned = append(req.Assigned, ele)
		}
	}
	done = SendReqForMigration(client, &req)
	if done == true {
		//TODO: SEND REQUEST TO NAPOLET
		for _, request := range req.Assigned {
			pmid := request.PMId
			reqparam := getKeyValue(GetKeyResp(context.Background(), cli, pmid))
			var value = new(pbt.PM)
			error := proto.Unmarshal([]byte(reqparam), value)
			if error != nil {
				zap.L().Error("Unmarshalling Error", zap.Error(error))
			}
			migreq := &pbn.MigrateVMRequest{
				VMId:      request.VMId,
				IPAddress: value.Ipaddress,
			}

			keyvalue := getKeyValue(GetKeyResp(context.Background(), cli, srcpmid))
			var temp pbt.PM
			error = proto.Unmarshal([]byte(keyvalue), &temp)
			if error != nil {
				zap.L().Error("Unmarshalling Error", zap.Error(error))
			}
			zap.L().Debug("Trying to connect with napolet")
			conn, err := grpc.Dial(temp.Ipaddress, grpc.WithInsecure())
			if err != nil {
				zap.L().Error("Failed to dial", zap.Error(err))
			}
			zap.L().Debug("Connected with napolet")
			defer conn.Close()
			client := pbn.NewPingClient(conn)
			SendMigrateReqToNapolet(client, migreq)
		}
	}
	return
}

func SendMigrateReqToNapolet(client pbn.PingClient, req *pbn.MigrateVMRequest) (done bool) {
	zap.L().Debug("Contacting napolet For Conformation Migration")
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.MigrateVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Migrate Request Accepted by Napolet",
			zap.String("Virtual Machine", req.VMId),
		)
	} else {
		zap.L().Debug("Migrate Request Not Accepted by Napolet",
			zap.String("Virtual Machine", req.VMId),
		)
	}
	return
}

func SendReqForMigration(client pbl.LeaderClient, req *pbl.MigrateVMRequest) (done bool) {
	zap.L().Debug("Contacting Leader For Conformation Migration")
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	resp, err := client.MigrateVM(ctx, req)
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	done = resp.Accepted
	if done == true {
		zap.L().Debug("Migration Request Accepted")
	} else {
		zap.L().Debug("Migration Request Not Accepted")
	}
	return
}
