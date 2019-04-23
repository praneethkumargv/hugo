package main

import (
	"container/heap"
	"context"
	"math"
	"net"
	"sync"
	"time"

	pbc "napoleon/controller"
	pb "napoleon/leader"
	pbtype "napoleon/types"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"google.golang.org/grpc"
)

const (
	MaxUint        = ^uint64(0)
	MinUint        = uint64(0)
	TopOnElements  = 15
	TopOffElements = 5
)

var (
	muoff     sync.Mutex
	muon      sync.Mutex
	etcdstore sync.Mutex
	offqueue  = make(PriorityQueue, 0)
)

func Partition(cli *clientv3.Client) {

	for {
		muoff.Lock()
		muon.Lock()
		zap.L().Info("Starting partition")
		key := "pm_" + string(MinUint)
		end := "pm_" + string(MaxUint)
		zap.L().Info("Key Starting and Ending", zap.String("Start", key), zap.String("End", end))

		resp := GetKeyRangeResp(cli, key, end)

		values := resp.Kvs
		var arroff, arron []pbtype.PM
		var keyoff []string
		for _, value := range values {
			var temp pbtype.PM
			error := proto.Unmarshal(value.Value, &temp)
			if error != nil {
				zap.L().Error("Unmarshalling Error", zap.Error(error))
			}
			if temp.State == true {
				arron = append(arron, temp)
			} else {
				arroff = append(arroff, temp)
				keyoff = append(keyoff, string(value.Key))
			}

		}
		noOfPM := len(arron)
		zap.L().Info("Found no of PM's", zap.Int("Number of Physical Machines", noOfPM))

		key = "master_s" + string(1)
		end = "master_s" + string(9)
		zap.L().Info("Master Starting and Ending", zap.String("Start", key), zap.String("End", end))

		masterResp := GetKeyRangeResp(cli, key, end)
		noOfMaster := len(masterResp.Kvs)
		zap.L().Info("Found no of Masters", zap.Int("Number of masters", noOfMaster))

		noOfIp := math.Ceil(float64(noOfPM) / float64(noOfMaster))
		zap.L().Info("Found no of IP's", zap.Float64("Number of PM's per Master", noOfIp))

		masters := masterResp.Kvs

		i := 0
		for _, master := range masters {
			var partition pbtype.Partition
			for j := 0; j < int(noOfIp) && i < noOfPM; j++ {
				partition.Ipaddress = append(partition.Ipaddress, arron[i].Ipaddress)
				i++
			}
			out, err := proto.Marshal(&partition)
			if err != nil {
				zap.L().Error("Marshalling Error", zap.Error(err))
			}
			//KEY: partition_$(hostname) VALUE: protobuf of IPAddress
			key := "partition_" + string(master.Value)
			value := string(out)
			resp := InsertKey(cli, key, value)
			zap.L().Debug("The returned response is", zap.Any("Response", resp))
			zap.L().Info("Key is inserted",
				zap.String("Key", key),
			)
		}

		offqueue := make(PriorityQueue, 0)
		for i, offpm := range arroff {
			item := &Item{
				PMId:     keyoff[i],
				scpu:     offpm.Vcpu,
				smemory:  offpm.Memory,
				priority: offpm.Vcpu * offpm.Memory,
				index:    i,
			}
			offqueue = append(offqueue, item)
		}
		heap.Init(&offqueue)
		muon.Unlock()
		muoff.Unlock()

		time.Sleep(partitionUpdateInterval)
	}

}

type LeaderServer struct {
	onqueue  PriorityQueue
	offqueue PriorityQueue
	client   *clientv3.Client
}

func changeStateOfVM(client *clientv3.Client, vmkey string, status pbc.VMStatusResponse_Status) {
	resp := GetKeyResp(context.Background(), client, vmkey)
	value := getKeyValue(resp)
	var temp pbc.VMStatusResponse
	error := proto.Unmarshal([]byte(value), &temp)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	temp.Status = status

	out, err := proto.Marshal(&temp)
	if err != nil {
		zap.L().Error("Marshalling Error", zap.Error(err))
	}
	value = string(out)
	getResp := InsertKey(client, vmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", getResp))
	zap.L().Info("Key is inserted",
		zap.String("Key", vmkey),
	)
}

func insertVMToPMMapping(client *clientv3.Client, vmkey string, pmkey string) {
	vmkey = vmkey + "_on"
	value := pmkey
	resp := InsertKey(client, vmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", resp))
	zap.L().Info("Key is inserted",
		zap.String("Key", vmkey),
	)
}

func insertVMToPMMappingMigrating(client *clientv3.Client, vmkey string, pmkey string) {
	vmkey = "migrating_" + vmkey
	value := pmkey
	resp := InsertKey(client, vmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", resp))
	zap.L().Info("Key is inserted",
		zap.String("Key", vmkey),
	)
}

func insertPMToVMMapping(client *clientv3.Client, vmkey string, pmkey string) {
	var list pbtype.VMONPM
	key := "on_" + pmkey
	resp := GetKeyResp(context.Background(), client, key)
	if isKeyPresent(resp) {
		value := getKeyValue(resp)
		error := proto.Unmarshal([]byte(value), &list)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}
		list.VMId = append(list.VMId, vmkey)
	} else {
		list.VMId = append(list.VMId, vmkey)
	}

	out, err := proto.Marshal(&list)
	if err != nil {
		zap.L().Error("Marshalling Error", zap.Error(err))
	}
	value := string(out)
	getResp := InsertKey(client, key, value)
	zap.L().Debug("The returned response is", zap.Any("Response", getResp))
	zap.L().Info("Key is inserted",
		zap.String("Key", key),
	)
}

func insertPMToVMMappingMigrating(client *clientv3.Client, vmkey string, pmkey string) {
	var list pbtype.VMONPM
	key := "migrating_" + pmkey
	resp := GetKeyResp(context.Background(), client, key)
	if isKeyPresent(resp) {
		value := getKeyValue(resp)
		error := proto.Unmarshal([]byte(value), &list)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}
		list.VMId = append(list.VMId, vmkey)
	} else {
		list.VMId = append(list.VMId, vmkey)
	}

	out, err := proto.Marshal(&list)
	if err != nil {
		zap.L().Error("Marshalling Error", zap.Error(err))
	}
	value := string(out)
	getResp := InsertKey(client, key, value)
	zap.L().Debug("The returned response is", zap.Any("Response", getResp))
	zap.L().Info("Key is inserted",
		zap.String("Key", key),
	)
}

func changePhysicalMachineState(client *clientv3.Client, pmkey string, state bool) {
	resp := GetKeyResp(context.Background(), client, pmkey)
	value := getKeyValue(resp)
	var temp pbtype.PM
	error := proto.Unmarshal([]byte(value), &temp)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	temp.State = state

	out, err := proto.Marshal(&temp)
	if err != nil {
		zap.L().Error("Marshalling Error", zap.Error(err))
	}
	value = string(out)
	getResp := InsertKey(client, pmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", getResp))
	zap.L().Info("Key is inserted",
		zap.String("Key", pmkey),
	)
}

func (leader *LeaderServer) CreateNewVM(ctx context.Context, req *pb.CreateNewVMRequest) (*pb.CreateNewVMResponse, error) {
	var temp []*Item
	vmid := req.VMId
	pmid := req.PMId
	pcpu := req.Pcpu
	pmem := req.Pmemory
	done := false
	anotherOnPM := false
	var check *Item
	muon.Lock()
	for i := 0; i < TopOnElements; i++ {
		check = heap.Pop(&leader.onqueue).(*Item)
		if check.PMId == pmid && check.scpu >= pcpu && check.smemory >= pmem {
			zap.L().Info("Found the necessary PM",
				zap.String("check.PMId", check.PMId),
				zap.String("pmid", pmid),
				zap.Uint32("check.spcu", check.scpu),
				zap.Uint32("pcpu", pcpu),
				zap.Uint32("check.smemory", check.smemory),
				zap.Uint32("pmem", pmem),
			)
			check.scpu = check.scpu - pcpu
			check.smemory = check.smemory - pmem
			check.priority = check.scpu * check.smemory
			temp = append(temp, check)
			done = true
			break
		} else if check.scpu >= pcpu && check.smemory >= pmem {
			anotherOnPM = true
		}
		temp = append(temp, check)
	}
	for _, itemptr := range temp {
		heap.Push(&leader.onqueue, *itemptr)
	}
	muon.Unlock()

	if done == true {
		// changing state of Virtual Machine
		changeStateOfVM(leader.client, vmid, pbc.VMStatusResponse_CREATING)

		// inserting vm to pm mapping
		insertVMToPMMapping(leader.client, vmid, check.PMId)

		// inserting pm to vm mapping
		insertPMToVMMapping(leader.client, vmid, check.PMId)

	} else if anotherOnPM == false {
		temp = make([]*Item, 0)
		var onPM *Item
		muoff.Lock()
		for i := 0; i < TopOffElements; i++ {
			check := heap.Pop(&leader.offqueue).(*Item)
			if check.PMId == pmid && check.scpu >= pcpu && check.smemory >= pmem {
				zap.L().Info("Found the necessary PM",
					zap.String("check.PMId", check.PMId),
					zap.String("pmid", pmid),
					zap.Uint32("check.spcu", check.scpu),
					zap.Uint32("pcpu", pcpu),
					zap.Uint32("check.smemory", check.smemory),
					zap.Uint32("pmem", pmem),
				)
				check.scpu = check.scpu - pcpu
				check.smemory = check.smemory - pmem
				check.priority = check.scpu * check.smemory
				onPM = check
				done = true
				break
			}
			temp = append(temp, check)
		}
		for _, itemptr := range temp {
			heap.Push(&leader.offqueue, *itemptr)
		}
		muoff.Unlock()

		muon.Lock()
		//changing the state of Physical Machine
		changePhysicalMachineState(leader.client, onPM.PMId, true)
		heap.Push(&leader.onqueue, &onPM)
		muon.Unlock()

		// changing state of Virtual Machine
		changeStateOfVM(leader.client, vmid, pbc.VMStatusResponse_CREATING)

		// inserting vm to pm mapping
		insertVMToPMMapping(leader.client, vmid, check.PMId)

		// inserting pm to vm mapping
		insertPMToVMMapping(leader.client, vmid, check.PMId)
	}
	return &pb.CreateNewVMResponse{Accepted: done}, nil
}

func (leader *LeaderServer) DeleteVM(ctx context.Context, req *pb.DeleteVMRequest) (*pb.DeleteVMResponse, error) {
	done := false
	vmid := req.VMId
	etcdstore.Lock()
	value := getKeyValue(GetKeyResp(context.Background(), leader.client, vmid))
	var temp pbc.VMStatusResponse
	error := proto.Unmarshal([]byte(value), &temp)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	if temp.Status == pbc.VMStatusResponse_CREATED || temp.Status == pbc.VMStatusResponse_SUSPENDED {
		temp.Status = pbc.VMStatusResponse_DELETING
		out, err := proto.Marshal(&temp)
		if err != nil {
			zap.L().Error("Marshalling Error", zap.Error(err))
		}
		value = string(out)
		getResp := InsertKey(leader.client, vmid, value)
		zap.L().Debug("The returned response is", zap.Any("Response", getResp))
		zap.L().Info("Key is inserted",
			zap.String("Key", vmid),
		)
		zap.L().Info("Changed the state of key to deleting",
			zap.String("Key", vmid),
		)
		done = true
	}

	etcdstore.Unlock()
	return &pb.DeleteVMResponse{Accepted: done}, nil
}

// TODO: READERS LOCK AND WRITERS LOCK
// message PMInformation{
//     string PMId = 1;
//     uint32 capacityCpu = 2;
//     uint32 capacityMemory = 3;
//     uint32 slackCpu = 4;
//     uint32 slackMemory = 5;
//     uint32 number = 6;
//     bool state = 7;
// }
// PMId                    string
// 	scpu, smemory, priority uint32

// ONLY READING FROM THE DATABASE
func heapOperation(client *clientv3.Client, queue *PriorityQueue, resp *pb.StateResponse, state bool, noofpm uint32) {
	var lock sync.Mutex
	var temp []*Item
	if state == true {
		lock = muon
	} else {
		lock = muoff
	}

	lock.Lock()
	for i := uint32(0); i < noofpm; i++ {
		check := heap.Pop(queue).(*Item)
		temp = append(temp, check)
		var part pb.PMInformation
		part.PMId = check.PMId
		part.SlackCpu = check.scpu
		part.SlackMemory = check.smemory
		part.Number = i + 1
		part.State = state

		// Reading the database here
		value := getKeyValue(GetKeyResp(context.Background(), client, part.PMId))
		var valueOfPM pbtype.PM
		error := proto.Unmarshal([]byte(value), &valueOfPM)
		if error != nil {
			zap.L().Error("Unmarshalling Error", zap.Error(error))
		}

		part.CapacityCpu = valueOfPM.Vcpu
		part.CapacityMemory = valueOfPM.Memory
		resp.Pm = append(resp.Pm, &part)
	}

	for _, itemptr := range temp {
		heap.Push(queue, *itemptr)
	}
	lock.Lock()
}

func (leader *LeaderServer) RetrieveStateChanges(ctx context.Context, req *pb.StateRequest) (*pb.StateResponse, error) {
	onpm := req.NumOn
	offpm := req.NumOff
	var resp pb.StateResponse

	heapOperation(leader.client, &leader.onqueue, &resp, true, onpm)
	heapOperation(leader.client, &leader.offqueue, &resp, false, offpm)
	return &resp, nil
}

// message StateUpdateRequest{
//     string PMId = 1;
//     uint32 slackCpu = 2;
//     uint32 slackMemory = 3;
// }
func (leader *LeaderServer) SendStateUpdate(ctx context.Context, req *pb.StateUpdateRequest) (*pb.StateUpdateResponse, error) {
	// The State Update request can be only of PM which is on
	pmid := req.PMId
	scpu := req.SlackCpu
	smemory := req.SlackMemory
	done := false
	muon.Lock()
	for i, node := range leader.onqueue {
		if node.PMId == pmid {
			node.scpu = scpu
			node.smemory = smemory
			node.priority = scpu * smemory
			heap.Fix(&leader.onqueue, i)
			done = true
			break
		}
	}
	if done == false {
		//TODO: SHOULD TAKE CARE OF THIS INSERTION
		item := &Item{
			PMId:     pmid,
			scpu:     scpu,
			smemory:  smemory,
			priority: scpu * smemory,
		}
		heap.Push(&leader.onqueue, item)
		done = true
	}
	muon.Unlock()
	return &pb.StateUpdateResponse{Accepted: done}, nil
}

// message MigrateVMRequest{
//     repeated CreateNewVMRequest assigned = 1;
// }
// message CreateNewVMRequest{
//     string VMId = 1; // vm_(64bitid)
//     string PMId = 2; // pm_(64bitid)
//     uint32 pcpu = 3;
//     uint32 pmemory = 4;
// }
type Resources struct {
	cpu, memory uint32
}

func (leader *LeaderServer) MigrateVM(ctx context.Context, req *pb.MigrateVMRequest) (*pb.MigrateVMResponse, error) {
	muon.Lock()
	muoff.Lock()
	var onpm, offpm []*Item
	available := make(map[string]Resources)
	required := make(map[string]Resources)
	for i := 0; i < TopOnElements; i++ {
		check := heap.Pop(&leader.onqueue).(*Item)
		onpm = append(onpm, check)
		available[check.PMId] = Resources{cpu: check.scpu, memory: check.smemory}
	}
	for i := 0; i < TopOffElements; i++ {
		check := heap.Pop(&leader.offqueue).(*Item)
		offpm = append(offpm, check)
		available[check.PMId] = Resources{cpu: check.scpu, memory: check.smemory}
	}
	for _, vmrequest := range req.Assigned {
		if resource, ok := required[vmrequest.PMId]; ok {
			required[vmrequest.PMId] = Resources{
				cpu:    resource.cpu + vmrequest.Pcpu,
				memory: resource.memory + vmrequest.Pmemory,
			}
		} else {
			required[vmrequest.PMId] = Resources{cpu: vmrequest.Pcpu, memory: vmrequest.Pmemory}
		}
	}
	allpmidpresent := true
	for pmid := range required {
		if _, ok := available[pmid]; !ok {
			allpmidpresent = false
		}
	}
	if allpmidpresent == false {
		muoff.Unlock()
		muon.Unlock()
		return &pb.MigrateVMResponse{Accepted: false}, nil
	}
	allocation := true
	for pmid := range required {
		if required[pmid].cpu > available[pmid].cpu || required[pmid].memory > available[pmid].memory {
			allocation = false
		}
	}
	if allocation == false {
		muoff.Unlock()
		muon.Unlock()
		return &pb.MigrateVMResponse{Accepted: false}, nil
	}
	var chaonpm, chaoffpm []*Item
	for _, temp := range onpm {
		if _, ok := required[temp.PMId]; ok {
			temp.scpu = temp.scpu - required[temp.PMId].cpu
			temp.smemory = temp.smemory - required[temp.PMId].memory
			temp.priority = temp.scpu * temp.smemory
			chaonpm = append(chaonpm, temp)
		} else {
			chaonpm = append(chaonpm, temp)
		}
	}
	for _, itemptr := range chaonpm {
		heap.Push(&leader.onqueue, *itemptr)
	}
	for _, temp := range offpm {
		if _, ok := required[temp.PMId]; ok {
			temp.scpu = temp.scpu - required[temp.PMId].cpu
			temp.smemory = temp.smemory - required[temp.PMId].memory
			temp.priority = temp.scpu * temp.smemory
			// changing state of Physical Machine
			changePhysicalMachineState(leader.client, temp.PMId, true)
			heap.Push(&leader.onqueue, &temp)
		} else {
			chaoffpm = append(chaoffpm, temp)
		}
	}

	for _, itemptr := range chaoffpm {
		heap.Push(&leader.offqueue, *itemptr)
	}

	for _, vmrequest := range req.Assigned {
		// changing state of Virtual Machine
		changeStateOfVM(leader.client, vmrequest.VMId, pbc.VMStatusResponse_MIGRATING)

		// inserting vm to pm mapping
		insertVMToPMMappingMigrating(leader.client, vmrequest.VMId, vmrequest.PMId)

		// inserting pm to vm mapping
		insertPMToVMMappingMigrating(leader.client, vmrequest.VMId, vmrequest.PMId)
	}
	muoff.Unlock()
	muon.Unlock()
	return &pb.MigrateVMResponse{Accepted: true}, nil
}

func StartLeaderProcess(cli *clientv3.Client, lead chan bool, hostName string) {
	go Partition(cli)

	//TODO: Poweroff asynchronously also see migrate keys

	zap.L().Info("Trying to start GRPC Server")
	address := hostName + ":" + string(LeaderPort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		zap.L().Error("Failed to listen",
			zap.Error(err),
		)
	}
	grpcServer := grpc.NewServer()
	leaderServer := LeaderServer{onqueue: make(PriorityQueue, 0), offqueue: offqueue}
	zap.L().Info("Registering GRPC Server")
	pb.RegisterLeaderServer(grpcServer, &leaderServer)
	grpcServer.Serve(lis)

}
