package main

import (
	"container/heap"
	"context"
	"fmt"
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
	pmtovm    sync.Mutex
	pmstate   sync.Mutex
	pmmigrate sync.Mutex
	// offqueue  = make(PriorityQueue, 0)
)

type LeaderServer struct {
	onqueue  PriorityQueue
	offqueue PriorityQueue
	client   *clientv3.Client
}

// Master Hostname should be like "s1" to "s9"
func Partition(cli *clientv3.Client, leader *LeaderServer) {
	// TODO: CHANGE ONLY NO OF PMS WERE ON
	for {
		start := time.Now()

		muoff.Lock()
		muon.Lock()
		zap.L().Info("Starting partition")
		zap.L().Debug("Trying to start partitioning the available physical machines to available masters")
		key := "pm_" + fmt.Sprintf("%d", 0)
		end := "pm_" + fmt.Sprintf("%d", 9999999)
		zap.L().Info("Key Starting and Ending",
			zap.String("Start", key),
			zap.String("End", end),
		)

		resp := GetKeyRangeResp(cli, key, end)
		values := resp.Kvs
		zap.L().Debug("No of Values returned from range response from Physical Machine(PM)",
			zap.Int("No of PM's", len(values)),
		)

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
		zap.L().Info("Found no of PM's that are Powered ON",
			zap.Int("Number of Physical Machines", noOfPM),
		)
		zap.L().Info("Found these no of systems are powered off",
			zap.Int("Powered Off PM's", len(arroff)),
		)

		key = "master_s" + fmt.Sprintf("%d", 1)
		end = "master_s" + fmt.Sprintf("%d", 9)
		zap.L().Info("Master Starting and Ending",
			zap.String("Start", key),
			zap.String("End", end),
		)

		masterResp := GetKeyRangeResp(cli, key, end)
		masters := masterResp.Kvs
		noOfMasters := len(masters)
		zap.L().Info("Found no of Masters",
			zap.Int("Number of masters", noOfMasters),
		)

		noOfIp := math.Ceil(float64(noOfPM) / float64(noOfMasters))
		zap.L().Info("Found no of IP's",
			zap.Float64("Number of PM's per Master", noOfIp),
		)

		i := 0
		for _, master := range masters {
			var partition pbtype.Partition
			for j := 0; j < int(noOfIp) && i < noOfPM; j++ {
				partition.Ipaddress = append(partition.Ipaddress, arron[i].Ipaddress)
				i++
				zap.L().Debug("Mapping of Partition",
					zap.String("partition_"+string(master.Value), partition.Ipaddress[j]),
				)
			}
			out, err := proto.Marshal(&partition)
			if err != nil {
				zap.L().Error("Marshalling Error", zap.Error(err))
			}

			//KEY: partition_$(hostname) VALUE: protobuf of IPAddress
			key := "partition_" + string(master.Key)
			zap.L().Debug("Trying to insert 'partition_$(hostname) key'",
				zap.String("Partition Key", key),
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

		zap.L().Debug("Trying to insert all the OFF PM's into Heap for use")
		leader.offqueue = leader.offqueue[:0]
		for i, offpm := range arroff {
			item := &Item{
				PMId:     keyoff[i],
				scpu:     offpm.Vcpu,
				smemory:  offpm.Memory,
				priority: offpm.Vcpu * offpm.Memory,
				index:    i,
			}
			leader.offqueue = append(leader.offqueue, item)
		}
		zap.L().Debug("Trying to initiate the Heap with OFF PM's",
			zap.Int("No of Off PM's", len(leader.offqueue)),
		)
		heap.Init(&leader.offqueue)
		zap.L().Debug("Trying to initiate the Heap with OFF PM's",
			zap.Int("No of Off PM's", len(leader.offqueue)),
		)
		zap.L().Debug("All OFF PM's are inserted into Heap")
		muon.Unlock()
		muoff.Unlock()

		elapsed := time.Since(start)
		zap.L().Info("Time Taken for Partiton is ",
			zap.Duration("Partition Time Elapsed", elapsed),
		)
		zap.L().Debug("Sleeping until next period for partition")
		time.Sleep(partitionUpdateInterval)
	}

}

func changeStateOfVM(client *clientv3.Client, vmkey string, status pbc.VMStatusResponse_Status) {
	zap.L().Debug("Trying to change the state of VM to ",
		zap.Any("To State", status),
	)
	resp := GetKeyResp(context.Background(), client, vmkey)
	value := getKeyValue(resp)
	var temp pbc.VMStatusResponse
	error := proto.Unmarshal([]byte(value), &temp)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	zap.L().Debug("Present Status", zap.Any("Status", temp.Status))
	// changed here
	if temp.Status == status {
		zap.L().Debug("After Status", zap.Any("Status", temp.Status))
		return
	}
	temp.Status = status
	zap.L().Debug("After Status", zap.Any("Status", temp.Status))

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
	zap.L().Debug("Creating relation between vm and pm",
		zap.String("VMId", vmkey),
		zap.String("PMId", pmkey),
	)
	vmkey = vmkey + "_on"
	value := pmkey
	resp := InsertKey(client, vmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", resp))
	zap.L().Info("Key is inserted",
		zap.String("Key", vmkey),
	)
}

func insertVMToPMMappingMigrating(client *clientv3.Client, vmkey string, pmkey string) {
	zap.L().Debug("Inserting relation between VM to PM for migating",
		zap.String("Virtual machine Id", vmkey),
		zap.String("Physical Machine Id", pmkey),
	)
	vmkey = "migrating_" + vmkey
	value := pmkey
	resp := InsertKey(client, vmkey, value)
	zap.L().Debug("The returned response is", zap.Any("Response", resp))
	zap.L().Info("Key is inserted",
		zap.String("Key", vmkey),
	)
}

func insertPMToVMMapping(client *clientv3.Client, vmkey string, pmkey string) {
	zap.L().Debug("Creating relation between pm and vm",
		zap.String("Physical Machine Id", pmkey),
		zap.String("Virtuall Machine Id", vmkey),
	)

	var list pbtype.VMONPM
	key := "on_" + pmkey

	// Because another thread can be changing the pm to vm mapping
	// TODO: LOCKING ON KEY LEVEL SHOULD BE DONE
	pmtovm.Lock()
	defer pmtovm.Unlock()
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
	zap.L().Debug("Inserting relation between PM to VM for migating",
		zap.String("Physical Machine Id", pmkey),
		zap.String("Virtual machine Id", vmkey),
	)
	var list pbtype.VMONPM
	key := "migrating_" + pmkey
	pmmigrate.Lock()
	defer pmmigrate.Unlock()
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
	zap.L().Debug("Trying to change the state of PM",
		zap.Bool("State ON or OFF", state),
	)
	pmstate.Lock()
	defer pmstate.Unlock()
	resp := GetKeyResp(context.Background(), client, pmkey)
	value := getKeyValue(resp)
	var temp pbtype.PM
	error := proto.Unmarshal([]byte(value), &temp)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}

	zap.L().Debug("Present Status", zap.Any("Status", temp.State))
	temp.State = state
	zap.L().Debug("After Status", zap.Any("Status", temp.State))

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

func printElements(queue PriorityQueue) {
	zap.L().Debug("Printing elements in queue")
	for _, item := range queue {
		zap.L().Debug("", zap.String("PMId", item.PMId),
			zap.Uint32("Slack CPU", item.scpu),
			zap.Uint32("Slack Memory", item.smemory),
			zap.Uint32("Priority", item.priority),
			zap.Int("Index", item.index),
		)
	}
}

func (leader *LeaderServer) CreateNewVM(ctx context.Context, req *pb.CreateNewVMRequest) (*pb.CreateNewVMResponse, error) {
	var temp []*Item

	vmid := req.VMId
	pmid := req.PMId
	pcpu := req.Pcpu
	pmem := req.Pmemory
	zap.L().Debug("VMCreate Request came for",
		zap.String("vmid", vmid),
		zap.String("pmid", pmid),
		zap.Uint32("CPU", pcpu),
		zap.Uint32("Memory", pmem),
	)
	done := false
	anotherOnPM := false
	var check *Item

	zap.L().Debug("Trying to get hold on ONQueue")
	muon.Lock()
	zap.L().Debug("Got lock on OnQueue")
	printElements(leader.onqueue)
	length := len(leader.onqueue)
	for i := 0; i < TopOnElements && i < length; i++ {
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
		heap.Push(&leader.onqueue, itemptr)
	}
	muon.Unlock()
	zap.L().Debug("Lock Released on ONQueue")

	if done == true {
		zap.L().Debug("Found a ON PM")
		// changing state of Virtual Machine
		changeStateOfVM(leader.client, vmid, pbc.VMStatusResponse_CREATING)

		// inserting vm to pm mapping
		insertVMToPMMapping(leader.client, vmid, check.PMId)

		// inserting pm to vm mapping
		insertPMToVMMapping(leader.client, vmid, check.PMId)

	} else if anotherOnPM == false {
		zap.L().Info("Can't find a ON PM, falling back to OFF PM")
		temp = make([]*Item, 0)
		var onPM *Item
		zap.L().Debug("Trying to Get Lock on Off queue")
		muoff.Lock()
		zap.L().Debug("Locked OFF queue")
		printElements(leader.offqueue)
		length := len(leader.offqueue)
		for i := 0; i < TopOffElements && i < length; i++ {
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
			heap.Push(&leader.offqueue, itemptr)
		}
		zap.L().Debug("Unlocking OFF queue")
		muoff.Unlock()
		zap.L().Debug("Unlocked OFF queue")

		if done == true {
			zap.L().Debug("Found a OFF PM")

			muon.Lock()
			//changing the state of Physical Machine
			changePhysicalMachineState(leader.client, onPM.PMId, true)
			zap.L().Debug("Inserting the ONed PM to ON Queue",
				zap.String("Physical Machine Id", onPM.PMId),
			)
			heap.Push(&leader.onqueue, onPM)
			muon.Unlock()

			// changing state of Virtual Machine
			changeStateOfVM(leader.client, vmid, pbc.VMStatusResponse_CREATING)

			// inserting vm to pm mapping
			insertVMToPMMapping(leader.client, vmid, onPM.PMId)

			// inserting pm to vm mapping
			insertPMToVMMapping(leader.client, vmid, onPM.PMId)
		}

	}
	return &pb.CreateNewVMResponse{Accepted: done}, nil
}

func (leader *LeaderServer) DeleteVM(ctx context.Context, req *pb.DeleteVMRequest) (*pb.DeleteVMResponse, error) {
	done := false
	vmid := req.VMId

	// TODO: Writers Lock and Readers Lock
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
	zap.L().Debug("Entered to retrieve Physical Machines", zap.Bool("Queue Name", state))
	var lock *sync.Mutex
	var temp []*Item
	if state == true {
		lock = &muon
	} else {
		lock = &muoff
	}

	zap.L().Debug("Trying to Lock the", zap.Bool("Queue Name", state))
	lock.Lock()
	defer lock.Unlock()
	printElements(*queue)
	length := len(*queue)
	zap.L().Debug("Length Of Queue", zap.Int("Length", length))
	for i := uint32(0); i < noofpm && i < uint32(length); i++ {
		check := heap.Pop(queue).(*Item)
		temp = append(temp, check)
		//There may be PM's of even zero slack on ON queue
		if check.scpu == 0 || check.smemory == 0 {
			zap.L().Debug("Found a PM with zero slack cpu or zero slack memory",
				zap.Uint32("Slack CPU", check.scpu),
				zap.Uint32("Slack Memory", check.smemory),
			)
			break
		}
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
		heap.Push(queue, itemptr)
	}
	zap.L().Debug("Trying to UnLock the", zap.Bool("Queue Name", state))
}

func (leader *LeaderServer) RetrieveStateChanges(ctx context.Context, req *pb.StateRequest) (*pb.StateResponse, error) {
	onpm := req.NumOn
	offpm := req.NumOff
	var resp pb.StateResponse

	length := len(leader.onqueue)
	zap.L().Debug("Length Of Queue", zap.Int("Length", length))
	heapOperation(leader.client, &leader.onqueue, &resp, true, onpm)
	length = len(leader.offqueue)
	zap.L().Debug("Length Of Queue", zap.Int("Length", length))
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
	defer muon.Unlock()
	printElements(leader.onqueue)
	for i, node := range leader.onqueue {
		if node.PMId == pmid {
			zap.L().Info("Found the given pmid in ONQueue")
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
		zap.L().Info("Cannot find the given pmid in OnQueue")
		zap.L().Info("So inserting it in the OnQueue")
		item := &Item{
			PMId:     pmid,
			scpu:     scpu,
			smemory:  smemory,
			priority: scpu * smemory,
		}
		heap.Push(&leader.onqueue, item)
		done = true
	}

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
	defer muoff.Unlock()
	defer muon.Unlock()
	var onpm, offpm []*Item
	available := make(map[string]Resources)
	required := make(map[string]Resources)
	printElements(leader.onqueue)
	lenOnQueue := len(leader.onqueue)
	printElements(leader.offqueue)
	lenOffQueue := len(leader.offqueue)
	for i := 0; i < TopOnElements && i < lenOnQueue; i++ {
		check := heap.Pop(&leader.onqueue).(*Item)
		onpm = append(onpm, check)
		available[check.PMId] = Resources{cpu: check.scpu, memory: check.smemory}
	}
	for i := 0; i < TopOffElements && i < lenOffQueue; i++ {
		check := heap.Pop(&leader.offqueue).(*Item)
		offpm = append(offpm, check)
		available[check.PMId] = Resources{cpu: check.scpu, memory: check.smemory}
	}
	// Finding out how much every PM requires
	for _, vmrequest := range req.Assigned {
		if resource, ok := required[vmrequest.PMId]; ok {
			// If Key Exists
			required[vmrequest.PMId] = Resources{
				cpu:    resource.cpu + vmrequest.Pcpu,
				memory: resource.memory + vmrequest.Pmemory,
			}
		} else {
			// If key doesn't exists
			required[vmrequest.PMId] = Resources{cpu: vmrequest.Pcpu, memory: vmrequest.Pmemory}
		}
	}
	allpmidpresent := true
	for pmid := range required {
		if _, ok := available[pmid]; !ok {
			zap.L().Debug("A PMID is not present", zap.String("PMID NOT PRESENT", pmid))
			allpmidpresent = false
		}
	}
	if allpmidpresent == false {
		zap.L().Debug("All PMID are not present")
		for _, itemptr := range onpm {
			heap.Push(&leader.onqueue, itemptr)
		}
		for _, itemptr := range offpm {
			heap.Push(&leader.offqueue, itemptr)
		}
		return &pb.MigrateVMResponse{Accepted: false}, nil
	}
	// Is important
	allocation := true
	for pmid := range required {
		if required[pmid].cpu > available[pmid].cpu || required[pmid].memory > available[pmid].memory {
			zap.L().Debug("", zap.String("PMID", pmid),
				zap.Uint32("Required CPU", required[pmid].cpu),
				zap.Uint32("Available CPU", available[pmid].cpu),
				zap.Uint32("Required Memory", required[pmid].memory),
				zap.Uint32("Available Memory", available[pmid].memory),
			)
			allocation = false
		}
		zap.L().Debug("", zap.String("PMID", pmid),
			zap.Uint32("Required CPU", required[pmid].cpu),
			zap.Uint32("Available CPU", available[pmid].cpu),
			zap.Uint32("Required Memory", required[pmid].memory),
			zap.Uint32("Available Memory", available[pmid].memory),
		)
	}
	if allocation == false {
		zap.L().Debug("There are no available resources present on the destined PM's")
		for _, itemptr := range onpm {
			heap.Push(&leader.onqueue, itemptr)
		}
		for _, itemptr := range offpm {
			heap.Push(&leader.offqueue, itemptr)
		}
		return &pb.MigrateVMResponse{Accepted: false}, nil
	}
	var chaonpm, chaoffpm []*Item
	// only change the priority of those which are present as destinations to migrate
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
		heap.Push(&leader.onqueue, itemptr)
	}
	// only change the priority of those which are present as destinations to migrate
	for _, temp := range offpm {
		if _, ok := required[temp.PMId]; ok {
			temp.scpu = temp.scpu - required[temp.PMId].cpu
			temp.smemory = temp.smemory - required[temp.PMId].memory
			temp.priority = temp.scpu * temp.smemory
			// changing state of Physical Machine
			changePhysicalMachineState(leader.client, temp.PMId, true)
			heap.Push(&leader.onqueue, temp)
		} else {
			chaoffpm = append(chaoffpm, temp)
		}
	}

	for _, itemptr := range chaoffpm {
		heap.Push(&leader.offqueue, itemptr)
	}

	for _, vmrequest := range req.Assigned {
		// changing state of Virtual Machine
		zap.L().Debug("Trying to acquire etcdstore lock for migrating")
		etcdstore.Lock()
		zap.L().Debug("Acquired etcdstore lock for migrating")
		changeStateOfVM(leader.client, vmrequest.VMId, pbc.VMStatusResponse_MIGRATING)

		// inserting vm to pm mapping
		insertVMToPMMappingMigrating(leader.client, vmrequest.VMId, vmrequest.PMId)

		// inserting pm to vm mapping
		insertPMToVMMappingMigrating(leader.client, vmrequest.VMId, vmrequest.PMId)
		zap.L().Debug("Trying to release etcdstore lock for migrating")
		etcdstore.Unlock()
		zap.L().Debug("etcdstore lock released")
	}

	return &pb.MigrateVMResponse{Accepted: true}, nil
}

func StartLeaderProcess(cli *clientv3.Client, lead chan bool, hostName, ipaddress string) {
	zap.L().Info("Leader Process Started")
	zap.L().Info("Now every master can start sending requests to leader")
	//TODO: Poweroff asynchronously also see migrate keys

	zap.L().Info("Trying to start GRPC Server")
	address := fmt.Sprintf("%s:%d", ipaddress, LeaderPort)
	zap.L().Debug("Trying to listen on", zap.String("Address", address))
	lis, err := net.Listen("tcp", address)
	if err != nil {
		zap.L().Error("Failed to listen",
			zap.Error(err),
		)
	}
	grpcServer := grpc.NewServer()
	leaderServer := LeaderServer{onqueue: make(PriorityQueue, 0), offqueue: make(PriorityQueue, 0), client: cli}

	go Partition(cli, &leaderServer)

	zap.L().Info("Registering GRPC Server")
	pb.RegisterLeaderServer(grpcServer, &leaderServer)
	zap.L().Debug("gRPC Server started")
	grpcServer.Serve(lis)

}
