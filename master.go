package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	pb "napoleon/controller"
	pbtype "napoleon/types"

	clientv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	schedulingQueueLength   = 1000 // the buffer for scheduling queue
	noOfReadRPCs            = 10
	uniqueIdLength          = 64
	partitionUpdateInterval = 10 * time.Second
)

var (
	leader    string
	partition pbtype.Partition // should need to discuss about partition format
	mu        sync.Mutex
	sched     sync.Mutex
)

type VM struct {
	client *clientv3.Client
	queue  chan string
}

// This function will run when there is a create VM request
// KEY for every VM
// KEY: vm_$(64bitid) value: *pb.VMStatusResponse
func (v *VM) CreateVM(ctx context.Context, req *pb.VMCreateRequest) (resp *pb.VMCreateResponse, err error) {
	zap.L().Debug("Got a CreateVM request",
		zap.String("VM Name", req.VMName),
		zap.Uint32("CPUs", req.Vcpus),
		zap.Uint32("Memory", req.Memory),
	)
	vm := &pb.VMStatusResponse{Vm: req}
	vm.Status = pb.VMStatusResponse_PENDING
	number := make([]byte, uniqueIdLength)
	var vminfo string
	for {
		_, err := rand.Read(number)
		if err != nil {
			zap.L().Error("Error in Generating Random Number",
				zap.Error(err),
			)
		}

		key := "vm_" + string(number)
		zap.L().Info("Key is generated", zap.String("Key", key))
		vm.VMId = key

		value, err := proto.Marshal(vm)
		if err != nil {
			zap.L().Error("Marshalling Error", zap.Error(err))
		}

		vminfo = string(value)
		inserted := InsertKeyIfKeyNotPresent(v.client, key, vminfo)
		if inserted == true {
			break
		}
	}
	resp = &pb.VMCreateResponse{VMId: vm.VMId}

	sched.Lock()
	v.queue <- string(1)
	v.queue <- vminfo
	sched.Unlock()

	zap.L().Info("Inserted into scheduling queue and in pending state")
	return
}

func (v *VM) DeleteVM(ctx context.Context, req *pb.VMDeleteRequest) (*pb.VMDeleteResponse, error) {
	zap.L().Debug("Got a DeleteVM request",
		zap.String("VMId", req.VMId),
	)
	key := req.VMId
	getResp := GetKeyResp(context.Background(), v.client, key)
	if isKeyPresent(getResp) == false {
		zap.L().Info("The key is not present. So, it can't be deleted", zap.String("Key", key))
		return &pb.VMDeleteResponse{Accepted: false}, errors.New("The given VMId is not present")
	}
	temp := getKeyValue(getResp)
	var value = new(pb.VMStatusResponse)
	error := proto.Unmarshal([]byte(temp), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	zap.L().Info("Value is returned for a given VMID", zap.String("Value", temp))
	if value.Status == 0 || value.Status == 1 || value.Status == 4 {
		zap.L().Info("The VM is not in a state to delete. So, it can't be deleted", zap.Any("Status", value.Status))
		return &pb.VMDeleteResponse{Accepted: false}, errors.New("The given VMId can't be deleted at present")
	}
	if value.Status == 2 || value.Status == 3 {
		value.Status = pb.VMStatusResponse_DELETE_PENDING
		newValue, err := proto.Marshal(value)
		if err != nil {
			zap.L().Error("Marshalling Error", zap.Error(err))
		}
		resp := InsertKey(v.client, key, string(newValue))
		zap.L().Debug("The returned response is", zap.Any("Response", resp))
		zap.L().Info("Key is inserted",
			zap.String("Key", key),
		)
		sched.Lock()
		v.queue <- string(2)
		v.queue <- string(newValue)
		sched.Unlock()

	}
	return &pb.VMDeleteResponse{Accepted: true}, nil
}

func (v *VM) StatusVM(ctx context.Context, req *pb.VMStatusRequest) (*pb.VMStatusResponse, error) {
	zap.L().Info("Got A Status VM request",
		zap.String("VMId", req.VMId),
	)
	key := req.VMId
	getResp := GetKeyResp(context.Background(), v.client, key)
	if isKeyPresent(getResp) == false {
		zap.L().Info("The key is not present.", zap.String("Key", key))
		return &pb.VMStatusResponse{}, errors.New("The given VMId is not present")
	}
	temp := getKeyValue(getResp)
	var value = new(pb.VMStatusResponse)
	error := proto.Unmarshal([]byte(temp), value)
	if error != nil {
		zap.L().Error("Unmarshalling Error", zap.Error(error))
	}
	zap.L().Info("Value is returned for a given VMID", zap.String("Value", temp))
	return value, nil
}

func getKeyValue(resp *clientv3.GetResponse) string {
	return (string(resp.Kvs[0].Value))
}

func isKeyPresent(resp *clientv3.GetResponse) bool {
	if len(resp.Kvs) == 0 {
		return true
	}
	return false
}

// This function will take care of leader changes and partition changes
// Partition Key
// KEY: partition_master_$(hostName)
func CheckLeader(cli *clientv3.Client, lead chan bool, hostName string) {
	zap.L().Info("Taking care of Leader Changes")
	for {
		zap.L().Debug("Checking for leader for next period")
		resp := GetKeyResp(context.Background(), cli, LeaderKey)

		// should use reader and writer lock
		mu.Lock()
		leader = getKeyValue(resp)
		mu.Unlock()

		zap.L().Info("Leader Information is known",
			zap.String("Leader", leader),
		)

		zap.L().Debug("Updating the partition for next period")
		partitionKey := "partition_master_" + hostName
		resp = GetKeyResp(context.Background(), cli, partitionKey)

		mu.Lock()
		temp := getKeyValue(resp)
		error := proto.Unmarshal([]byte(temp), &partition)
		zap.L().Error("Error in Unmarshalling", zap.Error(error))
		mu.Unlock()

		zap.L().Info("Partition Information is known",
			zap.String("Partition", leader),
		)

		_ = <-lead
	}
}

func MasterUpdation(mast chan bool) {
	for {
		zap.L().Debug("Master Update started for next period")
		time.Sleep(partitionUpdateInterval)
		mast <- true
	}
}

func MasterServer(cli *clientv3.Client, hostName string, queue chan string) {
	zap.L().Info("Trying to start GRPC Server")
	address := hostName + ":" + string(ControllerPort)
	zap.L().Info("Master Server starting on",
		zap.String("IPAddress", address),
	)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		zap.L().Error("Failed to listen",
			zap.Error(err),
		)
	}
	zap.L().Info("Starting gRPC Server")
	grpcServer := grpc.NewServer()
	vmserver := VM{client: cli, queue: queue}
	zap.L().Info("Registering GRPC Server")
	pb.RegisterVMServer(grpcServer, &vmserver)
	zap.L().Info("Master listening on",
		zap.String("IPaddress", address),
	)
	grpcServer.Serve(lis)
}

func Controller(cli *clientv3.Client, lead chan bool, hostName string) {
	zap.L().Info("Controller is Started")
	go CheckLeader(cli, lead, hostName)

	schedulingQueue := make(chan string, schedulingQueueLength)
	go Scheduler(cli, schedulingQueue)
	go ReadRPC(cli, schedulingQueue)
	MasterServer(cli, hostName, schedulingQueue)
}
