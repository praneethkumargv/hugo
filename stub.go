package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"sync"
	"time"

	pb "napoleon/napolet"
	pbt "napoleon/types"

	mat "github.com/skelterjohn/go.matrix"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type JobResources struct {
	cpu uint32
	mem uint32
}

type SendThrough struct {
	name, id string
}

// sum 7, z 7, last deviation, last prediction, count, alpha - 0.25, l = 6 --- for both memory and cpu
type ResourcePrediction struct {
	sum, z    [7]float64
	dev, pred float64
}

type ResourcesVM struct {
	cpu, mem *ResourcePrediction
	count    uint64
}

const (
	alpha = float64(0.25)
)

var (
	hostName    string
	port        int
	pmid        string
	TotalMemory uint32
	TotalCpu    uint32
	jobid       chan SendThrough
	lock        sync.Mutex
	jbres       map[string][]JobResources
	prediction  map[string]*ResourcesVM
	vm          map[string]*pb.CreateVMRequest
)

type napolet struct {
	pm     pbt.PM
	vmstat map[string]*pb.VMStat
	pmstat map[string]*pb.PMStat
}

func (obj *napolet) CreateVM(ctx context.Context, req *pb.CreateVMRequest) (resp *pb.CreateVMResponse, err error) {
	elem := SendThrough{name: req.VMName, id: req.VMId}
	jobid <- elem
	vm[req.VMId] = req
	resp = &pb.CreateVMResponse{Accepted: true}
	return
}

func (obj *napolet) DeleteVM(ctx context.Context, req *pb.DeleteVMRequest) (resp *pb.DeleteVMResponse, err error) {
	resp = &pb.DeleteVMResponse{Accepted: false}
	if _, ok := vm[req.VMId]; ok {
		delete(vm, req.VMId)
		resp.Accepted = true
	}
	return
}

func (obj *napolet) GetStat(ctx context.Context, req *pb.Dummy) (resp *pb.Stat, err error) {
	var pcpu, pmem uint32
	for vmid, vminfo := range vm {
		pvm := &pb.VMStat{
			VMId:            vmid,
			State:           "Created",
			PredictedCpu:    uint32(prediction[vmid].cpu.pred),
			PredictedMemory: uint32(prediction[vmid].mem.pred),
		}
		resp.VMS = append(resp.VMS, pvm)
		pcpu = pcpu + pvm.PredictedCpu
		pmem = pmem + pvm.PredictedMemory
	}
	var scpu, smem uint32
	if pcpu > TotalCpu {
		scpu = 0
	} else {
		scpu = TotalCpu - pcpu
	}
	if pmem > TotalMemory {
		smem = 0
	} else {
		smem = TotalMemory - pmem
	}

	resp.PM = &pb.PMStat{
		PMId:        pmid,
		TotalMemory: TotalMemory,
		TotalCpu:    TotalCpu,
		SlackCpu:    scpu,
		SlackMemory: smem,
	}
	return
}

func calculate() {
	for {
		lock.Lock()
		for vmid, jobres := range jbres {
			var index uint64
			var resourcesVM *ResourcesVM
			if resources, ok := prediction[vmid]; ok {
				index = resources.count - 1
				resourcesVM = resources
			} else {
				index = 0
				count := uint64(1)
				prediction[vmid] = &ResourcesVM{count: count}
				resourcesVM = prediction[vmid]
				resourcesVM.cpu = &ResourcePrediction{}
				resourcesVM.mem = &ResourcePrediction{}
			}
			resusage := jobres[index]
			acpu := resusage.cpu
			amemory := resusage.mem
			cpu := resourcesVM.cpu
			mem := resourcesVM.mem

			if index == 0 {
				cpu.dev = 0
				cpu.pred = float64(vm[vmid].Vcpus)
				cpu.z[0] = float64(acpu)
				cpu.sum[0] = float64(acpu * acpu)
				mem.dev = 0
				mem.pred = float64(vm[vmid].Memory)
				mem.z[0] = float64(amemory)
				mem.sum[0] = float64(amemory * amemory)
			} else if index <= 5 {
				// EMWA
				cpu.dev = (1.0-alpha)*cpu.dev + alpha*(float64(acpu)-cpu.pred)
				mem.dev = (1.0-alpha)*mem.dev + alpha*(float64(amemory)-mem.pred)
				cpu.pred = float64(vm[vmid].Vcpus)
				mem.pred = float64(vm[vmid].Memory)
				for i := index; i >= 0; i-- {
					cpu.z[i+1] = cpu.z[i]
					mem.z[i+1] = mem.z[i]
				}
				cpu.z[0] = float64(acpu)
				mem.z[0] = float64(amemory)
				for i := 0; i <= int(index); i++ {
					cpu.sum[i] = cpu.sum[i] + cpu.z[0]*cpu.z[i]
					mem.sum[i] = mem.sum[i] + mem.z[0]*mem.z[i]
				}
			} else if index >= 6 {
				cpu.dev = (1.0-alpha)*cpu.dev + alpha*(float64(acpu)-cpu.pred)
				mem.dev = (1.0-alpha)*mem.dev + alpha*(float64(amemory)-mem.pred)
				for i := index; i >= 0; i-- {
					cpu.z[i+1] = cpu.z[i]
					mem.z[i+1] = mem.z[i]
				}
				cpu.z[0] = float64(acpu)
				mem.z[0] = float64(amemory)
				for i := 0; i <= int(index) && i < 7; i++ {
					cpu.sum[i] = cpu.sum[i] + cpu.z[0]*cpu.z[i]
					mem.sum[i] = mem.sum[i] + mem.z[0]*mem.z[i]
				}
				var arrcpua, arrmema [36]float64
				var arr2cpu, arr2mem []float64
				for i := 0; i < 6; i++ {
					arr2cpu = append(arr2cpu, cpu.sum[i+1])
					arr2mem = append(arr2mem, mem.sum[i+1])
					for j := i; j < 6; j++ {
						arrcpua[i*6+j] = cpu.sum[j]
						arrmema[i*6+j] = mem.sum[j]
						arrcpua[i+6*j] = cpu.sum[j]
						arrmema[i+6*j] = mem.sum[j]
					}

				}
				var arrcpu, arrmem []float64
				for i := 0; i < 36; i++ {
					arrcpu = append(arrcpu, arrcpua[i])
					arrmem = append(arrmem, arrmema[i])
				}

				var arr3cpu, arr3mem []float64
				for i := 1; i <= 6; i++ {
					arr3cpu = append(arr3cpu, cpu.z[i])
					arr3mem = append(arr3mem, mem.z[i])
				}
				cpuPredMat := mat.MakeDenseMatrix(arrcpu, 6, 6)
				memPredMat := mat.MakeDenseMatrix(arrmem, 6, 6)
				cpu2PredMat := mat.MakeDenseMatrix(arr2cpu, 1, 6)
				mem2PredMat := mat.MakeDenseMatrix(arr2mem, 1, 6)
				cpuPredMatInv, _ := cpuPredMat.Inverse()
				memPredMatInv, _ := memPredMat.Inverse()
				prodCpuWeights := mat.Product(cpuPredMatInv, cpu2PredMat.Transpose())
				prodMemWeights := mat.Product(memPredMatInv, mem2PredMat.Transpose())
				anscpu := (mat.Product(prodCpuWeights, mat.MakeDenseMatrix(arr3cpu, 6, 1)))
				ansmem := mat.Product(prodMemWeights, mat.MakeDenseMatrix(arr3mem, 6, 1))
				cpu.pred = anscpu.Get(1, 1) + cpu.dev
				mem.pred = ansmem.Get(1, 1) + mem.dev
				if cpu.pred > float64(vm[vmid].Vcpus) {
					cpu.pred = float64(vm[vmid].Vcpus)
				}
				if mem.pred > float64(vm[vmid].Memory) {
					mem.pred = float64(vm[vmid].Memory)
				}
			}
		}
		lock.Unlock()
		time.Sleep(5 * time.Minute)
	}
}

func parse() {
	jbres = make(map[string][]JobResources)
	for {
		elem := <-jobid
		filename := elem.name
		id := elem.id
		file, err := ioutil.ReadFile(filename + ".json")
		if err != nil {
			zap.L().Error("Read File Error")
		}
		var data []JobResources
		err = json.Unmarshal([]byte(file), &data)
		if err != nil {
			zap.L().Error("Read File Error")
		}
		lock.Lock()
		jbres[id] = data
		lock.Unlock()
	}
}

func main() {
	// TODO: physical Machine
	zap.L().Info("Trying to start GRPC Server")
	address := hostName + ":" + string(port)
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
	napo := napolet{}
	zap.L().Info("Registering GRPC Server")
	pb.RegisterPingServer(grpcServer, &napo)
	zap.L().Info("Master listening on",
		zap.String("IPaddress", address),
	)
	go parse()
	go calculate()
	grpcServer.Serve(lis)
}
