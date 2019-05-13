package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
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

const (
	alpha          = float64(0.25)
	L              = 6
	ContextTimeout = 10 * time.Second
)

// sum 7, z 7, last deviation, last prediction, count, alpha - 0.25, l = 6 --- for both memory and cpu
type ResourcePrediction struct {
	sum, z    [L + 1]float64
	dev, pred float64
}

type ResourcesVM struct {
	cpu, mem *ResourcePrediction
	count    uint64
}

var (
	hostName    string
	port        int
	pmid        string
	TotalMemory uint
	TotalCpu    uint
	jobid       chan SendThrough
	lock        sync.Mutex
	jbres       map[string][]JobResources
	prediction  map[string]*ResourcesVM
	vm          map[string]*pb.CreateVMRequest
)

func init() {
	flag.StringVar(&hostName, "ip", "localhost", "Controller Port For CRUD Operations")
	flag.IntVar(&port, "port", 8000, "To talk to napolets")
	flag.StringVar(&pmid, "pmid", "pm_12", "To talk to napolets")
	flag.UintVar(&TotalMemory, "mem", 10000, "To talk to napolets")
	flag.UintVar(&TotalCpu, "cpu", 25000, "To talk to napolets")
	flag.Parse()
}

type napolet struct {
	pm     pbt.PM
	vmstat map[string]*pb.VMStat
	pmstat map[string]*pb.PMStat
}

func (obj *napolet) CreateVM(ctx context.Context, req *pb.CreateVMRequest) (resp *pb.CreateVMResponse, err error) {
	vm[req.VMId] = req
	elem := SendThrough{name: req.VMName, id: req.VMId}
	zap.L().Info("Received a VM Create Request",
		zap.String("VM Name", req.VMName),
	)
	jobid <- elem
	zap.L().Info("Got Exited from jobid")
	resp = &pb.CreateVMResponse{Accepted: true}
	return
}

func (obj *napolet) DeleteVM(ctx context.Context, req *pb.DeleteVMRequest) (resp *pb.DeleteVMResponse, err error) {
	resp = &pb.DeleteVMResponse{Accepted: false}
	resp.Accepted = deleteVM(req.VMId)
	return
}

func deleteVM(vmid string) (done bool) {
	lock.Lock()
	defer lock.Unlock()
	done_counter := 0
	if _, ok := vm[vmid]; ok {
		zap.L().Debug("Found resources related to VM and deleting it")
		delete(vm, vmid)
		done_counter++
	}
	if _, ok := jbres[vmid]; ok {
		zap.L().Debug("Found resources related to job resources and deleting it")
		delete(jbres, vmid)
		done_counter++
	}
	if _, ok := prediction[vmid]; ok {
		zap.L().Debug("Found resources related to prediction and deleting it")
		delete(prediction, vmid)
		done_counter++
	}
	done = false
	if done_counter == 3 {
		done = true
		zap.L().Debug("Succesfully deleted every info about VM on this PM")
	}
	return
}

func (obj *napolet) MigrateVM(ctx context.Context, req *pb.MigrateVMRequest) (resp *pb.MigrateVMResponse, err error) {
	resp = &pb.MigrateVMResponse{
		Accepted: false,
	}
	VMId := req.VMId
	ipaddress := req.IPAddress

	if request, ok := vm[VMId]; ok {
		zap.L().Debug("Found resources related to VM and migrating it")
		zap.L().Debug("Trying to connect with napolet")
		conn, err := grpc.Dial(ipaddress, grpc.WithInsecure())
		if err != nil {
			zap.L().Error("Failed to dial", zap.Error(err))
		}
		zap.L().Debug("Connected with napolet")
		defer conn.Close()
		client := pb.NewPingClient(conn)
		done := SendCreateReqToNapolet(client, request)
		if done == true {
			zap.L().Debug("Deleting Resources of VM on the selected PM")
			deleteVM(VMId)
		}
		resp.Accepted = done
	}
	return
}

func SendCreateReqToNapolet(client pb.PingClient, req *pb.CreateVMRequest) (done bool) {
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

func (obj *napolet) GetStat(ctx context.Context, req *pb.Dummy) (*pb.Stat, error) {
	var resp = new(pb.Stat)
	var pcpu, pmem uint32
	zap.L().Info("Trying to access all VMS on PM")
	if len(vm) != 0 {
		// lock.Lock()
		pcpu = 0
		pmem = 0
		for vmid := range vm {
			if _, ok := prediction[vmid]; !ok {
				// lock.Unlock()
				break
			}
			cpu := prediction[vmid].cpu
			mem := prediction[vmid].mem
			pvm := &pb.VMStat{
				VMId:            vmid,
				State:           "Created",
				PredictedCpu:    uint32(cpu.pred),
				PredictedMemory: uint32(mem.pred),
			}
			resp.VMS = append(resp.VMS, pvm)
			pcpu = pcpu + pvm.PredictedCpu
			pmem = pmem + pvm.PredictedMemory
		}
		var scpu, smem uint32
		if pcpu > uint32(TotalCpu) {
			scpu = 0
		} else {
			scpu = uint32(TotalCpu) - pcpu
		}
		if pmem > uint32(TotalMemory) {
			smem = 0
		} else {
			smem = uint32(TotalMemory) - (pmem)
		}
		resp.PM = &pb.PMStat{
			PMId:        pmid,
			TotalMemory: uint32(TotalMemory),
			TotalCpu:    uint32(TotalCpu),
			SlackCpu:    scpu,
			SlackMemory: smem,
		}
		// lock.Unlock()
		zap.L().Debug("", zap.String("PMId", pmid),
			zap.Uint32("Total Memory", uint32(TotalMemory)),
			zap.Uint32("Total CPU", uint32(TotalCpu)),
			zap.Uint32("Slack CPU", scpu),
			zap.Uint32("Slack Memory", smem),
		)
		return resp, nil
	}
	resp.PM = &pb.PMStat{
		PMId:        pmid,
		TotalMemory: uint32(TotalMemory),
		TotalCpu:    uint32(TotalCpu),
		SlackCpu:    uint32(TotalMemory),
		SlackMemory: uint32(TotalCpu),
	}

	zap.L().Debug("Function about to return")
	return resp, nil
}

func calculate() {
	for {
		lock.Lock()
		for vmid, jobres := range jbres {
			zap.L().Debug("", zap.String("VMId", vmid))
			var index uint64
			var resourcesVM *ResourcesVM
			if resources, ok := prediction[vmid]; ok {
				zap.L().Debug("Past created VM")
				index = resources.count - 1
				resources.count = resources.count + 1
				if int(index) >= len(jobres) {
					index = uint64(len(jobres) - 1)
				}
				// zap.L().Debug("Predicting Resources for", zap.Uint64("index", index))
				// zap.L().Debug("Predicting Resources for", zap.Int("length", len(jobres)))
				resourcesVM = resources
			} else {
				zap.L().Debug("Newly created VM")
				index = 0
				count := uint64(2)
				prediction[vmid] = &ResourcesVM{count: count}
				resourcesVM = prediction[vmid]
				resourcesVM.cpu = &ResourcePrediction{}
				resourcesVM.mem = &ResourcePrediction{}
			}
			resusage := jobres[index]
			acpu := resusage.cpu
			amemory := resusage.mem
			zap.L().Debug("Actual CPU Usage and Memory Usage",
				zap.Uint32("CPU", acpu),
				zap.Uint32("Memory", amemory),
			)
			cpu := resourcesVM.cpu
			mem := resourcesVM.mem

			if index == 0 {
				cpu.dev = 0
				// changing this row
				// cpu.pred = float64(vm[vmid].Vcpus)
				cpu.pred = 0
				cpu.z[0] = float64(acpu)
				cpu.sum[0] = float64(acpu * acpu)
				mem.dev = 0
				// changing this row
				// mem.pred = float64(vm[vmid].Memory)
				mem.pred = 0
				mem.z[0] = float64(amemory)
				mem.sum[0] = float64(amemory * amemory)
			} else if index <= L-1 {
				// EMWA
				cpu.dev = 0
				// (1.0-alpha)*cpu.dev + alpha*(float64(acpu)-cpu.pred)
				mem.dev = 0
				// (1.0-alpha)*mem.dev + alpha*(float64(amemory)-mem.pred)
				cpu.pred = float64(vm[vmid].Vcpus)
				mem.pred = float64(vm[vmid].Memory)
				for i := int(index - 1); i >= 0; i-- {
					if i < 0 {
						break
					}
					zap.L().Debug("", zap.Int("", len(cpu.z)), zap.Int("", int(i)))
					cpu.z[i+1] = cpu.z[i]
					mem.z[i+1] = mem.z[i]
				}
				cpu.z[0] = float64(acpu)
				mem.z[0] = float64(amemory)
				for i := 0; i <= int(index); i++ {
					cpu.sum[i] = cpu.sum[i] + cpu.z[0]*cpu.z[i]
					mem.sum[i] = mem.sum[i] + mem.z[0]*mem.z[i]
				}
			} else if index >= L {
				// For Exponential Moving Average
				cpu.dev = (1.0-alpha)*cpu.dev + alpha*(float64(acpu)-cpu.pred)
				mem.dev = (1.0-alpha)*mem.dev + alpha*(float64(amemory)-mem.pred)
				if index == L {
					cpu.dev = 0
					mem.dev = 0
				}

				// Weiner Algorithm starts
				for i := int(L - 1); i >= 0; i-- {
					cpu.z[i+1] = cpu.z[i]
					mem.z[i+1] = mem.z[i]
				}
				cpu.z[0] = float64(acpu)
				mem.z[0] = float64(amemory)
				for i := 0; i <= int(index) && i < L+1; i++ {
					cpu.sum[i] = cpu.sum[i] + cpu.z[0]*cpu.z[i]
					mem.sum[i] = mem.sum[i] + mem.z[0]*mem.z[i]
				}
				var arrcpua, arrmema [36]float64
				var arr2cpu, arr2mem []float64
				for i := 0; i < L; i++ {
					arr2cpu = append(arr2cpu, cpu.sum[i+1]/float64((int(index)-i+1)))
					arr2mem = append(arr2mem, mem.sum[i+1]/float64((int(index)-i+1)))
					k := 0
					for j := i; j < L; j++ {
						arrcpua[i*L+j] = cpu.sum[k] / float64((int(index) - i))
						arrmema[i*L+j] = mem.sum[k] / float64((int(index) - i))
						arrcpua[i+L*j] = cpu.sum[k] / float64((int(index) - i))
						arrmema[i+L*j] = mem.sum[k] / float64((int(index) - i))
						k++
					}
				}
				var arrcpu, arrmem []float64
				for i := 0; i < L*L; i++ {
					arrcpu = append(arrcpu, arrcpua[i])
					arrmem = append(arrmem, arrmema[i])
				}

				var arr3cpu, arr3mem []float64
				for i := 0; i < L; i++ {
					arr3cpu = append(arr3cpu, cpu.z[L-i-1])
					arr3mem = append(arr3mem, mem.z[L-i-1])
				}
				cpuPredMat := mat.MakeDenseMatrix(arrcpu, 6, 6)
				memPredMat := mat.MakeDenseMatrix(arrmem, 6, 6)
				cpu2PredMat := mat.MakeDenseMatrix(arr2cpu, 1, 6)
				mem2PredMat := mat.MakeDenseMatrix(arr2mem, 1, 6)
				cpuPredMatInv, err := cpuPredMat.Inverse()
				if err != nil {
					zap.L().Error("error", zap.Error(err))
					cpu.pred = float64(acpu)
					mem.pred = float64(amemory)
					continue
				}
				memPredMatInv, err := memPredMat.Inverse()
				if err != nil {
					cpu.pred = float64(acpu)
					mem.pred = float64(amemory)
					zap.L().Error("error", zap.Error(err))
					continue
				}
				prodCpuWeights := mat.Product(cpuPredMatInv, cpu2PredMat.Transpose())
				prodMemWeights := mat.Product(memPredMatInv, mem2PredMat.Transpose())

				anscpu := mat.Product(mat.MakeDenseMatrix(arr3cpu, 1, 6), prodCpuWeights)
				ansmem := mat.Product(mat.MakeDenseMatrix(arr3mem, 1, 6), prodMemWeights)

				var cpupred, mempred float64
				cpupred = anscpu.Get(0, 0)
				mempred = ansmem.Get(0, 0)

				zap.L().Debug("Predicted",
					zap.Float64("CPU", cpupred),
					zap.Float64("Memory", mempred),
				)

				// Exponential Moving Average
				cpu.pred = cpupred + cpu.dev
				mem.pred = mempred + mem.dev

				// Base Conditions
				if cpu.pred > float64(vm[vmid].Vcpus) {
					cpu.pred = float64(vm[vmid].Vcpus)
				}
				if mem.pred > float64(vm[vmid].Memory) {
					mem.pred = float64(vm[vmid].Memory)
				}
				if cpu.pred < 0 {
					cpu.pred = 0
				}
				if mem.pred < 0 {
					mem.pred = 0
				}
			}
			zap.L().Info("The details of job resources for VM is",
				zap.String("Virtual Machine", vmid),
			)
			zap.L().Info("", zap.Float64("CPU DEVIATION", cpu.dev),
				zap.Float64("CPU PREDICTION", cpu.pred),
				// zap.Float64s("Previous CPU Usage", cpu.z[:]),
				// zap.Float64s("Sum for Wiener Filtering", cpu.sum[:]),
			)
			zap.L().Info("", zap.Float64("MEM DEVIATION", mem.dev),
				zap.Float64("MEM PREDICTION", mem.pred),
				// zap.Float64s("Previous CPU Usage", mem.z[:]),
				// zap.Float64s("Sum for Wiener Filtering", mem.sum[:]),
			)
		}
		lock.Unlock()
		time.Sleep(1000 * time.Millisecond)
		// time.Sleep(5 * time.Minute)
	}
}

func parse() {
	jbres = make(map[string][]JobResources)
	for {
		zap.L().Info("Waiting for a job Id")
		elem := <-jobid
		zap.L().Info("got a job")
		filename := elem.name
		id := elem.id
		zap.L().Debug("Received a File", zap.String("FileName", filename))

		file, err := os.Open(filename + ".json")
		if err != nil {
			zap.L().Error("", zap.Error(err))
		}

		var cpu, mem float64
		var arr []JobResources
		for {
			n, _ := fmt.Fscanln(file, &cpu, &mem)
			if n == 0 {
				break
			}
			fmt.Println(cpu, mem)
			res := JobResources{cpu: uint32(cpu), mem: uint32(mem)}
			arr = append(arr, res)
		}

		lock.Lock()
		jbres[id] = arr
		lock.Unlock()
		file.Close()
	}
}

func NewLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{
		"/home/praneeth/go/src/napoleon/stub.log",
	}
	return cfg.Build()
}

func main() {

	// logger, err := NewLogger()
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Logger Error")
	}
	zap.ReplaceGlobals(logger)
	defer zap.L().Sync()

	zap.L().Info("Trying to start GRPC Server")
	address := fmt.Sprintf("%s:%d", hostName, port)
	zap.L().Info("Master Server starting on",
		zap.String("IPAddress", address),
	)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		zap.L().Error("Failed to listen",
			zap.Error(err),
		)
	}
	vm = make(map[string]*pb.CreateVMRequest)
	jobid = make(chan SendThrough)
	prediction = make(map[string]*ResourcesVM)
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
