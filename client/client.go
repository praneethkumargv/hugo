package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	pbc "napoleon/controller"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	hostName       string
	port           int
	ContextTimeout = 10 * time.Second
)

func init() {
	flag.StringVar(&hostName, "ip", "localhost", "Controller Port For CRUD Operations")
	flag.IntVar(&port, "port", 8000, "To talk to napolets")
	flag.Parse()
}

func CreateVM(client pbc.VMClient) {
	zap.L().Debug("Contacting Controller")
	ctx, cancel := context.WithTimeout(context.Background(), ContextTimeout)
	defer cancel()
	pms, err := client.CreateVM(ctx,
		&pbc.VMCreateRequest{
			VMName:    "a",
			Vcpus:     6200,
			Memory:    4730,
			Storage:   2,
			ImageName: "praneeth",
		})
	if err != nil {
		zap.L().Error("Error in calling RPC", zap.Error(err))
	}
	// if pms.Accepted == true {
	zap.L().Info("Done", zap.String("VMId is", pms.VMId))
	// }
}

func NewLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{
		"/home/praneeth/go/src/napoleon/client.log",
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

	ipaddress := fmt.Sprintf("%s:%d", hostName, port)
	conn, err := grpc.Dial(ipaddress, grpc.WithInsecure())
	if err != nil {
		zap.L().Error("Failed to dial", zap.Error(err))
	}
	defer conn.Close()
	client := pbc.NewVMClient(conn)
	CreateVM(client)
}
