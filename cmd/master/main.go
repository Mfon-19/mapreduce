package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"mapreduce/pkg/coordinator"
	rpcpb "mapreduce/pkg/rpc/pb"
	srv "mapreduce/pkg/server"
	"net"
	"os"
)

func main() {
	addr := getenv("MR_ADDR", ":50051")

	lg := log.New(os.Stdout, "master ", log.LstdFlags|log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := coordinator.NewMaster(ctx, lg)

	grpcSrv := grpc.NewServer()
	ctx = context.Background()
	s := srv.NewRPCServer(ctx, m, lg)
	rpcpb.RegisterMapReduceServer(grpcSrv, s)
	rpcpb.RegisterCoordinatorServer(grpcSrv, s)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		lg.Fatalf("listen %s: %v", addr, err)
	}
	lg.Printf("listening on %s", addr)
	if err := grpcSrv.Serve(lis); err != nil {
		lg.Fatalf("serve: %v", err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
