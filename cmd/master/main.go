package cmd

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"mapreduce/pkg/coordinator"
	rpcpb "mapreduce/pkg/rpc/pb"
	srv "mapreduce/pkg/server"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addr := getenv("MR_ADDR", ":50051")

	lg := log.New(os.Stdout, "master ", log.LstdFlags|log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		lg.Println("shutting down...")
		cancel()
	}()

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
