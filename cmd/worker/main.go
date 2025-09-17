package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"mapreduce/pkg/worker"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	master := flag.String("master", "127.0.0.1:50051", "master gRPC address host:port")
	workDir := flag.String("workdir", "./mr-work", "working directory for intermediate/output files")
	id := flag.String("id", "", "worker ID (default host:pid)")
	block := flag.Bool("block", true, "block until connected to master on startup")
	flag.Parse()

	if *id == "" {
		host, _ := os.Hostname()
		*id = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *workDir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if *block {
		opts = append(opts, grpc.WithBlock())
	}

	dialCtx := ctx
	var cancelDial context.CancelFunc
	if *block {
		dialCtx, cancelDial = context.WithTimeout(ctx, 10*time.Second)
		defer cancelDial()
	}

	conn, err := grpc.DialContext(dialCtx, *master, opts...)
	if err != nil {
		log.Fatalf("dial %s: %v", *master, err)
	}
	defer conn.Close()

	w := worker.NewWorker(conn, *workDir, *id, log.New(os.Stdout, "", log.LstdFlags))
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("worker run: %v", err)
	}
}
