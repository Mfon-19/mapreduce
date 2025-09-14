package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rpcpb "mapreduce/pkg/rpc/pb"
)

func main() {
	master := flag.String("master", "127.0.0.1:50051", "master gRPC address host:port")
	inputs := flag.String("inputs", "", "comma-separated input file paths")
	nReduce := flag.Int("nreduce", 1, "number of reduce tasks")
	pluginPath := flag.String("plugin", "", "local path to plugin .so (or file:///path)")
	mapSym := flag.String("map", "Map", "map function symbol in plugin")
	reduceSym := flag.String("reduce", "Reduce", "reduce function symbol in plugin")
	timeout := flag.Duration("timeout", 10*time.Second, "RPC timeout")
	block := flag.Bool("block", true, "block until the connection is ready")
	flag.Parse()

	if *inputs == "" || *pluginPath == "" {
		fmt.Fprintf(os.Stderr, "usage: mrctl -master host:port -inputs a.txt,b.txt -nreduce 3 -plugin ./wordcount.so [-map Map -reduce Reduce]\n")
		os.Exit(2)
	}

	plug := strings.TrimPrefix(*pluginPath, "file://")
	if !filepath.IsAbs(plug) {
		abs, err := filepath.Abs(plug)
		if err != nil {
			log.Fatal(err)
		}
		plug = abs
	}
	if _, err := os.Stat(plug); err != nil {
		log.Fatalf("plugin not found: %s (%v)", plug, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if *block {
		opts = append(opts, grpc.WithBlock())
	}

	conn, err := grpc.DialContext(ctx, *master, opts...)
	if err != nil {
		log.Fatalf("dial master: %v", err)
	}
	defer conn.Close()

	cli := rpcpb.NewCoordinatorClient(conn)
	req := &rpcpb.SubmitJobRequest{
		InputFiles:   splitComma(*inputs),
		NReduce:      int32(*nReduce),
		PluginPath:   plug,
		MapSymbol:    *mapSym,
		ReduceSymbol: *reduceSym,
	}
	resp, err := cli.SubmitJob(ctx, req)
	if err != nil {
		log.Fatalf("submit failed: %v", err)
	}
	fmt.Println(resp.JobId)
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
