//go:build !windows

package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"mapreduce/pkg/coordinator"
	rpcpb "mapreduce/pkg/rpc/pb"
	"mapreduce/pkg/server"
	"mapreduce/pkg/worker"
)

const (
	sampleFiles    = 16
	sampleFileSize = 256 * 1024
	sampleReduce   = 4
)

func TestPerformanceSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in -short")
	}
	if os.Getenv("MR_BENCH") == "" {
		t.Skip("set MR_BENCH=1 to run perf test")
	}

	if runtime.GOMAXPROCS(0) < 4 {
		runtime.GOMAXPROCS(4)
	}

	root := repoRoot(t)
	pluginPath := buildPlugin(t, root)

	files := envInt("MR_BENCH_FILES", sampleFiles)
	size := envInt("MR_BENCH_FILE_SIZE", sampleFileSize)
	reduce := envInt("MR_BENCH_REDUCE", sampleReduce)
	if files <= 0 || size <= 0 || reduce <= 0 {
		t.Fatalf("invalid bench config: files=%d size=%d reduce=%d", files, size, reduce)
	}

	inputs := makeDataset(t, files, size)

	run1 := runJob(t, root, pluginPath, inputs, 1, reduce)
	run4 := runJob(t, root, pluginPath, inputs, 4, reduce)

	p95_1 := p95(run1.taskLatencies)
	p95_4 := p95(run4.taskLatencies)
	speedup := float64(run1.jobDuration) / float64(run4.jobDuration)

	t.Logf("dataset: files=%d size=%dB reduce=%d", files, size, reduce)
	t.Logf("1 worker: job=%s p95=%s tasks=%d", run1.jobDuration, p95_1, len(run1.taskLatencies))
	t.Logf("4 workers: job=%s p95=%s tasks=%d", run4.jobDuration, p95_4, len(run4.taskLatencies))
	t.Logf("speedup 1->4: %.2fx", speedup)

	if os.Getenv("MR_ASSERT_PERF") != "" {
		if speedup < 3.0 {
			t.Fatalf("speedup below 3x: %.2fx", speedup)
		}
		if p95_4 > 200*time.Millisecond {
			t.Fatalf("p95 above 200ms: %s", p95_4)
		}
	}
}

type runResult struct {
	jobDuration   time.Duration
	taskLatencies []time.Duration
}

func runJob(t *testing.T, root, pluginPath string, inputs []string, workers, nReduce int) runResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	lg := log.New(io.Discard, "", 0)
	master := coordinator.NewMaster(ctx, lg)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	rpcSrv := server.NewRPCServer(ctx, master, lg)
	rpcpb.RegisterMapReduceServer(grpcSrv, rpcSrv)
	rpcpb.RegisterCoordinatorServer(grpcSrv, rpcSrv)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	defer func() {
		grpcSrv.Stop()
		_ = lis.Close()
		master.Close()
	}()

	addr := lis.Addr().String()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	defer conn.Close()

	cli := rpcpb.NewCoordinatorClient(conn)
	_, err = cli.SubmitJob(ctx, &rpcpb.SubmitJobRequest{
		InputFiles:   inputs,
		NReduce:      int32(nReduce),
		PluginPath:   pluginPath,
		MapSymbol:    "Map",
		ReduceSymbol: "Reduce",
	})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	workRoot := t.TempDir()
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, len(inputs)+nReduce)

	start := time.Now()
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		workdir := filepath.Join(workRoot, workerID)
		if err := os.MkdirAll(workdir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", workdir, err)
		}

		wconn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			t.Fatalf("dial worker: %v", err)
		}

		w := worker.NewWorker(wconn, workdir, workerID, lg)
		w.SetTaskObserver(func(taskType rpcpb.TaskType, taskID int32, duration time.Duration, ok bool) {
			if !ok {
				return
			}
			mu.Lock()
			latencies = append(latencies, duration)
			mu.Unlock()
		})

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer wconn.Close()
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker %s run: %v", workerID, err)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("job timeout: %v", ctx.Err())
	}

	if len(latencies) < len(inputs)+nReduce {
		t.Fatalf("too few task samples: got %d want >= %d", len(latencies), len(inputs)+nReduce)
	}

	return runResult{
		jobDuration:   time.Since(start),
		taskLatencies: latencies,
	}
}

func buildPlugin(t *testing.T, root string) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "wordcount.so")
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", out, "./plugins/wordcount")
	cmd.Dir = root
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build plugin: %v\n%s", err, string(output))
	}
	return out
}

func makeDataset(t *testing.T, files, size int) []string {
	t.Helper()

	dir := t.TempDir()
	words := []string{
		"alpha", "beta", "gamma", "delta", "epsilon", "zeta",
		"eta", "theta", "iota", "kappa", "lambda", "mu",
		"nu", "xi", "omicron", "pi", "rho", "sigma",
		"tau", "upsilon", "phi", "chi", "psi", "omega",
	}
	rng := rand.New(rand.NewSource(1))
	inputs := make([]string, 0, files)

	for i := 0; i < files; i++ {
		var b strings.Builder
		b.Grow(size + 64)
		for b.Len() < size {
			word := words[rng.Intn(len(words))]
			b.WriteString(word)
			b.WriteByte(' ')
		}
		path := filepath.Join(dir, fmt.Sprintf("input-%02d.txt", i))
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		inputs = append(inputs, path)
	}

	return inputs
}

func p95(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i] < ordered[j]
	})
	index := int(math.Ceil(0.95*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root not found from %s", wd)
	}
	return root
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
