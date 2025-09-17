package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	rpcpb "mapreduce/pkg/rpc/pb"
	"mapreduce/pkg/sdk"
	"math/rand"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
)

type Worker struct {
	id       string
	client   rpcpb.MapReduceClient
	workDir  string
	mapf     sdk.MapFunc
	reducef  sdk.ReduceFunc
	lg       *log.Logger
	curJobID string
}

func NewWorker(conn *grpc.ClientConn, workDir string, workerID string, lg *log.Logger) *Worker {
	if lg == nil {
		lg = log.New(os.Stdout, "worker", log.LstdFlags|log.Lmicroseconds)
	}
	return &Worker{
		id:      workerID,
		client:  rpcpb.NewMapReduceClient(conn),
		workDir: workDir,
		lg:      lg,
	}
}

func (worker *Worker) Run(ctx context.Context) error {
	backoff := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rep, err := worker.client.GetTask(ctx, &rpcpb.GetTaskArgs{WorkerId: worker.id})
		if err != nil {
			worker.lg.Printf("GetTask error: %v", err)
			backoff = boundedJitterBackoff(backoff)
			time.Sleep(backoff)
			continue
		}

		switch rep.Type {
		case rpcpb.TaskType_TaskNone:
			time.Sleep(250 * time.Millisecond)
			continue

		case rpcpb.TaskType_TaskExit:
			worker.lg.Println("received TaskExit; shutting down")
			return nil

		case rpcpb.TaskType_TaskMap:
			mapf, _, err := worker.ensurePlugin(rep.JobId, rep.PluginPath, rep.MapSymbol, rep.ReduceSymbol)
			if err != nil {
				worker.lg.Printf("ensurePlugin(job=%s) failed: %v", rep.JobId, err)
				_, _ = worker.client.ReportTask(ctx, &rpcpb.ReportTaskArgs{
					Type:     rpcpb.TaskType_TaskMap,
					MapId:    rep.MapId,
					Success:  false,
					WorkerId: worker.id,
				})
				time.Sleep(300 * time.Millisecond)
				continue
			}

			ok := worker.doMap(ctx, int(rep.MapId), rep.Filename, int(rep.NReduce), mapf)
			_, _ = worker.client.ReportTask(ctx, &rpcpb.ReportTaskArgs{
				Type:     rpcpb.TaskType_TaskMap,
				MapId:    rep.MapId,
				Success:  ok,
				WorkerId: worker.id,
			})

		case rpcpb.TaskType_TaskReduce:
			_, reducef, err := worker.ensurePlugin(rep.JobId, rep.PluginPath, rep.MapSymbol, rep.ReduceSymbol)
			if err != nil {
				worker.lg.Printf("ensurePlugin(job=%s) failed: %v", rep.JobId, err)
				_, _ = worker.client.ReportTask(ctx, &rpcpb.ReportTaskArgs{
					Type:     rpcpb.TaskType_TaskReduce,
					MapId:    rep.ReduceId,
					Success:  false,
					WorkerId: worker.id,
				})
				time.Sleep(300 * time.Millisecond)
				continue
			}

			ok := worker.doReduce(ctx, int(rep.ReduceId), int(rep.NMap), reducef)
			_, _ = worker.client.ReportTask(ctx, &rpcpb.ReportTaskArgs{
				Type:     rpcpb.TaskType_TaskReduce,
				ReduceId: rep.ReduceId,
				Success:  ok,
				WorkerId: worker.id,
			})

		default:
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (worker *Worker) ensurePlugin(jobID, pluginPath, mapSym, reduceSym string) (sdk.MapFunc, sdk.ReduceFunc, error) {
	if worker.curJobID == jobID && worker.mapf != nil && worker.reducef != nil {
		return worker.mapf, worker.reducef, nil
	}
	if pluginPath == "" {
		return nil, nil, fmt.Errorf("empty plugin path for job %s", jobID)
	}

	local := pluginPath
	if strings.HasPrefix(local, "file://") {
		local = strings.TrimPrefix(local, "file://")
	}
	abs, err := filepath.Abs(local)
	if err != nil {
		return nil, nil, err
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, nil, err
	}

	p, err := plugin.Open(abs)
	if err != nil {
		return nil, nil, err
	}

	mp, err := p.Lookup(mapSym)
	if err != nil {
		return nil, nil, err
	}
	rp, err := p.Lookup(reduceSym)
	if err != nil {
		return nil, nil, err
	}

	mapfPtr, ok := mp.(*sdk.MapFunc)
	if !ok {
		return nil, nil, fmt.Errorf("map symbol %s has wrong type: expected a *sdk.MapFunc, got %T", mapSym, mp)
	}
	reducefPtr, ok := rp.(*sdk.ReduceFunc)
	if !ok {
		return nil, nil, fmt.Errorf("reduce symbol %s has wrong type: expected a *sdk.ReduceFunc, got %T", reduceSym, rp)
	}
	mapf := *mapfPtr
	reducef := *reducefPtr

	worker.curJobID = jobID
	worker.mapf = mapf
	worker.reducef = reducef
	return mapf, reducef, nil
}

// doMap reads the input file, applies mapf, partitions the result into nReduce buckets,
// and writes files with name mr-<mapID>-<reduceID>.
func (worker *Worker) doMap(ctx context.Context, mapID int, filename string, nReduce int, mapf sdk.MapFunc) bool {
	start := time.Now()
	data, err := os.ReadFile(filename)
	if err != nil {
		worker.lg.Printf("map %d: read %s: %v", mapID, filename, err)
		return false
	}
	pairs, err := mapf(filename, data)
	if err != nil {
		worker.lg.Printf("map %d: mapf failed: %v", mapID, err)
		return false
	}

	// make nReduce partitions, and one encoder per partition
	temps := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for r := 0; r < nReduce; r++ {
		path := worker.tmpPath(fmt.Sprintf("mr-%d-%d.json", mapID, r))
		file, err := os.Create(path)
		if err != nil {
			worker.lg.Printf("map %d: create %s: %v", mapID, path, err)
			return false
		}
		temps[r] = file
		encoders[r] = json.NewEncoder(file)
	}

	for _, kv := range pairs {
		p := partition(kv.Key, nReduce)
		if err := encoders[p].Encode(&kv); err != nil {
			worker.lg.Printf("map %d: encode partition %d: %v", mapID, p, err)
			for _, f := range temps {
				if f != nil {
					f.Close()
					_ = os.Remove(f.Name())
				}
			}
			return false
		}
	}

	// rename to final names
	for r := 0; r < nReduce; r++ {
		file := temps[r]
		_ = file.Sync()
		_ = file.Close()
		final := filepath.Join(worker.workDir, fmt.Sprintf("mr-%d-%d.json", mapID, r))
		if err := os.Rename(file.Name(), final); err != nil {
			worker.lg.Printf("map %d: rename -> %s: %v", mapID, final, err)
			return false
		}
	}
	worker.lg.Printf("map %d: wrote %d pairs in %s", mapID, len(pairs), time.Since(start))
	return true
}

// doReduce reads all mr-<mapID>-<reduceID> files produced by doMap, groups by key, applies reducef,
// and finally writes files with name mr-out-<reduceID>.
func (worker *Worker) doReduce(ctx context.Context, reduceID int, nMap int, reducef sdk.ReduceFunc) bool {
	start := time.Now()
	var kvs []sdk.KeyValue
	for m := 0; m < nMap; m++ {
		path := filepath.Join(worker.workDir, fmt.Sprintf("mr-%d-%d.json", m, reduceID))
		file, err := os.Open(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				worker.lg.Printf("reduce %d: missing shard %s (skipping)", reduceID, path)
				continue
			}
			worker.lg.Printf("reduce %d: open %s: %v", reduceID, path, err)
			return false
		}

		decoder := json.NewDecoder(bufio.NewReader(file))
		for {
			var kv sdk.KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				_ = file.Close()
				worker.lg.Printf("reduce %d: decode %s: %v", reduceID, path, err)
				return false
			}
			kvs = append(kvs, kv)
		}
		_ = file.Close()
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[i].Key
	})

	tmp := worker.tmpPath(fmt.Sprintf("mr-out-%d.txt", reduceID))
	out, err := os.Create(tmp)
	if err != nil {
		worker.lg.Printf("reduce %d: create %s: %v", reduceID, tmp, err)
		return false
	}

	bw := bufio.NewWriter(out)
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		key := kvs[i].Key
		var vals []string
		for k := i; k < j; k++ {
			vals = append(vals, kvs[k].Value)
		}
		line, err := reducef(key, vals)
		if err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			worker.lg.Printf("reduce %d: reducef key=%q: %v", reduceID, key, err)
			return false
		}
		if _, err := bw.WriteString(line + "\n"); err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			worker.lg.Printf("reduce %d: write %v", reduceID, err)
			return false
		}
		i = j
	}
	_ = bw.Flush()
	_ = out.Sync()
	_ = out.Close()

	final := filepath.Join(worker.workDir, fmt.Sprintf("mr-out-%d.txt", reduceID))
	if err := os.Rename(tmp, final); err != nil {
		worker.lg.Printf("reduce %d: rename -> %s: %v", reduceID, final, err)
		return false
	}
	worker.lg.Printf("reduce %d: wrote %s in %s (inputs=%d)", reduceID, final, time.Since(start), len(kvs))
	return true
}

func (worker *Worker) tmpPath(name string) string {
	base := strings.TrimSuffix(name, ".tmp")
	return filepath.Join(worker.workDir, fmt.Sprintf("%s.%s.tmp", base, worker.id))
}

func partition(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

func boundedJitterBackoff(prev time.Duration) time.Duration {
	if prev <= 0 {
		prev = 100 * time.Millisecond
	}
	next := time.Duration(float64(prev) * (1.6 + 0.2*(rand.Float64()-0.5)))
	if next > 3*time.Second {
		next = 3 * time.Second
	}
	return next
}
