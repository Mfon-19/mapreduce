package server

import (
	"context"
	"fmt"
	"log"
	"mapreduce/pkg/coordinator"
	"mapreduce/pkg/core"
	rpcpb "mapreduce/pkg/rpc/pb"
	"time"
)

type RPCServer struct {
	rpcpb.UnimplementedMapReduceServer
	rpcpb.UnimplementedCoordinatorServer
	master *coordinator.Master
	log    *log.Logger
}

func NewRPCServer(ctx context.Context, master *coordinator.Master, log *log.Logger) *RPCServer {
	return &RPCServer{
		master: master,
		log:    log,
	}
}

func (server *RPCServer) GetTask(ctx context.Context, request *rpcpb.GetTaskArgs) (*rpcpb.GetTaskReply, error) {
	workerID := request.GetWorkerId()
	spec, ok := server.master.PollTask(workerID)
	nMap, nReduce := server.master.Totals()
	jobID, path, mapSym, reduceSym, haveJob := server.master.JobInfo()

	if !ok {
		if !server.master.Active() {
			return &rpcpb.GetTaskReply{
				Type:    rpcpb.TaskType_TaskExit,
				NMap:    int32(nMap),
				NReduce: int32(nReduce),
			}, nil
		}
		return &rpcpb.GetTaskReply{
			Type:         rpcpb.TaskType_TaskNone,
			NMap:         int32(nMap),
			NReduce:      int32(nReduce),
			JobId:        jobID,
			PluginPath:   path,
			MapSymbol:    mapSym,
			ReduceSymbol: reduceSym,
		}, nil
	}

	reply := &rpcpb.GetTaskReply{
		NMap:         int32(nMap),
		NReduce:      int32(nReduce),
		JobId:        jobID,
		PluginPath:   path,
		MapSymbol:    mapSym,
		ReduceSymbol: reduceSym,
	}

	if haveJob {
		switch spec.Type {
		case core.MapTask:
			reply.Type = rpcpb.TaskType_TaskMap
			reply.MapId = int32(spec.ID)
			reply.Filename = spec.Filename
		case core.ReduceTask:
			reply.Type = rpcpb.TaskType_TaskReduce
			reply.ReduceId = int32(spec.ID)
		case core.TaskExit:
			reply.Type = rpcpb.TaskType_TaskExit
		default:
			reply.Type = rpcpb.TaskType_TaskNone
		}
	}

	return reply, nil
}

func (server *RPCServer) ReportTask(ctx context.Context, request *rpcpb.ReportTaskArgs) (*rpcpb.ReportTaskReply, error) {
	workerID := request.GetWorkerId()

	result := core.TaskResult{OK: request.Success}
	switch request.Type {
	case rpcpb.TaskType_TaskMap:
		result.Type = core.MapTask
		result.ID = int(request.MapId)
	case rpcpb.TaskType_TaskReduce:
		result.Type = core.ReduceTask
		result.ID = int(request.ReduceId)
	default:
		return &rpcpb.ReportTaskReply{}, nil
	}

	_, err := server.master.ReportTaskResult(workerID, result)
	if err != nil && server.log != nil {
		server.log.Printf("ReportTask error: %v", err)
	}
	return &rpcpb.ReportTaskReply{}, nil
}

func (server *RPCServer) SubmitJob(ctx context.Context, request *rpcpb.SubmitJobRequest) (*rpcpb.SubmitJobResponse, error) {
	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	const lease = 30 * time.Second

	if err := server.master.SubmitJob(jobID, request.InputFiles, int(request.NReduce), lease,
		request.PluginPath, request.MapSymbol, request.ReduceSymbol); err != nil {
		return nil, err
	}
	return &rpcpb.SubmitJobResponse{JobId: jobID}, nil
}
