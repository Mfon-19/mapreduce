# Go MapReduce Framework

This project is a simple framework for distributed MapReduce jobs in Go. It's inspired by the original Google MapReduce paper. It uses gRPC for communication between the master and worker nodes. Map and Reduce functions are provided as Go plugins, allowing users to define their own jobs.

## Project Structure

- `cmd/`: Contains the source code for the cli.
  - `master/`: The coordinator process.
  - `worker/`: The worker process that executes map and reduce tasks.
  - `control/`: A command-line tool (named `mrctl` when built) to submit jobs.
- `pkg/`: Contains the core library code.
  - `coordinator/`: The master's implementation.
  - `worker/`: The worker's implementation.
  - `rpc/`: gRPC protocol definitions and generated code.
  - `sdk/`: The user-facing SDK for writing MapReduce jobs.
- `plugins/`: Example MapReduce jobs.
  - `wordcount/`: A classic word count example.
- `data/`: Sample input data.

## Getting Started

### Prerequisites

- Go 1.18 or later.
- A C compiler (like GCC) for building Go plugins.

### Building

1.  **Build the master, worker, and control tool:**

    The following commands will create the `master`, `worker`, and `mrctl` executables in a `bin/` directory.

    ```sh
    go build -o bin/master ./cmd/master
    go build -o bin/worker ./cmd/worker
    go build -o bin/mrctl ./cmd/control
    ```

2.  **Build the wordcount plugin:**

    ```sh
    go build -buildmode=plugin -o wordcount.so ./plugins/wordcount
    ```

## Usage

Here's how to run the included word count example.

### 1. Start the Master

The master node coordinates the MapReduce job.

```sh
./bin/master
```

By default, it listens on `:50051`. You can change this by setting the `MR_ADDR` environment variable:

```sh
MR_ADDR=:12345 ./bin/master
```

### 2. Start Workers

Workers connect to the master and wait for tasks. You can start as many as you like.

```sh
./bin/worker -master 127.0.0.1:50051
```

Each worker needs its own working directory. The default is `./mr-work`. For multiple workers on the same machine, you should specify different work directories:

```sh
./bin/worker -master 127.0.0.1:50051 -workdir ./mr-work-1
./bin/worker -master 127.0.0.1:50051 -workdir ./mr-work-2
```

### 3. Submit a Job

Use the `mrctl` tool to submit the wordcount job.
The `mrctl` executable is built from the `cmd/control` source code.

```sh
./bin/mrctl -master 127.0.0.1:50051 \
  -inputs data/a.txt,data/b.txt \
  -nreduce 2 \
  -plugin ./wordcount.so
```

- `-inputs`: A comma-separated list of input files.
- `-nreduce`: The number of reduce tasks.
- `-plugin`: The path to the compiled job plugin (`.so` file).

After the job completes, the output will be in files named `mr-out-*.txt` in the workers' working directories.

## Writing Your Own Job

To create a new MapReduce job, you need to create a Go package that defines `Map` and `Reduce` functions with the correct signatures, as defined in `pkg/sdk/sdk.go`.

Here is the `wordcount` example (`plugins/wordcount/main.go`):

```go
package main

import (
	"fmt"
	"mapreduce/pkg/sdk"
	"strings"
)

var Map sdk.MapFunc = func(filename string, contents []byte) ([]sdk.KeyValue, error) {
	text := string(contents)
	fields := strings.Fields(text)
	kvs := make([]sdk.KeyValue, 0, len(fields))
	for _, word := range fields {
		kvs = append(kvs, sdk.KeyValue{Key: strings.ToLower(word), Value: "1"})
	}
	return kvs, nil
}

var Reduce sdk.ReduceFunc = func(key string, values []string) (string, error) {
	return fmt.Sprintf("%s %d", key, len(values)), nil
}
```

Then, build it as a plugin:

```sh
go build -buildmode=plugin -o myjob.so ./plugins/myjob
```

And submit it:

```sh
./bin/mrctl -plugin ./myjob.so ...
```
