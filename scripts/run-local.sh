#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ADDR="${ADDR:-127.0.0.1:50051}"
WORKERS="${WORKERS:-2}"
NREDUCE="${NREDUCE:-2}"
INPUTS="${INPUTS:-data/a.txt,data/b.txt}"
PLUGIN_PATH="${PLUGIN_PATH:-./wordcount.so}"
MAP_SYM="${MAP_SYM:-Map}"
REDUCE_SYM="${REDUCE_SYM:-Reduce}"

mkdir -p "$ROOT_DIR/bin"

go build -o "$ROOT_DIR/bin/master" ./cmd/master
go build -o "$ROOT_DIR/bin/worker" ./cmd/worker
go build -o "$ROOT_DIR/bin/mrctl" ./cmd/control

if [[ "$PLUGIN_PATH" == "./wordcount.so" || "$PLUGIN_PATH" == "$ROOT_DIR/wordcount.so" ]]; then
	go build -buildmode=plugin -o "$ROOT_DIR/wordcount.so" ./plugins/wordcount
fi

if [[ "$PLUGIN_PATH" != /* ]]; then
	PLUGIN_PATH="$ROOT_DIR/$PLUGIN_PATH"
fi

MASTER_LOG="${MASTER_LOG:-$ROOT_DIR/mr-master.log}"

master_pid=""
worker_pids=()

cleanup() {
	for pid in "${worker_pids[@]:-}"; do
		if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
			kill "$pid" 2>/dev/null || true
		fi
	done
	if [[ -n "${master_pid}" ]] && kill -0 "$master_pid" 2>/dev/null; then
		kill "$master_pid" 2>/dev/null || true
	fi
}
trap cleanup EXIT INT TERM

MR_ADDR="$ADDR" "$ROOT_DIR/bin/master" >"$MASTER_LOG" 2>&1 &
master_pid=$!

for ((i = 1; i <= WORKERS; i++)); do
	workdir="$ROOT_DIR/mr-work-$i"
	mkdir -p "$workdir"
	worker_log="$workdir/worker.log"
	"$ROOT_DIR/bin/worker" -master "$ADDR" -workdir "$workdir" -id "worker-$i" >"$worker_log" 2>&1 &
	worker_pids+=("$!")
done

job_id="$("$ROOT_DIR/bin/mrctl" -master "$ADDR" -inputs "$INPUTS" -nreduce "$NREDUCE" -plugin "$PLUGIN_PATH" -map "$MAP_SYM" -reduce "$REDUCE_SYM")"
printf "job id: %s\n" "$job_id"

set +e
for pid in "${worker_pids[@]}"; do
	wait "$pid"
done
set -e

if kill -0 "$master_pid" 2>/dev/null; then
	kill "$master_pid" 2>/dev/null || true
fi
wait "$master_pid" 2>/dev/null || true

printf "outputs: %s\n" "$ROOT_DIR/mr-work-*/mr-out-*.txt"
printf "master log: %s\n" "$MASTER_LOG"
