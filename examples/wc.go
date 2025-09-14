package examples

import (
	"fmt"
	"mapreduce/pkg/worker"
	"strings"
)

func WordCountMap(_ string, contents []byte) ([]worker.KeyValue, error) {
	text := string(contents)
	fields := strings.Fields(text)
	kvs := make([]worker.KeyValue, 0, len(fields))
	for _, word := range fields {
		kvs = append(kvs, worker.KeyValue{Key: strings.ToLower(word), Value: "1"})
	}
	return kvs, nil
}

func WordCountReduce(key string, values []string) (string, error) {
	return fmt.Sprintf("%s %d", key, len(values)), nil
}
