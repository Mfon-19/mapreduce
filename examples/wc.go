package examples

import (
	"fmt"
	"strings"
	"mapreduce/pkg/sdk"
)

func WordCountMap(_ string, contents []byte) ([]sdk.KeyValue, error) {
	text := string(contents)
	fields := strings.Fields(text)
	kvs := make([]sdk.KeyValue, 0, len(fields))
	for _, word := range fields {
		kvs = append(kvs, sdk.KeyValue{Key: strings.ToLower(word), Value: "1"})
	}
	return kvs, nil
}

func WordCountReduce(key string, values []string) (string, error) {
	return fmt.Sprintf("%s %d", key, len(values)), nil
}
