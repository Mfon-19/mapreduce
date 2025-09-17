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
