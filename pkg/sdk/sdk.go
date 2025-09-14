package sdk

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(filename string, contents []byte) ([]KeyValue, error)
type ReduceFunc func(key string, values []string) (string, error)
