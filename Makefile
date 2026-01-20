BIN_DIR := bin
PLUGIN := wordcount.so

.PHONY: build plugin demo clean

build:
	go build -o $(BIN_DIR)/master ./cmd/master
	go build -o $(BIN_DIR)/worker ./cmd/worker
	go build -o $(BIN_DIR)/mrctl ./cmd/control

plugin:
	go build -buildmode=plugin -o $(PLUGIN) ./plugins/wordcount

demo: build plugin
	./scripts/run-local.sh

clean:
	rm -rf $(BIN_DIR) mr-work-* mr-master.log $(PLUGIN)
