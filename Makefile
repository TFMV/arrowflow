.PHONY: build run-producer run-consumer run-arrowflow benchmark experiment

build:
	go build -o bin/arrowflow ./cmd/arrowflow
	go build -o bin/producer ./cmd/producer
	go build -o bin/consumer ./cmd/consumer
	go build -o bin/benchmark ./cmd/benchmark
	go build -o bin/experiment ./cmd/experiment
	go build -o bin/chaos-producer ./cmd/chaos-producer

run-producer:
	./bin/arrowflow producer

run-consumer:
	./bin/arrowflow consumer

benchmark:
	./bin/arrowflow benchmark

experiment:
	./bin/arrowflow experiment

chaos:
	./bin/arrowflow chaos
