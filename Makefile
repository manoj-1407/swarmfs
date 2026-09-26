.PHONY: build test test-v test-race test-integration bench lint clean experiment analyze

build:
	go build -mod=vendor ./cmd/tracker ./cmd/peer ./cmd/experiment ./cmd/analyze

test:
	go test -mod=vendor ./internal/... ./pkg/... ./tests/...

test-v:
	go test -mod=vendor -v ./internal/... ./pkg/... ./tests/...

test-race:
	go test -mod=vendor -race -timeout 120s ./internal/... ./pkg/... ./tests/...

test-integration:
	go test -mod=vendor -race -timeout 120s ./tests/integration/...

bench:
	go test -mod=vendor -bench=. -benchmem -benchtime=3s ./benchmarks/...

lint:
	golangci-lint run ./...

experiment: build
	mkdir -p results
	./experiment -reps 3 -output results/results.csv

analyze:
	./analyze -input results/results.csv

clean:
	rm -rf bin/ .swarmfs/ results/ tracker peer experiment analyze
	go clean -mod=vendor ./...
