.PHONY: test build demo
test:
	go test ./...
build:
	CGO_ENABLED=0 go build -o bin/ ./cmd/...
demo: build
	./scripts/demo.sh
