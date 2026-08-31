.PHONY: test build demo e2e verify module-path
test:
	go test ./...
build:
	CGO_ENABLED=0 go build -o bin/ ./cmd/...
demo: build
	./scripts/demo.sh
# e2e: the full stack on this machine (postgres + controld + dial-mode
# runnerd) driven by the real CLI, then the R4 egress acceptance. The
# fake-driver chaos suite is plain `go test ./internal/e2e/`; this one needs
# docker and a GitHub login. Exit 3 means the CLI half was skipped for lack
# of GitHub auth — see scripts/e2e-fleet.sh.
e2e: build
	./scripts/e2e-fleet.sh
module-path:
	./scripts/check-module-path.sh

verify: module-path test build
	go vet ./...
