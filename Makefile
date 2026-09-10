.PHONY: test build demo e2e verify module-path protocols control session-image session-image-security-policy session-image-smoke session-image-browser-e2e session-image-verify

DOCKER ?= docker
SESSION_IMAGE ?= rainier-session:smoke

test:
	go test ./...
build:
	bash scripts/build.sh -o bin/ ./cmd/...
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

protocols:
	./scripts/check-public-protocols.sh

control:
	./scripts/check-public-control.sh

# session-image builds the image a hosted session actually runs — the same
# Dockerfile rainier-cloud's runner-artifacts workflow builds and publishes.
# It pulls a large Debian base and downloads a pinned toolchain, so the first
# build is minutes and gigabytes; later ones are layer cache.
#
# BASE_IMAGE defaults to a linux/amd64 digest because that is what the hosted
# Dedicated runners run. On an arm64 machine, pass the tag instead of building
# the amd64 image under emulation:
#
#   make session-image BUILD_ARGS='--build-arg BASE_IMAGE=node:22-bookworm'
#
# and understand that the result is then pinned by a tag, not a digest, and is
# a development convenience rather than something to publish.
session-image:
	$(DOCKER) build $(BUILD_ARGS) -t "$(SESSION_IMAGE)" .

# session-image-security-policy checks the public, test-only policy snapshot
# used by image qualification. Hosted Rainier Cloud owns and qualifies its
# runtime copy independently; core CI must not need a cross-repository token.
session-image-security-policy:
	python3 scripts/session-image-security-policy-test.py

# session-image-smoke does the part `--version` cannot: it builds, runs,
# installs and serves inside containers wearing the driver's real restrictions
# — uid 1000, read-only rootfs, noexec /tmp, no network at all. See the header
# of the script.
session-image-smoke:
	DOCKER="$(DOCKER)" ./scripts/session-image-smoke.sh "$(SESSION_IMAGE)"

# session-image-browser-e2e is the browser half, and the one step in image
# qualification that is deliberately allowed a network: it stages a sample
# project that has never been in the image, installs its locked dependencies
# from the registry, and then runs its Playwright suite twice with no network
# at all. See the header of the script.
session-image-browser-e2e:
	DOCKER="$(DOCKER)" ./scripts/session-image-browser-e2e.sh "$(SESSION_IMAGE)"

session-image-verify: session-image session-image-smoke session-image-browser-e2e

verify: module-path protocols control session-image-security-policy test build
	go vet ./...
