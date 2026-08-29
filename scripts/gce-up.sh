#!/usr/bin/env bash
# scripts/gce-up.sh — provision the Rainier dogfood VM: one e2-medium in GCP
# project "rainier-cloud", with Docker and Tailscale installed. Run once; safe to
# re-run (every step is a create-if-absent).
#
# This script stops at "the VM exists and has the two things it needs".
# Everything after it — clone, build, postgres, controld, the fleet, the
# acceptance checklist — is docs/deploy-gce.md, deliberately: those steps are
# a human's first walk through the system, and a script that hides them
# teaches nothing when one of them fails.
#
# Prereqs:
#   - gcloud installed and authenticated (`gcloud auth login`)
#   - project "rainier-cloud" exists with billing enabled
#   - a Tailscale account (the auth step is interactive, at the end)
#
# Env: PROJECT, ZONE, VM, MACHINE_TYPE, DISK_SIZE, GO_VERSION override the
# defaults.
set -euo pipefail

PROJECT=${PROJECT:-rainier-cloud}
ZONE=${ZONE:-us-west1-b}
VM=${VM:-rainier-1}
MACHINE_TYPE=${MACHINE_TYPE:-e2-medium}
DISK_SIZE=${DISK_SIZE:-50GB}
# go.mod says `go 1.25.0`, and Debian bookworm's golang-go is 1.19 — apt's Go
# cannot build this repo at all ("go.mod requires go >= 1.25.0"). Install the
# official tarball instead, which is also how you upgrade later: bump this and
# re-run.
GO_VERSION=${GO_VERSION:-1.25.0}

command -v gcloud >/dev/null || { echo "gcloud not found — install the Google Cloud CLI first" >&2; exit 2; }

say() { printf '\n=== %s\n' "$*"; }

say "project $PROJECT / zone $ZONE / vm $VM ($MACHINE_TYPE, $DISK_SIZE)"
gcloud projects describe "$PROJECT" >/dev/null 2>&1 \
  || { echo "project $PROJECT not found (or you lack access) — create it and enable billing first" >&2; exit 2; }

# Compute Engine has to be on before an instance can exist. Enabling an
# already-enabled service is a no-op, which is what keeps this re-runnable.
say "enabling compute.googleapis.com (no-op if already enabled)"
gcloud services enable compute.googleapis.com --project "$PROJECT"

if gcloud compute instances describe "$VM" --project "$PROJECT" --zone "$ZONE" >/dev/null 2>&1; then
  say "instance $VM already exists — leaving it alone"
else
  say "creating $VM"
  # No external firewall rules and no public service ports by design: every
  # port Rainier listens on is reached over the tailnet, and the tailnet is
  # not the VPC. The default egress-only posture is exactly what we want.
  gcloud compute instances create "$VM" --project "$PROJECT" --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --image-family debian-12 --image-project debian-cloud \
    --boot-disk-size "$DISK_SIZE"
fi

say "installing docker + tailscale + go $GO_VERSION on $VM (idempotent)"
# Each install is guarded by a version/command check, so re-running costs one
# ssh round trip and changes nothing. The docker group membership only takes
# effect on the next login — hence the note at the end rather than a `newgrp`
# dance inside a non-interactive shell.
#
# Single-quoted heredoc: this whole block is remote shell, so $USER, $(…) and
# friends must reach the VM unexpanded. GO_VERSION is the one local value it
# needs, so it is passed in as an env assignment on the remote command line.
gcloud compute ssh "$VM" --project "$PROJECT" --zone "$ZONE" --command "GO_VERSION=$GO_VERSION bash -s" <<'REMOTE'
  set -e
  if ! command -v docker >/dev/null; then
    echo "--- installing docker"
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
  else
    echo "--- docker already installed: $(docker --version)"
  fi
  if ! command -v tailscale >/dev/null; then
    echo "--- installing tailscale"
    curl -fsSL https://tailscale.com/install.sh | sh
  else
    echo "--- tailscale already installed: $(tailscale version | head -1)"
  fi
  if ! command -v git >/dev/null; then
    echo "--- installing git"
    sudo apt-get update -qq
    sudo apt-get install -y -qq git
  fi

  # Go from the official tarball, NOT apt: Debian bookworm ships golang-go
  # 1.19 and go.mod requires 1.25.0, so `make build` would hard-fail at
  # runbook step 2 with "go.mod requires go >= 1.25.0". /usr/local/go is
  # exactly where go.dev/doc/install puts it, so PATH advice everywhere else
  # applies unchanged.
  if [ "$(/usr/local/go/bin/go version 2>/dev/null | awk '{print $3}')" = "go${GO_VERSION}" ]; then
    echo "--- go already installed: $(/usr/local/go/bin/go version)"
  else
    ARCH=$(dpkg --print-architecture)   # amd64 on e2-medium, arm64 on t2a
    echo "--- installing go ${GO_VERSION} (linux-${ARCH}) into /usr/local/go"
    curl -fsSL -o /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz"
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tgz
    rm -f /tmp/go.tgz
    echo "$(/usr/local/go/bin/go version) installed"
  fi
  # On PATH for every future login shell (and idempotent: one grep guard).
  if ! grep -qs '/usr/local/go/bin' ~/.profile; then
    echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
    echo "--- added /usr/local/go/bin to ~/.profile (takes effect next login)"
  fi

  echo
  if tailscale status >/dev/null 2>&1; then
    echo "tailscale is already up: $(tailscale status --json | grep -m1 DNSName || true)"
  else
    echo "NEXT, on the VM: sudo tailscale up    (authenticate in the browser it prints)"
  fi
REMOTE

cat <<EOF

=== provisioned
Next steps are in docs/deploy-gce.md:
  1. gcloud compute ssh $VM --project $PROJECT --zone $ZONE
     (a FRESH login: the docker group and /usr/local/go/bin in PATH both
      only take effect on the next one — 'go version' should print go$GO_VERSION)
  2. sudo tailscale up            # if it isn't already
  3. clone the repo, make build, start postgres + controld + the fleet
  4. from this laptop: rainier login --from-gh --server http://$VM:9090
     (MagicDNS name over the tailnet — no public port, no LB, no cert)
Then run the acceptance checklist at the bottom of docs/deploy-gce.md.
EOF
