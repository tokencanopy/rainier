#!/usr/bin/env bash
# scripts/gce-up.sh — provision the Rainier dogfood VM: one e2-medium in GCP
# project "rainier", with Docker and Tailscale installed. Run once; safe to
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
#   - project "rainier" exists with billing enabled
#   - a Tailscale account (the auth step is interactive, at the end)
#
# Env: PROJECT, ZONE, VM, MACHINE_TYPE, DISK_SIZE all override the defaults.
set -euo pipefail

PROJECT=${PROJECT:-rainier}
ZONE=${ZONE:-us-west1-b}
VM=${VM:-rainier-1}
MACHINE_TYPE=${MACHINE_TYPE:-e2-medium}
DISK_SIZE=${DISK_SIZE:-50GB}

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

say "installing docker + tailscale on $VM (idempotent)"
# Each install is guarded by a command -v check, so re-running costs one ssh
# round trip and changes nothing. The docker group membership only takes
# effect on the next login — hence the note at the end rather than a `newgrp`
# dance inside a non-interactive shell.
gcloud compute ssh "$VM" --project "$PROJECT" --zone "$ZONE" --command '
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
  if ! command -v git >/dev/null || ! command -v go >/dev/null; then
    echo "--- installing git + go toolchain"
    sudo apt-get update -qq
    sudo apt-get install -y -qq git golang-go
  fi
  echo
  if tailscale status >/dev/null 2>&1; then
    echo "tailscale is already up: $(tailscale status --json | grep -m1 DNSName || true)"
  else
    echo "NEXT, on the VM: sudo tailscale up    (authenticate in the browser it prints)"
  fi
'

cat <<EOF

=== provisioned
Next steps are in docs/deploy-gce.md:
  1. gcloud compute ssh $VM --project $PROJECT --zone $ZONE
  2. sudo tailscale up            # if it isn't already
  3. clone the repo, make build, start postgres + controld + the fleet
  4. from this laptop: rainier login --from-gh --server http://$VM:9090
     (MagicDNS name over the tailnet — no public port, no LB, no cert)
Then run the acceptance checklist at the bottom of docs/deploy-gce.md.
EOF
