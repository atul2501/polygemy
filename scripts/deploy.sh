#!/usr/bin/env bash
# Deploy a tested git ref (tag or commit) to Stockholm from the PC:
#   scripts/deploy.sh <ssh-host> <git-ref>
# The server keeps a clone at ~/polygemy with its own .env.stockholm (never in git).
set -euo pipefail
host=${1:?usage: deploy.sh <ssh-host> <git-ref>}
ref=${2:?usage: deploy.sh <ssh-host> <git-ref>}
git rev-parse --verify --quiet "$ref^{commit}" >/dev/null || { echo "unknown ref $ref"; exit 2; }
git merge-base --is-ancestor "$ref" "origin/main" 2>/dev/null || echo "warning: $ref is not on origin/main"
ssh "$host" "cd ~/polygemy && git fetch --tags origin && REF='$ref' DEPLOYED_BY='$(whoami)@$(hostname)' bash scripts/server_deploy.sh"
