#!/usr/bin/env bash
# Runs on the server (called by deploy.sh). Backup, build, migrate, start, health-check; on any
# failure go back to the previous release. Production data is never overwritten by a deploy.
set -euo pipefail
cd "$(dirname "$0")/.."
export ENV_FILE=${ENV_FILE:-.env.stockholm}
set -a; . "./$ENV_FILE"; set +a
compose="docker compose --env-file $ENV_FILE"
prev=$(git rev-parse HEAD)
record() { # action status sha
  $compose exec -T postgres psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "INSERT INTO deployment_history (app_env, git_sha, version, deployed_by, action, status, notes) VALUES ('$APP_ENV', '$3', '$3', '${DEPLOYED_BY:-$(whoami)}', '$1', '$2', 'from $prev')" || true
}
up() { # sha
  export GIT_SHA=$1 IMAGE_TAG=$1
  $compose up -d --build --wait --wait-timeout 300 && $compose exec -T engine polygemy-engine check
}

echo "== pre-deploy backup"
$compose exec -T backup bash /backup.sh once
git checkout --detach "${REF:?REF required}"
new=$(git rev-parse --short HEAD)
record deploy started "$new"
echo "== deploying $new (previous $(git rev-parse --short "$prev"))"
if up "$new"; then
  record deploy succeeded "$new"
  echo "== deployed $new"
else
  record deploy failed "$new"
  echo "== deploy failed: rolling back to $prev"
  git checkout --detach "$prev"
  old=$(git rev-parse --short HEAD)
  if up "$old"; then
    record rollback succeeded "$old"
    echo "== rolled back to $old"
  else
    record rollback failed "$old"
    echo "!! ROLLBACK FAILED. If $new applied a new migration, the previous build refuses the newer schema:"
    echo "!! restore the pre-deploy backup (docs/DEPLOYMENT.md, 'Restore') or roll forward with a fix."
  fi
  exit 1
fi
