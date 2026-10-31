#!/usr/bin/env bash
# Restores the newest backup into a scratch database and compares row counts of the durable
# tables with the live database. Never touches the live database.
#   ENV_FILE=.env.stockholm scripts/restore_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."
compose="docker compose --env-file ${ENV_FILE:-.env.local}"
$compose exec -T backup bash /backup.sh once
latest=$($compose exec -T backup bash -c 'ls -1t /backups/polygemy-*.dump | head -1' | tr -d '\r')
echo "restoring $latest into polygemy_restore_test"
$compose exec -T backup bash -c "dropdb --if-exists polygemy_restore_test && createdb polygemy_restore_test && pg_restore --no-owner -d polygemy_restore_test '$latest'"
fail=0
for t in markets market_lifecycles chainlink_reports feature_snapshots predictions strategy_signals orders fills pnl_ledger positions model_versions system_events; do
  live=$($compose exec -T backup psql -Atc "SELECT count(*) FROM $t")
  rest=$($compose exec -T backup psql -d polygemy_restore_test -Atc "SELECT count(*) FROM $t")
  # The live count can only have grown since the dump was taken.
  if [ "$rest" -gt "$live" ]; then echo "MISMATCH $t: restored $rest > live $live"; fail=1; else echo "ok $t: restored $rest (live now $live)"; fi
done
unbalanced=$($compose exec -T backup psql -d polygemy_restore_test -Atc "SELECT count(*) FROM pnl_ledger_unbalanced")
[ "$unbalanced" = "0" ] || { echo "restored ledger is unbalanced"; fail=1; }
$compose exec -T backup dropdb polygemy_restore_test
[ $fail = 0 ] && echo "RESTORE TEST PASSED" || { echo "RESTORE TEST FAILED"; exit 1; }
