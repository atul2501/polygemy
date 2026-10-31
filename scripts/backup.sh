#!/usr/bin/env bash
# Logical backup of the durable data. Raw partitions (exchange_events*, orderbook_snapshots*)
# are excluded: they are dropped after RAW_RETENTION_HOURS by design and dominate the size.
#   backup.sh once        one dump now
#   backup.sh loop        one dump per day (the compose `backup` service)
#   backup.sh list
set -euo pipefail
dir=/backups
keep=${BACKUP_KEEP_DAYS:-7}

dump() {
  local f="$dir/polygemy-$(date -u +%Y%m%dT%H%M%SZ).dump"
  pg_dump -Fc --no-owner \
    --exclude-table-data='exchange_events*' --exclude-table-data='orderbook_snapshots*' \
    -f "$f.partial"
  mv "$f.partial" "$f"
  echo "backup written: $f ($(du -h "$f" | cut -f1))"
  find "$dir" -name 'polygemy-*.dump' -mtime +"$keep" -print -delete
}

case "${1:-once}" in
  once) dump ;;
  loop) while true; do dump || echo "BACKUP FAILED $(date -u)"; sleep 86400; done ;;
  list) ls -lh "$dir" ;;
  *) echo "usage: backup.sh once|loop|list" >&2; exit 2 ;;
esac
