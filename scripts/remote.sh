#!/usr/bin/env bash
# Manage Stockholm from the PC over SSH (key-based; nothing else is exposed).
#   scripts/remote.sh <ssh-host> status|logs [service]|restart [service]|tunnel|live-check|kill "reason"|release "reason"|resources
set -euo pipefail
host=${1:?ssh host}; cmd=${2:?command}; shift 2
c="cd ~/polygemy && docker compose --env-file .env.stockholm"
case "$cmd" in
  status)     ssh "$host" "$c ps && $c exec -T engine polygemy-engine status" ;;
  logs)       ssh "$host" "$c logs --tail=200 -f ${1:-engine}" ;;
  restart)    ssh "$host" "$c restart ${1:-engine}" ;;
  live-check) ssh "$host" "$c exec -T engine polygemy-engine live-check" ;;
  resources)  ssh "$host" "free -m; df -h /; uptime; $c exec -T postgres psql -U \$USER -c 'SELECT pg_size_pretty(pg_database_size(current_database()))' 2>/dev/null || true; docker stats --no-stream" ;;
  kill|release)
    reason=${1:?a reason is required}
    engaged=$([ "$cmd" = kill ] && echo true || echo false)
    ssh "$host" "$c exec -T postgres sh -c 'psql -U \$POSTGRES_USER -d \$POSTGRES_DB -c \"UPDATE kill_switch SET engaged = $engaged, reason = \$\$$reason\$\$, changed_by = \$\$ssh:$(whoami)\$\$, changed_at = now() WHERE id = 1\"'" ;;
  tunnel)
    echo "dashboard: http://127.0.0.1:18000   postgres: 127.0.0.1:15432   (Ctrl-C to close)"
    ssh -N -L 18000:127.0.0.1:8000 -L 15432:127.0.0.1:5432 "$host" ;;
  *) echo "unknown command $cmd"; exit 2 ;;
esac
