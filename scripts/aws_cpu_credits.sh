#!/usr/bin/env bash
# CPU credit balance of a burstable (t-family) instance over the last 6 hours.
#   scripts/aws_cpu_credits.sh <instance-id>     (needs the AWS CLI configured for eu-north-1)
set -euo pipefail
id=${1:?instance id}
aws cloudwatch get-metric-statistics --region eu-north-1 --namespace AWS/EC2 --metric-name CPUCreditBalance \
  --dimensions Name=InstanceId,Value="$id" --statistics Minimum Average \
  --start-time "$(date -u -d '-6 hours' +%FT%TZ 2>/dev/null || date -u -v-6H +%FT%TZ)" --end-time "$(date -u +%FT%TZ)" --period 3600 \
  --query 'Datapoints | sort_by(@, &Timestamp)[].[Timestamp, Minimum, Average]' --output table
