#!/usr/bin/env bash
# Run the Pipeline once as a SparkApplication and wait for it.
#   scripts/run-pipeline.sh                     # bronze_to_silver -> checks -> silver_to_gold
#   scripts/run-pipeline.sh bronze_to_silver    # any comma-separated subset, or verify|fingerprint|reset-hwm
source "$(dirname "$0")/lib.sh"
require_cluster
spark_run "${1:-pipeline}" "${RUN_ID:-}"
