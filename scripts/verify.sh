#!/usr/bin/env bash
# PLAN verification step 5: correctness against the source.
# Pauses the seeder, lets the sink flush, runs the Pipeline, then compares MongoDB with silver
# (count + sample of documents) and gold with silver. Leaves the seeder paused (`make seed` resumes).
source "$(dirname "$0")/lib.sh"
require_cluster
info "pausing the seeder so MongoDB stops changing"
seeder_pause
sleep 20   # Debezium + one sink flush interval
wait_sink_caught_up
info "catch-up Pipeline run"
spark_run pipeline
info "verify"
if spark_run verify; then
  ok "silver matches MongoDB and gold adds up to silver"
else
  bad "verification failed: see MISMATCH lines above"
  hint "docs/how-to/operate-and-debug.md › Silver does not match MongoDB"
  exit 1
fi
