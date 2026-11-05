#!/usr/bin/env bash
# PLAN verification step 6: replays must never make silver regress.
#
# Part 1 (always): reset the High-Water Mark so the Pipeline re-reads ALL of bronze. Every old
#   Change Event is replayed at an Event Position <= the stored one, so the MERGE guard must ignore
#   it and silver's fingerprint must not change.
# Part 2 (REPLAY_SOURCE=1): reset the Debezium source offsets, so the connector re-snapshots
#   MongoDB and resends every User as op=r at *higher* Kafka offsets. Silver must still match MongoDB.
source "$(dirname "$0")/lib.sh"
require_cluster

info "pausing the seeder and catching up"
seeder_pause
sleep 20
wait_sink_caught_up
spark_run pipeline

info "fingerprint before replay"
spark_run fingerprint
before="$(fingerprint_from_log "$SPARK_LOG")"
[ -n "$before" ] || die "no FINGERPRINT in the driver log"
ok "before: $before"

info "Part 1: reset the High-Water Mark and replay all of bronze"
spark_run reset-hwm
spark_run pipeline
read_line="$(grep -o 'Change Events beyond the High-Water Mark: [0-9]*' "$SPARK_LOG" | tail -1 || true)"
echo "    ${read_line:-(no count found)}"
spark_run fingerprint
after="$(fingerprint_from_log "$SPARK_LOG")"
if [ "$before" = "$after" ]; then
  ok "silver unchanged after replaying bronze ($after)"
else
  bad "silver CHANGED after replay: before=$before after=$after"
  hint "the MERGE guard (Event Position) let an older event win: docs/how-to/operate-and-debug.md › Silver does not match MongoDB"
  exit 1
fi

if [ "${REPLAY_SOURCE:-0}" = "1" ]; then
  info "Part 2: reset the mongo-source connector offsets (Debezium re-snapshots MongoDB)"
  kubectl -n kafka patch kafkaconnector mongo-source --type merge -p '{"spec":{"state":"stopped"}}' >/dev/null
  sleep 15
  kubectl -n kafka annotate kafkaconnector mongo-source strimzi.io/connector-offsets=reset --overwrite >/dev/null
  for _ in $(seq 1 30); do
    kubectl -n kafka get kafkaconnector mongo-source -o jsonpath='{.metadata.annotations}' | grep -q connector-offsets || break
    sleep 2
  done
  kubectl -n kafka patch kafkaconnector mongo-source --type merge -p '{"spec":{"state":"running"}}' >/dev/null
  ok "offsets reset; connector running again (watch the snapshot on the CDC & Kafka dashboard)"
  sleep 60
  wait_sink_caught_up
  spark_run pipeline
fi

info "correctness against MongoDB"
spark_run verify
ok "replay test PASSED"
echo "  the seeder is still paused: \`make seed\` resumes it"
