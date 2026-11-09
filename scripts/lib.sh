# Shared helpers for scripts/*.sh. Source it: `source "$(dirname "$0")/lib.sh"`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${CLUSTER:-medallion}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

if [ -t 1 ]; then
  C_OK=$'\033[32m'; C_BAD=$'\033[31m'; C_WARN=$'\033[33m'; C_DIM=$'\033[2m'; C_B=$'\033[1m'; C_0=$'\033[0m'
else
  C_OK=""; C_BAD=""; C_WARN=""; C_DIM=""; C_B=""; C_0=""
fi

info() { echo "${C_B}==>${C_0} $*"; }
ok()   { echo "  ${C_OK}✔${C_0} $*"; }
bad()  { echo "  ${C_BAD}✘${C_0} $*"; }
warn() { echo "  ${C_WARN}!${C_0} $*"; }
hint() { echo "    ${C_DIM}↳ $*${C_0}"; }
die()  { echo "${C_BAD}error:${C_0} $*" >&2; exit 1; }

# Load infra/dev.env into the environment (credentials used by checks).
load_env() {
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/infra/dev.env"
  set +a
}

require_cluster() {
  command -v kubectl >/dev/null || die "kubectl not found (make prereqs)"
  kubectl config current-context 2>/dev/null | grep -q "kind-${CLUSTER}" \
    || die "kubectl context is not kind-${CLUSTER} (run: kubectl config use-context kind-${CLUSTER})"
}

# The every-minute schedule (infra/k8s/spark/scheduled-pipeline.yaml) must not overlap a manual run:
# both rebuild the same gold *__staging tables, and replay-test resets the High-Water Mark. So the
# first spark_run of a script pauses the schedule for the rest of that script, waits for a scheduled
# run already in flight, and restores the schedule on exit.
SCHEDULED_APP=medallion-pipeline-scheduled
SCHEDULE_HELD=""
hold_schedule() {
  [ -z "$SCHEDULE_HELD" ] || return 0
  SCHEDULE_HELD=1
  local suspended i active
  suspended="$(kubectl -n processing get scheduledsparkapplication "$SCHEDULED_APP" -o jsonpath='{.spec.suspend}' 2>/dev/null || true)"
  [ "$suspended" = "false" ] || return 0
  kubectl -n processing patch scheduledsparkapplication "$SCHEDULED_APP" --type merge -p '{"spec":{"suspend":true}}' >/dev/null
  trap 'kubectl -n processing patch scheduledsparkapplication '"$SCHEDULED_APP"' --type merge -p "{\"spec\":{\"suspend\":false}}" >/dev/null 2>&1 && echo "    schedule resumed"' EXIT
  echo "    schedule paused while this runs (resumed on exit)"
  for i in $(seq 1 60); do
    active="$(kubectl -n processing get sparkapplications -l "sparkoperator.k8s.io/scheduled-app-name=$SCHEDULED_APP" \
      -o jsonpath='{range .items[*]}{.status.applicationState.state}{"\n"}{end}' 2>/dev/null \
      | grep -cvE '^(COMPLETED|FAILED|SUBMISSION_FAILED)$' || true)"
    [ "${active:-0}" = "0" ] && return 0
    echo "    waiting for the scheduled run in progress"; sleep 5
  done
  warn "a scheduled run is still active after 5 min; continuing"
}

# spark_run <steps> [run_id]: submit the Pipeline template, wait, print the run's key log lines.
# Sets SPARK_APP and SPARK_LOG (path to the driver log) for callers. Returns non-zero on failure.
spark_run() {
  hold_schedule
  local steps="$1" run_id="${2:-}"
  # Random suffix without a pipe: `tr </dev/urandom | head -c 4` kills tr with SIGPIPE, which
  # pipefail + set -e turn into exit 141 before anything is submitted.
  [ -n "$run_id" ] || run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-$(printf '%04x' "$RANDOM")"
  local slug; slug="$(echo "$steps" | tr ',_' '--' | tr -cd 'a-z0-9-' | cut -c1-24)"
  SPARK_APP="medallion-${slug}-$(date -u +%m%d%H%M%S)"
  SPARK_LOG="$(mktemp -t "${SPARK_APP}.XXXX").log"
  info "SparkApplication ${SPARK_APP} (steps=${steps}, run_id=${run_id})"
  sed -e "s/__NAME__/${SPARK_APP}/" -e "s/__RUN_ID__/${run_id}/g" -e "s/__STEPS__/${steps}/" \
    "$ROOT/infra/k8s/spark/pipeline-template.yaml" | kubectl apply -f - >/dev/null

  local state="" last="" deadline=$(( $(date +%s) + ${PIPELINE_TIMEOUT:-1800} ))
  while :; do
    state="$(kubectl -n processing get sparkapplication "$SPARK_APP" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)"
    if [ "$state" != "$last" ]; then echo "    state: ${state:-PENDING}"; last="$state"; fi
    case "$state" in
      COMPLETED|FAILED|SUBMISSION_FAILED|INVALIDATING|UNKNOWN) break ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || { bad "timed out waiting for ${SPARK_APP}"; break; }
    sleep 5
  done

  kubectl -n processing logs "${SPARK_APP}-driver" >"$SPARK_LOG" 2>/dev/null || true
  echo "    ${C_DIM}driver log: ${SPARK_LOG}  (kubectl -n processing logs ${SPARK_APP}-driver)${C_0}"
  grep -E "medallion\.|FINGERPRINT=|VERIFY=|MISMATCH|Traceback|Error:" "$SPARK_LOG" | grep -vE " DEBUG " | tail -n "${LOG_TAIL:-25}" | sed 's/^/    /' || true
  if [ "$state" = "COMPLETED" ]; then
    ok "${SPARK_APP} COMPLETED"
    return 0
  fi
  bad "${SPARK_APP} ended in state ${state:-?}"
  kubectl -n processing get sparkapplication "$SPARK_APP" -o jsonpath='{.status.applicationState.errorMessage}' 2>/dev/null | sed 's/^/    /' || true
  echo
  hint "docs/how-to/operate-and-debug.md › A Pipeline run failed"
  return 1
}

seeder_pause() {
  kubectl -n processing scale deploy/seeder --replicas=0 >/dev/null
  kubectl -n processing wait --for=delete pod -l app.kubernetes.io/name=seeder --timeout=120s >/dev/null 2>&1 || true
  ok "seeder paused"
}

seeder_resume() {
  kubectl -n processing scale deploy/seeder --replicas=1 >/dev/null
  ok "seeder resumed"
}

# Wait for the bronze sink to flush everything already in Kafka (consumer lag 0), so a Pipeline
# run after pausing the seeder really sees every Change Event.
wait_sink_caught_up() {
  local i lag
  for i in $(seq 1 40); do
    lag="$(kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --describe --group connect-bronze-s3-sink 2>/dev/null \
      | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
    if [ "${lag:-1}" = "0" ]; then ok "bronze sink caught up (lag 0)"; return 0; fi
    echo "    waiting for the bronze sink to flush (lag ${lag:-?})"; sleep 5
  done
  warn "sink lag did not reach 0; continuing"
}

fingerprint_from_log() { grep -o 'FINGERPRINT=[0-9a-f]*' "$1" | tail -1 | cut -d= -f2; }
