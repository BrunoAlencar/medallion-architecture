#!/usr/bin/env bash
# `make status`: check every component in data-flow order and say where to look when one fails.
# Read-only. Each ✘ line is followed by the guide section to open (docs/how-to/operate-and-debug.md).
source "$(dirname "$0")/lib.sh"
require_cluster
load_env
FAILS=0
fail() { bad "$1"; [ -n "${2:-}" ] && hint "$2"; FAILS=$((FAILS+1)); }
G="guide › Component checklist ›"

# --- one probe pod per check family, so we do not depend on tools inside product images ------
probe_http() {  # probe_http <url> -> body (empty on failure); runs inside the cluster
  kubectl -n observability run "probe-$RANDOM" --rm -i --quiet --restart=Never \
    --image=curlimages/curl:8.11.1 --command -- curl -s -m 10 "$1" 2>/dev/null || true
}
sr_sql() {  # sr_sql <sql> -> tab-separated rows
  kubectl -n data run "probe-$RANDOM" --rm -i --quiet --restart=Never --image=mysql:8.4 \
    --env="MYSQL_PWD=${STARROCKS_ROOT_PASSWORD}" --command -- \
    mysql -h starrocks-fe-service -P 9030 -uroot -N -e "$1" 2>/dev/null || true
}

info "Kubernetes"
not_ok="$(kubectl get pods -A --no-headers 2>/dev/null | awk '$1 ~ /^(kafka|data|processing|observability)$/ && $4 !~ /Running|Completed|Succeeded/ {print $1"/"$2" ("$4")"}')"
if [ -z "$not_ok" ]; then ok "all pods Running/Completed"; else
  while read -r p; do fail "pod $p" "guide › A pod keeps restarting"; done <<<"$not_ok"; fi

info "1. Object storage (SeaweedFS)"
buckets="$(kubectl -n data exec seaweedfs-0 -- sh -c 'echo s3.bucket.list | weed shell -master=localhost:9333 -filer=localhost:8888' 2>/dev/null || true)"
for b in bronze silver gold loki tempo mimir; do
  if grep -q "$b" <<<"$buckets"; then ok "bucket $b"; else fail "bucket $b missing" "$G SeaweedFS (rerun: kubectl -n data delete job seaweedfs-buckets; make up-core)"; fi
done

info "2. Source (MongoDB)"
mongo_state="$(kubectl -n data exec mongo-0 -c mongo -- mongosh --quiet --eval 'db.hello().isWritablePrimary + " " + db.getSiblingDB("app").users.estimatedDocumentCount()' 2>/dev/null || true)"
if [[ "$mongo_state" == true* ]]; then ok "rs0 PRIMARY, app.users = ${mongo_state#true }"; else fail "MongoDB not PRIMARY (${mongo_state:-no answer})" "$G MongoDB"; fi
seeder="$(kubectl -n processing get deploy seeder -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || echo '?')"
case "$seeder" in
  1/1) ok "seeder running" ;;
  /0|0/0) warn "seeder paused (make seed to resume)" ;;
  *) fail "seeder not ready ($seeder)" "$G Seeder" ;;
esac

info "3. Kafka and CDC"
for kind_name in kafka/medallion kafkaconnect/medallion-connect kafkaconnector/mongo-source kafkaconnector/bronze-s3-sink; do
  ready="$(kubectl -n kafka get "$kind_name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  if [ "$ready" = "True" ]; then ok "$kind_name Ready"; else
    msg="$(kubectl -n kafka get "$kind_name" -o jsonpath='{.status.conditions[0].message}' 2>/dev/null | cut -c1-160)"
    fail "$kind_name not Ready ${msg:+($msg)}" "guide › A connector is not running"; fi
done
for c in mongo-source bronze-s3-sink; do
  st="$(probe_http "http://medallion-connect-connect-api.kafka.svc.cluster.local:8083/connectors/$c/status")"
  if grep -q '"state":"FAILED"' <<<"$st"; then fail "$c has a FAILED task" "guide › A connector is not running (the trace is in the status JSON)"
  elif grep -q '"state":"RUNNING"' <<<"$st"; then ok "$c RUNNING (Connect REST)"
  else warn "$c: could not read Connect REST status"; fi
done
offsets="$(kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic cdc.app.users 2>/dev/null | tr '\n' ' ')"
[ -n "$offsets" ] && ok "cdc.app.users end offsets: $offsets" || fail "cannot read topic offsets" "$G Kafka"
lag="$(kubectl -n kafka exec medallion-dual-role-0 -c kafka -- bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group connect-bronze-s3-sink 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
ok "bronze sink consumer lag: ${lag:-?} messages"

info "4. Bronze"
# Newest dt=/hr= folder and how many Parquet files it holds, via the filer's JSON listing.
newest="$(kubectl -n observability run "probe-$RANDOM" --rm -i --quiet --restart=Never --image=curlimages/curl:8.11.1 --command -- sh -c '
B=http://seaweedfs.data.svc.cluster.local:8888/buckets/bronze/topics/cdc.app.users
J="Accept: application/json"
dt=$(curl -s -H "$J" "$B/?limit=10000" | grep -o "dt=[0-9-]*" | sort -u | tail -1)
[ -z "$dt" ] && exit 0
hr=$(curl -s -H "$J" "$B/$dt/?limit=10000" | grep -o "hr=[0-9]*" | sort -u | tail -1)
n=$(curl -s -H "$J" "$B/$dt/$hr/?limit=10000" | grep -o "\.parquet\"" | wc -l)
echo "$dt/$hr: $n files"' 2>/dev/null || true)"
if [ -n "$newest" ]; then ok "bronze newest folder $newest"; else fail "no bronze files yet" "guide › Bronze is not getting new files"; fi

info "5. Silver catalog (Lakekeeper)"
health="$(probe_http http://lakekeeper.data.svc.cluster.local:8181/health)"
[ -n "$health" ] && ok "Lakekeeper healthy" || fail "Lakekeeper not answering /health" "$G Lakekeeper"
wh="$(probe_http http://lakekeeper.data.svc.cluster.local:8181/management/v1/warehouse)"
grep -q '"medallion"' <<<"$wh" && ok "warehouse medallion exists" || fail "warehouse medallion missing" "$G Lakekeeper (rerun the lakekeeper-warehouse Job)"

info "6. Gold (StarRocks)"
cn="$(sr_sql 'SHOW COMPUTE NODES' | awk '{for(i=1;i<=NF;i++) if($i=="true"){print "alive"; exit}}')"
[ "$cn" = "alive" ] && ok "StarRocks FE answers, CN alive" || fail "StarRocks FE/CN not ready" "$G StarRocks"
gold="$(sr_sql 'SELECT COUNT(*), MAX(built_at) FROM gold.users_by_country')"
if [ -n "$gold" ] && [ "$(awk '{print $1}' <<<"$gold")" != "0" ]; then ok "gold.users_by_country rows / built_at: $gold"; else warn "gold is empty: run make run-pipeline"; fi
last="$(sr_sql "SELECT run_id, status, started_at, IFNULL(failed_checks,''), LEFT(IFNULL(error,''),120) FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 1")"
if [ -z "$last" ]; then warn "no Pipeline run recorded yet"
elif grep -q success <<<"$last"; then ok "last run: $last"
else fail "last run: $last" "guide › A Pipeline run failed"; fi

info "7. Pipeline (Spark Operator)"
kubectl -n processing get sparkapplications --sort-by=.metadata.creationTimestamp 2>/dev/null | tail -n 4 | sed 's/^/    /' || warn "no SparkApplications"
sched="$(kubectl -n processing get scheduledsparkapplication medallion-pipeline-scheduled -o jsonpath='{.spec.suspend}' 2>/dev/null || echo missing)"
[ "$sched" = "false" ] && ok "schedule ON (every 10 min)" || ok "schedule off (make schedule-on to enable)"

info "8. Observability"
if kubectl get ns observability >/dev/null 2>&1 && kubectl -n observability get sts mimir >/dev/null 2>&1; then
  for u in "mimir:9009/ready" "loki:3100/ready" "tempo:3200/ready" "grafana:80/api/health"; do
    body="$(probe_http "http://${u%%:*}.observability.svc.cluster.local:${u#*:}")"
    if grep -qiE 'ready|ok|"database"' <<<"$body"; then ok "${u%%:*} ready"; else fail "${u%%:*} not ready (${body:0:80})" "$G ${u%%:*}"; fi
  done
  rules="$(probe_http http://mimir.observability.svc.cluster.local:9009/prometheus/api/v1/rules)"
  n="$(grep -o '"name":"[A-Za-z]*"' <<<"$rules" | wc -l | tr -d ' ')"
  [ "${n:-0}" -gt 0 ] && ok "Mimir ruler loaded alert rules (${n} names incl. groups)" || fail "no alert rules loaded" "$G Mimir (make alerts)"
  up="$(probe_http 'http://mimir.observability.svc.cluster.local:9009/prometheus/api/v1/query?query=up%7Bcomponent!%3D%22%22%7D%3D%3D0')"
  if grep -q '"result":\[\]' <<<"$up"; then ok "every scraped component is up"; else
    down="$(grep -o '"component":"[^"]*"' <<<"$up" | sort -u | tr '\n' ' ')"
    fail "scrape targets down: ${down:-see Grafana › Platform health}" "$G <that component>"; fi
else
  warn "observability not installed (make up installs it; make up-core does not)"
fi

echo
if [ "$FAILS" -eq 0 ]; then echo "${C_OK}${C_B}All checks passed.${C_0}"; else echo "${C_BAD}${C_B}${FAILS} check(s) failed.${C_0} Open docs/how-to/operate-and-debug.md at the section named above."; exit 1; fi
