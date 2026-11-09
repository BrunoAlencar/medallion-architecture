#!/usr/bin/env bash
# Port-forward every UI to localhost. Ctrl-C stops them all.
source "$(dirname "$0")/lib.sh"
require_cluster
load_env

pids=()
forward() {  # forward <namespace> <resource> <local:remote> <label> <url>
  kubectl -n "$1" port-forward "$2" "$3" >/dev/null 2>&1 &
  pids+=($!)
  printf "  %-28s %s\n" "$4" "$5"
}
trap 'kill "${pids[@]}" 2>/dev/null; exit 0' INT TERM

info "UIs (Ctrl-C to stop)"
forward observability svc/grafana            3000:80   "Grafana"                 "http://localhost:3000  (${GRAFANA_ADMIN_USER} / ${GRAFANA_ADMIN_PASSWORD}; anonymous = viewer)"
forward kafka         svc/kafka-ui           8081:8080 "Kafka UI (topics, Connect)" "http://localhost:8081"
forward data          svc/lakekeeper         8181:8181 "Lakekeeper (Iceberg) UI"  "http://localhost:8181/ui"
forward data          svc/starrocks-fe-service 8030:8030 "StarRocks FE web"       "http://localhost:8030  (root / ${STARROCKS_ROOT_PASSWORD})"
forward data          svc/starrocks-fe-service 9030:9030 "StarRocks SQL (MySQL)"  "mysql -h127.0.0.1 -P9030 -uroot -p${STARROCKS_ROOT_PASSWORD}"
forward data          svc/rustfs             9001:9001 "RustFS console (browse)"  "http://localhost:9001  (${S3_ACCESS_KEY} / ${S3_SECRET_KEY})"
# The Lakekeeper UI lists a table's files from the browser, signing S3 requests with vended
# credentials against the warehouse endpoint http://rustfs.data.svc.cluster.local:9000.
forward data          svc/rustfs             9000:9000 "RustFS S3"                "http://localhost:9000  (Lakekeeper UI file listing)"
forward observability svc/mimir              9009:9009 "Mimir (rules, ready)"     "http://localhost:9009/prometheus/api/v1/rules"
forward kafka         svc/medallion-connect-connect-api 8083:8083 "Kafka Connect REST" "http://localhost:8083/connectors?expand=status"
if ! grep -qE '^[^#]*[[:space:]]rustfs\.data\.svc\.cluster\.local([[:space:]]|$)' /etc/hosts; then
  echo
  warn "Lakekeeper UI file listing needs this host name to reach the forward above. Once:"
  echo "    echo '127.0.0.1 rustfs.data.svc.cluster.local' | sudo tee -a /etc/hosts"
fi
echo
echo "  Spark UI exists only while a run is active:"
echo "    kubectl -n processing port-forward \$(kubectl -n processing get pod -l spark-role=driver -o name | tail -1) 4040:4040"
wait
