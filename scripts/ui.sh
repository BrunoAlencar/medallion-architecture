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
forward data          svc/seaweedfs          9333:9333 "SeaweedFS master"         "http://localhost:9333"
forward data          svc/seaweedfs          8888:8888 "SeaweedFS filer (browse)" "http://localhost:8888/buckets/"
forward observability svc/mimir              9009:9009 "Mimir (rules, ready)"     "http://localhost:9009/prometheus/api/v1/rules"
forward kafka         svc/medallion-connect-connect-api 8083:8083 "Kafka Connect REST" "http://localhost:8083/connectors?expand=status"
echo
echo "  Spark UI exists only while a run is active:"
echo "    kubectl -n processing port-forward \$(kubectl -n processing get pod -l spark-role=driver -o name | tail -1) 4040:4040"
wait
