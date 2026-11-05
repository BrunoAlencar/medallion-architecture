#!/usr/bin/env bash
# PLAN verification step 8: a New Attribute reaches silver with zero code change.
# The seeder starts writing `phone` (and nested `preferences.newsletter`) on inserts and updates;
# the next Pipeline run adds STRING columns `phone` and `preferences_newsletter` to silver.users.
source "$(dirname "$0")/lib.sh"
require_cluster
ATTRS="${NEW_ATTRIBUTES:-phone,preferences.newsletter}"

info "seeder: NEW_ATTRIBUTES=${ATTRS}"
kubectl -n processing set env deploy/seeder NEW_ATTRIBUTES="$ATTRS" NEW_ATTRIBUTE_AFTER_SECONDS=0 >/dev/null
kubectl -n processing scale deploy/seeder --replicas=1 >/dev/null
kubectl -n processing rollout status deploy/seeder --timeout=180s >/dev/null
ok "seeder restarted with New Attributes"

info "letting changes with the new fields flow for 60 s"
sleep 60
spark_run pipeline
if grep -q "New Attributes added to silver" "$SPARK_LOG"; then
  ok "$(grep -o "New Attributes added to silver.*" "$SPARK_LOG" | tail -1)"
elif grep -q "new columns none" "$SPARK_LOG"; then
  warn "no new columns this run: they probably exist already from an earlier scenario run"
else
  bad "could not confirm the new columns from the driver log"
fi
echo "  Check it: Pipeline dashboard › 'New columns (last run)' / 'Silver columns',"
echo "  or StarRocks: SELECT phone, preferences_newsletter FROM lakehouse.silver.users WHERE phone IS NOT NULL LIMIT 5;"
