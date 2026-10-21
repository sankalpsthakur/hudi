#!/usr/bin/env bash
# usage: run_downgrade_step.sh STEP NAME LABEL
# Runs TestProbeDowngradeStep once with the hudi-common that is currently installed, prints its summary and
# collects the reports under reports-out/<label>. Never fails the job: the caller reads the summaries.
set -u
step="$1"; name="$2"; label="$3"
find . -type d -name surefire-reports -prune -exec rm -rf {} +
STEP="$step" NAME="$name" mvn test -DskipTests=false -Dmaven.test.failure.ignore=true $SPARK_ARGS \
  -Dtest=skipJavaTests -DwildcardSuites=org.apache.spark.sql.hudi.probe.TestProbeDowngradeStep \
  -Dsurefire.failIfNoSpecifiedTests=false $MVN_ARGS -pl hudi-spark-datasource/hudi-spark || true
python3 .github/scripts/validate_report.py summarize --root hudi-spark-datasource/hudi-spark/target --label "$label" \
  --expect TestProbeDowngradeStep --collect "reports-out/$(echo "$label" | tr -c 'A-Za-z0-9\n' '_')" || true
