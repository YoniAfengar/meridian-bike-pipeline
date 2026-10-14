#!/usr/bin/env bash
set -euo pipefail

echo "== Stage 2 acceptance checks =="

echo "-- fresh stack"
just down
just up

echo "-- initial coverage"
coverage="$(just inspect coverage trips:jc)"

python3 - "$coverage" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["earliest"] == "2021-01", value
assert value["watermark"] is None, value
assert value["complete"] == 0, value
assert value["gaps"] == [], value
assert value["next"] == "2021-01", value
PY


echo "-- hand loads are not operational progress"

just run ingest-to-bronze trips:jc 2021-01
just run transform-to-silver trips:jc 2021-01

for day in $(python3 - <<'PY'
import calendar

for day in range(1, calendar.monthrange(2021, 1)[1] + 1):
    print(f"2021-01-{day:02d}")
PY
); do
    just run transform-to-gold station-daily "$day"
done

coverage="$(just inspect coverage trips:jc)"

python3 - "$coverage" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["complete"] == 0, value
assert value["watermark"] is None, value
assert value["next"] == "2021-01", value
PY


echo "-- windowless run takes earliest due month"

run="$(just run pipeline jc)"

python3 - "$run" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["month"] == "2021-01", value
assert value["type"] == "manual", value
assert value["state"] == "success", value

gold = value["tasks"]["transform-to-gold station-daily"]

assert gold["state"] == "success", gold
assert gold["days"] == 31, gold
assert gold["failed_days"] == [], gold
PY


echo "-- coverage advances after operational run"

coverage="$(just inspect coverage trips:jc)"

python3 - "$coverage" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["watermark"] == "2021-01", value
assert value["complete"] == 1, value
assert value["gaps"] == [], value
assert value["next"] == "2021-02", value
PY


echo "-- backfill crosses schema change"

backfill="$(just run pipeline jc 2021-02 2021-04)"

python3 - "$backfill" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["market"] == "jc", value

runs = value["runs"]
assert [run["month"] for run in runs] == [
    "2021-04",
    "2021-03",
    "2021-02",
], runs

for run in runs:
    assert run["type"] == "backfill", run
    assert run["state"] == "success", run

expected_days = {
    "2021-02": 28,
    "2021-03": 31,
    "2021-04": 30,
}

for run in runs:
    gold = run["tasks"]["transform-to-gold station-daily"]
    assert gold["days"] == expected_days[run["month"]], gold
    assert gold["failed_days"] == [], gold
PY


echo "-- contiguous coverage advances"

coverage="$(just inspect coverage trips:jc)"

python3 - "$coverage" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])

assert value["watermark"] == "2021-04", value
assert value["complete"] == 4, value
assert value["gaps"] == [], value
assert value["next"] == "2021-05", value
PY


echo "-- named rerun is idempotent"

before="$(just inspect coverage trips:jc)"
rerun="$(just run pipeline jc 2021-02)"
after="$(just inspect coverage trips:jc)"

python3 - "$before" "$rerun" "$after" <<'PY'
import json
import sys

before = json.loads(sys.argv[1])
rerun = json.loads(sys.argv[2])
after = json.loads(sys.argv[3])

assert rerun["month"] == "2021-02", rerun
assert rerun["state"] == "success", rerun
assert before == after, (before, after)
PY


echo "== Stage 2 acceptance checks passed =="