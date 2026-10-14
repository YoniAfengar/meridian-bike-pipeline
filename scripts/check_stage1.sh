#!/usr/bin/env bash
set -euo pipefail

echo "== Stage 1 acceptance checks =="

echo "== Reset stack =="
just down
just up

echo "== Bronze: JC 2019-06 =="
just run ingest-to-bronze trips:jc 2019-06
jc_2019_bronze="$(just inspect bronze trips:jc 2019-06)"
echo "$jc_2019_bronze"
grep -q '"rows":39430' <<<"$jc_2019_bronze"

echo "== Bronze + Silver: JC 2026-06 =="
just run ingest-to-bronze trips:jc 2026-06

jc_2026_bronze="$(just inspect bronze trips:jc 2026-06)"
echo "$jc_2026_bronze"
grep -q '"rows":109897' <<<"$jc_2026_bronze"

just run transform-to-silver trips:jc 2026-06

jc_2026_silver="$(just inspect silver trips:jc 2026-06)"
echo "$jc_2026_silver"
grep -q '"rows":109510' <<<"$jc_2026_silver"
grep -q '"rejects":387' <<<"$jc_2026_silver"

echo "== Schema conformance: JC 2020-06 =="
just run ingest-to-bronze trips:jc 2020-06
just run transform-to-silver trips:jc 2020-06

jc_2020_silver="$(just inspect silver trips:jc 2020-06)"
echo "$jc_2020_silver"

rows="$(printf '%s\n' "$jc_2020_silver" | sed -n 's/.*"rows":\([0-9][0-9]*\).*/\1/p')"

if [[ -z "$rows" || "$rows" -le 0 ]]; then
    echo "JC 2020-06 schema conformance failed" >&2
    exit 1
fi

echo "== Gold answer =="
just run transform-to-gold station-daily 2026-06-02

gold="$(just report daily-station-trips jc JC115 2026-06-02)"
echo "$gold"
grep -q '"departures":216' <<<"$gold"
grep -q '"arrivals":209' <<<"$gold"

echo "== Idempotent rerun =="
before="$gold"

just run transform-to-silver trips:jc 2026-06
just run transform-to-gold station-daily 2026-06-02

after="$(just report daily-station-trips jc JC115 2026-06-02)"
echo "$after"

if [[ "$before" != "$after" ]]; then
    echo "rerun changed the gold answer" >&2
    exit 1
fi

echo "== Wrong grain refused =="

if just run ingest-to-bronze trips:jc 2026-06-02; then
    echo "ingest accepted a day instead of a month" >&2
    exit 1
fi

if just run transform-to-gold station-daily 2026-06; then
    echo "gold accepted a month instead of a day" >&2
    exit 1
fi

echo "== Stage 1 checks passed =="