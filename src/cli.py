import re
import sys

from src.ingest import ingest
from src.gold import transform as transform_gold, report as report_gold
from src.inspect import inspect_bronze, inspect_silver
from src.silver import transform as transform_silver


MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(2)


def parse_trips_job(job):
    if job not in ("trips:jc", "trips:nyc"):
        fail(f"invalid trips job: {job}")
    return job.split(":", 1)[1]


def require_month(window):
    if not MONTH_RE.fullmatch(window):
        fail(f"window must be an ISO month YYYY-MM: {window}")


def require_day(window):
    if not DAY_RE.fullmatch(window):
        fail(f"window must be an ISO day YYYY-MM-DD: {window}")


def run_command(args):
    if len(args) != 3:
        fail("run requires: <layer> <job> <window>")

    layer, job, window = args

    if layer == "ingest-to-bronze":
        require_month(window)
        market = parse_trips_job(job)
        ingest(market, window)
        return

    if layer == "transform-to-silver":
        require_month(window)
        market = parse_trips_job(job)
        transform_silver(market, window)
        return

    if layer == "transform-to-gold":
        require_day(window)
        if job != "station-daily":
            fail(f"invalid gold job: {job}")
        transform_gold(window)
        return

    fail(f"invalid run layer: {layer}")


def main():
    if len(sys.argv) < 2:
        fail("missing command")

    command = sys.argv[1]
    args = sys.argv[2:]

    if command == "run":
        run_command(args)
        return

    if command == "inspect":
        if len(args) != 3:
            fail("inspect requires: <layer> <job> <window>")

        layer, job, window = args

        require_month(window)
        market = parse_trips_job(job)

        if layer == "bronze":
            inspect_bronze(job, market, window)
            return

        if layer == "silver":
            inspect_silver(job, market, window)
            return

        fail(f"invalid inspect layer: {layer}")

    if command == "report":
        if len(args) != 4:
            fail("report requires: <question> <market> <station> <day>")

        question, market, station, day = args

        if question != "daily-station-trips":
            fail(f"invalid report question: {question}")

        if market not in ("jc", "nyc"):
            fail(f"invalid market: {market}")

        require_day(day)
        report_gold(market, station, day)
        return

    fail(f"unknown command: {command}")


if __name__ == "__main__":
    main()
