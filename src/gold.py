from src.db import connect


def transform(day):
    month = day[:7]

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM gold_station_daily WHERE day = %s",
                (day,),
            )

            cur.execute(
                """
                WITH movements AS (
                    SELECT
                        market,
                        start_station_id AS station_id,
                        COUNT(*) AS departures,
                        0::BIGINT AS arrivals
                    FROM silver_trips
                    WHERE window_key = %s
                      AND started_at::date = %s
                    GROUP BY market, start_station_id

                    UNION ALL

                    SELECT
                        market,
                        end_station_id AS station_id,
                        0::BIGINT AS departures,
                        COUNT(*) AS arrivals
                    FROM silver_trips
                    WHERE window_key = %s
                      AND ended_at::date = %s
                    GROUP BY market, end_station_id
                )
                INSERT INTO gold_station_daily (
                    market,
                    station_id,
                    day,
                    departures,
                    arrivals
                )
                SELECT
                    market,
                    station_id,
                    %s,
                    SUM(departures),
                    SUM(arrivals)
                FROM movements
                GROUP BY market, station_id
                """,
                (month, day, month, day, day),
            )


def report(market, station, day):
    import json

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT departures, arrivals
                FROM gold_station_daily
                WHERE market = %s
                  AND station_id = %s
                  AND day = %s
                """,
                (market, station, day),
            )
            row = cur.fetchone()

    departures, arrivals = row if row else (0, 0)

    print(json.dumps({
        "market": market,
        "station": station,
        "day": day,
        "departures": departures,
        "arrivals": arrivals,
    }, separators=(",", ":")))
