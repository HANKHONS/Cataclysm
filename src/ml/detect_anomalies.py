"""Fit on historical hourly counts and score the latest completed hour only."""

from src.common.db import init_db, get_pool, close_pool
from src.ml.anomaly import train_report_rate_detector, anomaly_scores


def main() -> None:
    init_db()
    try:
        with get_pool().connection() as conn:
            rows = conn.execute("""
                WITH bounds AS (
                  SELECT date_trunc('hour', MIN(timestamp)) AS first_hour,
                         date_trunc('hour', CURRENT_TIMESTAMP) - INTERVAL '1 hour' AS last_hour
                  FROM crisis_events WHERE timestamp <= CURRENT_TIMESTAMP
                    AND source NOT LIKE 'synthetic%%' AND source NOT IN ('integration','test','operator_manual_dispatch')
                ), hours AS (
                  SELECT generate_series(GREATEST(first_hour, last_hour - INTERVAL '167 hours'),
                                         last_hour, INTERVAL '1 hour') AS hour FROM bounds
                )
                SELECT hour, COUNT(e.event_id)::integer AS count FROM hours
                LEFT JOIN crisis_events e ON e.timestamp >= hour AND e.timestamp < hour + INTERVAL '1 hour'
                  AND e.source NOT LIKE 'synthetic%%' AND e.source NOT IN ('integration','test','operator_manual_dispatch')
                GROUP BY hour ORDER BY hour
            """).fetchall()
            if len(rows) < 21:
                print(
                    "Insufficient history: need 20 completed training hours plus one scoring hour"
                )
                return
            history = [row["count"] for row in rows[:-1]]
            if len(set(history)) < 2:
                print("Insufficient variability in historical report counts")
                return
            model = train_report_rate_detector(history)
            result = anomaly_scores(model, [rows[-1]["count"]])[0]
            conn.execute(
                """
                INSERT INTO anomaly_windows(window_start,report_count,score,is_anomaly,training_windows)
                VALUES (%s,%s,%s,%s,%s) ON CONFLICT(window_start) DO UPDATE SET
                report_count=EXCLUDED.report_count,score=EXCLUDED.score,is_anomaly=EXCLUDED.is_anomaly,
                training_windows=EXCLUDED.training_windows
            """,
                (
                    rows[-1]["hour"],
                    rows[-1]["count"],
                    result["score"],
                    result["is_anomaly"],
                    len(history),
                ),
            )
            print(result)
    finally:
        close_pool()


if __name__ == "__main__":
    import argparse
    import time
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=0)
    args = parser.parse_args()
    if args.interval and args.interval < 60:
        parser.error("interval must be at least 60 seconds")
    while True:
        try:
            main()
        except Exception:
            if not args.interval:
                raise
            logging.exception("Anomaly update failed")
        if not args.interval:
            break
        time.sleep(args.interval)
