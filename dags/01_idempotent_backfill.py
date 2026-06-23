"""Pattern: Idempotent, date-partitioned backfill.

The golden rule of backfills: re-running any day must produce the same result.
Achieve this by deriving everything from the logical date (the data interval),
never from `datetime.now()`, and by making writes overwrite-by-partition.

Run a single day:
    airflow dags backfill -s 2024-01-01 -e 2024-01-01 idempotent_backfill
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,  # flip to True to backfill history
    tags=["pattern", "backfill"],
)
def idempotent_backfill():

    @task
    def extract(ds: str) -> str:
        # `ds` is the logical date (YYYY-MM-DD) for THIS run, not "today".
        # All inputs must be scoped to this partition.
        print(f"Extracting data for partition {ds}")
        return ds

    @task
    def transform(partition: str) -> dict:
        return {"partition": partition, "rows": 42}

    @task
    def load(payload: dict) -> None:
        # Idempotent write: delete-then-insert (or overwrite) the partition,
        # so re-running the same day never double-counts.
        print(f"DELETE FROM table WHERE ds = '{payload['partition']}'")
        print(f"INSERT {payload['rows']} rows for {payload['partition']}")

    load(transform(extract()))


idempotent_backfill()
