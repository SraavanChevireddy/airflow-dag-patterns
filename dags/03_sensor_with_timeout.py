"""Pattern: Sensors that wait without hogging a worker slot.

Naive sensors sit in `poke` mode and occupy a worker the whole time they wait,
which deadlocks your pool under load. Two fixes:

1. mode="reschedule" — frees the slot between checks.
2. deferrable sensors — hand off to the triggerer (best, near-zero footprint).

Always set a `timeout` so a missing upstream fails the run instead of hanging
forever.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pattern", "sensors"],
)
def sensor_with_timeout():

    def _upstream_ready() -> bool:
        # Replace with a real check (file exists, partition present, API ready).
        return True

    wait_for_upstream = PythonSensor(
        task_id="wait_for_upstream",
        python_callable=_upstream_ready,
        mode="reschedule",      # release the worker slot between pokes
        poke_interval=60,       # check once a minute
        timeout=60 * 60 * 2,    # give up after 2 hours and fail the run
        exponential_backoff=True,
    )

    @task
    def proceed() -> None:
        print("Upstream is ready — running downstream work.")

    wait_for_upstream >> proceed()


sensor_with_timeout()
