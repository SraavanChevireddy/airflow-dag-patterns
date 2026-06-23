"""Pattern: Sensible retries, alerting, and SLAs.

Defaults that save you at 3am:
- retries with exponential backoff and a cap, so transient blips self-heal.
- execution_timeout, so a stuck task fails instead of running forever.
- on_failure_callback for alerting (wire to Slack/PagerDuty).
- sla to flag tasks that are "succeeding but too slow".
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


def alert_on_failure(context) -> None:
    ti = context["task_instance"]
    # Wire this to Slack / PagerDuty / email in real deployments.
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed on {context['ds']}")


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": alert_on_failure,
    "sla": timedelta(minutes=30),
}


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["pattern", "reliability"],
)
def retries_and_sla():

    @task
    def flaky_step() -> str:
        # Transient failures here are retried automatically with backoff.
        return "ok"

    @task
    def downstream(value: str) -> None:
        print(f"Got: {value}")

    downstream(flaky_step())


retries_and_sla()
