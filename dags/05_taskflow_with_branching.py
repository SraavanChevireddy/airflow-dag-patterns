"""Pattern: Conditional branching with the TaskFlow API.

Use @task.branch to choose a path at runtime. The branch function returns the
task_id(s) to follow; the others are skipped. A common gotcha: a join task
downstream of a branch will be skipped unless you set the right trigger rule.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pattern", "branching"],
)
def taskflow_with_branching():

    @task
    def get_record_count() -> int:
        return 1200

    @task.branch
    def choose_path(count: int) -> str:
        # Return the task_id to run next.
        return "big_load" if count > 1000 else "small_load"

    @task
    def big_load() -> None:
        print("Using the bulk/parallel loader")

    @task
    def small_load() -> None:
        print("Using the simple single-threaded loader")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def done() -> None:
        # Without the trigger rule this join is skipped because one branch was.
        print("Load complete")

    count = get_record_count()
    branch = choose_path(count)
    big = big_load()
    small = small_load()
    branch >> [big, small] >> done()


taskflow_with_branching()
