"""Pattern: Dynamic task mapping (fan-out / fan-in).

Instead of hardcoding N parallel tasks, generate them at runtime from a list.
Airflow creates one mapped task instance per element, runs them in parallel,
then a downstream task can reduce over all results.

This is the modern replacement for building tasks in a Python for-loop, which
broke the UI and scheduler.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pattern", "dynamic-mapping"],
)
def dynamic_task_mapping():

    @task
    def list_files() -> list[str]:
        # In real life this might list an S3 prefix or query a manifest.
        return [f"file_{i}.csv" for i in range(5)]

    @task
    def process(file_name: str) -> int:
        print(f"Processing {file_name}")
        return len(file_name)

    @task
    def summarize(sizes: list[int]) -> None:
        # Fan-in: receives the list of all mapped results.
        print(f"Processed {len(sizes)} files, total size metric = {sum(sizes)}")

    # `.expand()` fans out one `process` task per file.
    sizes = process.expand(file_name=list_files())
    summarize(sizes)


dynamic_task_mapping()
