# airflow-dag-patterns

> A cookbook of **runnable Apache Airflow DAG patterns** for the problems you actually hit in production — idempotent backfills, dynamic task mapping, sensors that don't deadlock, retries/SLAs, and branching. Modern TaskFlow API, copy-paste ready.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-017CEE.svg)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)

Airflow's docs explain individual operators well, but leave you to figure out the *patterns* yourself — and the wrong pattern (a worker-hogging sensor, a non-idempotent backfill, tasks built in a for-loop) only bites you in production. Each DAG here is a self-contained, commented example of the right way.

## Patterns

| DAG | Problem it solves |
|-----|-------------------|
| [`01_idempotent_backfill.py`](dags/01_idempotent_backfill.py) | Re-running any date produces the same result — no double-counting. |
| [`02_dynamic_task_mapping.py`](dags/02_dynamic_task_mapping.py) | Fan out N parallel tasks from a runtime list, then fan back in. |
| [`03_sensor_with_timeout.py`](dags/03_sensor_with_timeout.py) | Wait for upstream data without hogging a worker slot or hanging forever. |
| [`04_retries_and_sla.py`](dags/04_retries_and_sla.py) | Retries with backoff, execution timeouts, failure alerts, and SLAs. |
| [`05_taskflow_with_branching.py`](dags/05_taskflow_with_branching.py) | Choose a path at runtime and join correctly afterwards. |

## Try them locally

The fastest way to run these is the official Airflow Docker setup:

```bash
# 1. Grab the official compose file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# 2. Drop these DAGs in
mkdir -p ./dags && cp dags/*.py ./dags/

# 3. Boot Airflow
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080 (default login `airflow` / `airflow`) and you'll see all five DAGs ready to trigger.

Prefer no Docker? `pip install apache-airflow` then `airflow standalone`, and point `dags_folder` at this repo's `dags/`.

## Each file teaches one thing

The DAGs are intentionally small and heavily commented. Read the docstring at the top of each — it explains the trap the pattern avoids, not just the code.

## Contributing

Got a pattern you keep re-deriving (Kubernetes executor tuning, datasets/data-aware scheduling, dbt integration)? PRs welcome — keep each DAG self-contained and commented.

## License

[MIT](LICENSE) © Sraavan Chevireddy
