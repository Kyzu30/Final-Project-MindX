"""Daily airport departure pipeline, with selectable CSV input for manual runs."""

from datetime import timedelta
from functools import partial

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from airport_etl import storage

STEPS = ("ingest", "clean", "load", "check")


def run_step(name, operation):
    context = get_current_context()
    storage.execute_step(name, operation, context["run_id"], context["ti"].try_number)


@dag(
    dag_id="airport_flight_departure_etl",
    description="Load flight observations into MySQL and verify departure-delay metrics",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    params={"source_file": Param("flights.csv", type="string", description="CSV filename in data/")},
    tags=["airport", "etl", "mysql"],
)
def airport_flight_departure_etl():
    @task(do_xcom_push=False)
    def ingest():
        source_file = get_current_context()["params"]["source_file"]
        run_step("ingest", partial(storage.ingest, source_file=source_file))

    @task(do_xcom_push=False)
    def clean():
        run_step("clean", storage.clean)

    @task(do_xcom_push=False)
    def load():
        run_step("load", storage.load)

    @task(do_xcom_push=False)
    def check():
        run_step("check", storage.check)

    @task(trigger_rule=TriggerRule.ALL_DONE, do_xcom_push=False)
    def finalize():
        context = get_current_context()
        task_states = {
            ti.task_id: ti.state
            for ti in context["dag_run"].get_task_instances()
            if ti.task_id in STEPS
        }
        succeeded = all(task_states.get(step) == TaskInstanceState.SUCCESS for step in STEPS)
        outcome = "SUCCESS" if succeeded else "FAILED"
        message = "All ETL stages succeeded" if succeeded else "One or more ETL stages did not succeed"
        with storage.transaction() as cursor:
            storage.write_log(
                cursor, context["run_id"], "pipeline", context["ti"].try_number,
                outcome, message, {"task_states": task_states},
            )
        if not succeeded:
            raise AirflowFailException(f"{message}: {task_states}")

    ingest() >> clean() >> load() >> check() >> finalize()


airport_dag = airport_flight_departure_etl()
