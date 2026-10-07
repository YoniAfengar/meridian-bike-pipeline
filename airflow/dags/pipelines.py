from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.standard.sensors.python import PythonSensor

from src.config import EARLIEST_WINDOWS
from src.pipeline import (
    bronze_task,
    gold_task,
    select_month,
    silver_task,
    source_published,
    work_due,
)
from src.timetable import MarketTimetable


def build_dag(market, earliest):
    with DAG(
        dag_id=f"pipeline_{market}",
        start_date=pendulum.parse(f"{earliest}-01", tz="UTC"),
        schedule=MarketTimetable(market),
        catchup=True,
        is_paused_upon_creation=False,
        max_active_runs=16,
        max_active_tasks=32,
        render_template_as_native_obj=True,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(seconds=30),
        },
        tags=["meridian", market, "stage-2"],
    ) as dag:
        month = PythonOperator(
            task_id="which_month",
            python_callable=select_month,
            op_kwargs={"market": market},
            retries=0,
        )

        arguments = {
            "market": market,
            "window": month.output,
        }

        due = ShortCircuitOperator(
            task_id="is_due",
            python_callable=work_due,
            op_kwargs=arguments.copy(),
            retries=0,
        )

        published = PythonSensor(
            task_id="wait_until_published",
            python_callable=source_published,
            op_kwargs=arguments.copy(),
            mode="reschedule",
            poke_interval=300,
            timeout=7 * 24 * 60 * 60,
            retries=0,
        )

        bronze = PythonOperator(
            task_id="ingest_to_bronze",
            python_callable=bronze_task,
            op_kwargs=arguments.copy(),
            execution_timeout=timedelta(hours=2),
        )

        silver = PythonOperator(
            task_id="transform_to_silver",
            python_callable=silver_task,
            op_kwargs=arguments.copy(),
            execution_timeout=timedelta(hours=2),
        )

        gold = PythonOperator(
            task_id="transform_to_gold",
            python_callable=gold_task,
            op_kwargs=arguments.copy(),
            execution_timeout=timedelta(hours=2),
        )

        month >> due >> published >> bronze >> silver >> gold

    return dag


for market, earliest in EARLIEST_WINDOWS.items():
    globals()[f"pipeline_{market}"] = build_dag(market, earliest)
