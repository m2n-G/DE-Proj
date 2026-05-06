"""Airflow DAGs for the KRX stock data pipeline."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import time, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

import pendulum
import yaml
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.time_sensor import TimeSensor


logger = logging.getLogger(__name__)

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
REPO_ROOT = Path(__file__).resolve().parents[1]

PROJECT_CONFIG_DIR = AIRFLOW_HOME / "project_config"
if PROJECT_CONFIG_DIR.exists():
    sys.path.insert(0, str(AIRFLOW_HOME))


def _candidate_config_paths() -> list[Path]:
    configured_path = os.environ.get("KRX_DAG_CONFIG_PATH")
    if configured_path:
        return [Path(configured_path)]

    return [
        AIRFLOW_HOME / "config" / "mwaa_dag_config.yaml",
        PROJECT_CONFIG_DIR / "mwaa_dag_config.yaml",
        REPO_ROOT / "infra" / "airflow" / "mwaa_dag_config.yaml",
        REPO_ROOT / "config" / "mwaa_dag_config.yaml",
    ]


def _resolve_config_path() -> Path:
    candidates = _candidate_config_paths()

    for path in candidates:
        if path.exists():
            return path

    searched_paths = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "KRX DAG config file was not found. "
        f"Checked paths: {searched_paths}"
    )


def _load_yaml_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"DAG config must be a YAML mapping: {path}")

    return config


def _import_project_attr(dotted_path: str) -> Any:
    module_name, attr_name = dotted_path.rsplit(".", 1)

    module_candidates = [module_name]
    if module_name.startswith("config."):
        module_candidates.append(module_name.replace("config.", "project_config.", 1))

    last_error = None
    for candidate in module_candidates:
        try:
            module = import_module(candidate)
            return getattr(module, attr_name)
        except (ImportError, AttributeError) as exc:
            last_error = exc

    raise ImportError(f"Cannot import {dotted_path}") from last_error


def _load_watchlist(config: dict[str, Any]) -> dict[str, str]:
    source = config["market"]["watchlist_source"]
    dotted_path = f"{source['module']}.{source['attribute']}"
    return _import_project_attr(dotted_path)


def _check_market_day(callable_path: str, timezone_name: str, **context: Any) -> None:
    logical_date = context["logical_date"].in_timezone(timezone_name)
    market_date = logical_date.date()
    is_market_day = _import_project_attr(callable_path)

    if not is_market_day(market_date):
        raise AirflowSkipException(f"{market_date} is not a market day.")

    logger.info("%s is a market day.", market_date)


def _build_default_args(config: dict[str, Any]) -> dict[str, Any]:
    args = dict(config["airflow"]["default_args"])
    retry_delay_minutes = args.pop("retry_delay_minutes")
    execution_timeout_minutes = args.pop("execution_timeout_minutes")
    args["retry_delay"] = timedelta(minutes=retry_delay_minutes)
    args["execution_timeout"] = timedelta(minutes=execution_timeout_minutes)
    return args


def _parse_time(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(int(hour), int(minute))


def _render_payload(
    payload: dict[str, Any],
    dag_key: str,
    task_id: str,
    aws_config: dict[str, Any],
) -> str:
    enriched_payload = {
        **payload,
        "dag_key": dag_key,
        "task_id": task_id,
        "s3_bucket": aws_config["s3_bucket"],
        "kinesis_stream": aws_config["kinesis_stream"],
    }
    return json.dumps(enriched_payload)


def _make_task(
    dag: DAG,
    dag_key: str,
    task_config: dict[str, Any],
    global_config: dict[str, Any],
    watchlist: dict[str, str],
) -> Any:
    task_id = task_config["task_id"]
    operator = task_config["operator"]
    aws_conn_id = global_config["variables"]["aws_conn_id"]

    if operator == "python":
        return PythonOperator(
            task_id=task_id,
            python_callable=_check_market_day,
            op_kwargs={
                "callable_path": task_config["callable"],
                "timezone_name": global_config["market"]["timezone"],
            },
            retries=task_config.get("retries"),
            dag=dag,
        )

    if operator == "time_sensor":
        return TimeSensor(
            task_id=task_id,
            target_time=_parse_time(task_config["target_time"]),
            mode="reschedule",
            dag=dag,
        )

    if operator == "lambda":
        return LambdaInvokeFunctionOperator(
            task_id=task_id,
            function_name=task_config["function_name"],
            payload=_render_payload(
                task_config.get("payload", {}),
                dag_key,
                task_id,
                global_config["aws"],
            ),
            aws_conn_id=aws_conn_id,
            invocation_type="RequestResponse",
            dag=dag,
        )

    if operator == "s3_prefix_sensor":
        prefix_template = task_config["prefix_template"]
        if task_config.get("expand_over") == "watchlist":
            sensors = []
            for stock_code in sorted(watchlist):
                bucket_key = prefix_template.replace("{{ stock_code }}", stock_code)
                sensors.append(
                    S3KeySensor(
                        task_id=f"{task_id}_{stock_code}",
                        bucket_name=task_config["bucket"],
                        bucket_key=f"{bucket_key}*",
                        wildcard_match=True,
                        timeout=task_config["timeout_minutes"] * 60,
                        mode="reschedule",
                        soft_fail=task_config.get("soft_fail", False),
                        aws_conn_id=aws_conn_id,
                        dag=dag,
                    )
                )
            return sensors

        wildcard_match = prefix_template.endswith("/")
        bucket_key = f"{prefix_template}*" if wildcard_match else prefix_template
        return S3KeySensor(
            task_id=task_id,
            bucket_name=task_config["bucket"],
            bucket_key=bucket_key,
            wildcard_match=wildcard_match,
            timeout=task_config["timeout_minutes"] * 60,
            mode="reschedule",
            soft_fail=task_config.get("soft_fail", False),
            aws_conn_id=aws_conn_id,
            dag=dag,
        )

    raise ValueError(f"Unsupported operator type: {operator}")


def _as_task_list(task_or_tasks: Any) -> list[Any]:
    if isinstance(task_or_tasks, list):
        return task_or_tasks
    return [task_or_tasks]


def _link_tasks(task_map: dict[str, Any], dependency: str) -> None:
    left_id, right_id = [part.strip() for part in dependency.split(">>", 1)]
    left_tasks = _as_task_list(task_map[left_id])
    right_tasks = _as_task_list(task_map[right_id])

    for left_task in left_tasks:
        for right_task in right_tasks:
            left_task >> right_task


def _create_dag(
    dag_key: str,
    dag_config: dict[str, Any],
    global_config: dict[str, Any],
    watchlist: dict[str, str],
) -> DAG:
    timezone_name = dag_config.get("timezone", global_config["airflow"]["timezone"])
    timezone = pendulum.timezone(timezone_name)
    dag_defaults = global_config["airflow"]["dag_defaults"]

    dag = DAG(
        dag_id=dag_config["dag_id"],
        description=dag_config["description"],
        schedule=dag_config["schedule"],
        start_date=pendulum.parse(dag_config["start_date"], tz=timezone),
        catchup=dag_defaults["catchup"],
        max_active_runs=dag_defaults["max_active_runs"],
        max_active_tasks=dag_defaults["max_active_tasks"],
        default_args=_build_default_args(global_config),
        tags=dag_defaults["tags"],
        params=dag_config.get("params", {}),
    )

    task_map = {}
    for task_config in dag_config["tasks"]:
        task_map[task_config["task_id"]] = _make_task(
            dag,
            dag_key,
            task_config,
            global_config,
            watchlist,
        )

    for dependency in dag_config["dependencies"]:
        _link_tasks(task_map, dependency)

    return dag


CONFIG_PATH = _resolve_config_path()
CONFIG = _load_yaml_config(CONFIG_PATH)
WATCHLIST = _load_watchlist(CONFIG)

for _dag_key, _dag_config in CONFIG["dags"].items():
    if _dag_config.get("enabled", True):
        globals()[_dag_config["dag_id"]] = _create_dag(
            _dag_key,
            _dag_config,
            CONFIG,
            WATCHLIST,
        )
