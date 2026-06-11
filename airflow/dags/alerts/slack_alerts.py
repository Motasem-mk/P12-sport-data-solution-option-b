# airflow/dags/alerts/slack_alerts.py

"""
Reusable Slack failure alert callback for Airflow DAGs.

This module sends task-level Slack alerts when an Airflow task fails.

It reports:
- DAG ID
- failed task ID
- operator type
- run ID
- logical date
- try number
- exception message
- Airflow log URL

Environment variable required:
- SLACK_PIPELINE_ALERTS_WEBHOOK_URL
"""

import json
import os
import urllib.request
from typing import Any, Dict


SLACK_WEBHOOK_ENV_VAR = "SLACK_PIPELINE_ALERTS_WEBHOOK_URL"


def _safe_string(value: Any) -> str:
    """
    Convert any value to string safely.
    """
    if value is None:
        return "N/A"
    return str(value)


def _extract_failure_context(context: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract useful task-level failure information from Airflow context.

    The task_instance object is the key object here.
    It gives us the exact failed task, not only the DAG.
    """

    task_instance = context.get("task_instance")
    task = context.get("task")
    dag = context.get("dag")
    exception = context.get("exception")

    if task_instance:
        dag_id = _safe_string(task_instance.dag_id)
        task_id = _safe_string(task_instance.task_id)
        try_number = _safe_string(task_instance.try_number)
        log_url = _safe_string(task_instance.log_url)
    else:
        dag_id = _safe_string(getattr(dag, "dag_id", "unknown_dag"))
        task_id = _safe_string(getattr(task, "task_id", "unknown_task"))
        try_number = "N/A"
        log_url = "N/A"

    operator = _safe_string(task.__class__.__name__) if task else "N/A"
    retries = _safe_string(getattr(task, "retries", "N/A")) if task else "N/A"

    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "operator": operator,
        "run_id": _safe_string(context.get("run_id")),
        "logical_date": _safe_string(context.get("logical_date")),
        "try_number": try_number,
        "retries": retries,
        "exception": _safe_string(exception),
        "log_url": log_url,
    }


def _format_slack_message(data: Dict[str, str]) -> str:
    """
    Format the Slack alert message.
    """

    message = f"""
:red_circle: *Airflow Task Failed*

*DAG:* `{data["dag_id"]}`
*Task:* `{data["task_id"]}`
*Operator:* `{data["operator"]}`
*Run ID:* `{data["run_id"]}`
*Logical date:* `{data["logical_date"]}`
*Try number:* `{data["try_number"]}`
*Configured retries:* `{data["retries"]}`

*Error:*
```{data["exception"]}```

*Logs:*
{data["log_url"]}
"""

    return message.strip()


def _send_slack_message(webhook_url: str, message: str) -> None:
    """
    Send a message to Slack using an Incoming Webhook.

    This uses only the Python standard library.
    No extra package is required.
    """

    payload = {"text": message}

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        print(f"[ALERT] Slack alert sent successfully. Status code: {response.status}")


def send_slack_failure_alert(context: Dict[str, Any]) -> None:
    """
    Airflow task failure callback.

    Use this function in DAG default_args:

        default_args = {
            "on_failure_callback": send_slack_failure_alert,
        }

    This function reports the exact failed task.
    """

    webhook_url = os.getenv(SLACK_WEBHOOK_ENV_VAR)

    if not webhook_url:
        print(
            f"[ALERT] {SLACK_WEBHOOK_ENV_VAR} is not configured. "
            "Skipping Slack notification."
        )
        return

    data = _extract_failure_context(context)
    message = _format_slack_message(data)

    try:
        _send_slack_message(webhook_url=webhook_url, message=message)
    except Exception as alert_error:
        print(f"[ALERT] Failed to send Slack alert: {alert_error}")