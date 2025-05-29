from datetime import timedelta

from datahub.common.constants import TaskQueue

DAG_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": None,
    "end_date": None,
    "email": None,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "max_active_tasks": 20,
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=2),
    "queue": TaskQueue.KUBERNETES,
    "executor_config": {
        "KubernetesExecutor": {
            "request_cpu": "500m",
            "limit_cpu": "500m",
            "request_memory": "750Mi",
            "limit_memory": "750Mi",
        },
    },
}

DAG_DEFAULT_ARGS_DS = DAG_DEFAULT_ARGS.copy()
DAG_DEFAULT_ARGS_DS["owner"] = "data_science"

PSP_EXECUTOR_CONFIG = {
    "KubernetesExecutor": {
        "request_cpu": "100m",
        "limit_cpu": "1",
        "request_memory": "256Mi",
        "limit_memory": "1Gi",
    },
}

ALFRED_EXECUTOR_CONFIG = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "executor_config": {
        "KubernetesExecutor": {
            "request_cpu": "500m",
            "limit_cpu": "500m",
            "request_memory": "750Mi",
            "limit_memory": "750Mi",
        },
    },
}