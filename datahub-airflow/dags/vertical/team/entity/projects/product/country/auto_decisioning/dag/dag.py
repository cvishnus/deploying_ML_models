from datetime import date, datetime, timedelta

from airflow import DAG
from datahub.common import alerts
from datahub.common.constants import TaskQueue

from configuration.default_params import DAG_DEFAULT_ARGS_DS
from team.entity.projects.product.country.auto_decisioning.dag.dag_configuration import (
    DagConfiguration,
)
from team.entity.projects.product.country.auto_decisioning.dag.dag_tasks import (
    AutoDecisioningTasks,
)

version = 1
change_log = 12  # only used to see if the code in airflow got updated, so increment this, and be sure that we are running the right code

default_args = {
    # "run_step": DagConfiguration().run_step,
    "dt_cmmtt": date.today()
    - timedelta(
        days=0
    ),  # Talabat's API looks at the -1 day, however our pipeline ends at night
    "metrics_ref_date": date.today() - timedelta(days=2),
    "retries": 2,
    "max_active_runs": 1,
    "retry_delay": timedelta(minutes=5),
    "startup_timeout_seconds": 900,
    "queue": TaskQueue.KUBERNETES,
    "start_date": datetime(2022, 8, 31, 7, 00, 0),
}

doc_md = """
 Dag which is responsible to automate decisioning.
"""


with DAG(
    dag_id=f"entity-product-auto-decisioning-{DagConfiguration().country}-v{version}",
    description="Dag which is responsible to automate models...",
    # schedule="30 20 * * *",  # if you use 0 9 * * 1 then it means once in a week on Monday 09.00,
    schedule=None,
    default_args={**DAG_DEFAULT_ARGS_DS, **default_args},
    max_active_runs=1,
    tags=[DAG_DEFAULT_ARGS_DS["owner"], "dwh", "talabat"],
    catchup=False,  # if true it runs retroactively for backfilling purposes
    on_failure_callback=alerts.setup_on_failure_callback(),
    access_control={"DS_execute_dag_export": {"can_edit"}},
    params={
        "metrics_ref_date": default_args["metrics_ref_date"],
        "dt_cmmtt": default_args["dt_cmmtt"],
    },
) as dag:
    dag.doc_md = doc_md
    auto_decisioning = AutoDecisioningTasks(dag)
    auto_decisioning.render()
    exec_config = {
        "KubernetesExecutor": {
            "request_cpu": "6",
            "limit_cpu": "20",
            "request_memory": "20Gi",
            "limit_memory": "80Gi",
        }
    }