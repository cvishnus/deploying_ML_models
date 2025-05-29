# from datetime import timedelta

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
from datahub.operators.bigquery.bigquery_operator import BigQueryOperator

from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.a_score.combine_scores.main import (
    call_combine_scores as call_combine_scores_a,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.a_score.generic.main import (
    call_start_scoring as call_start_scoring_generic_ascore,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.a_score.low_frequency.main import (
    call_start_scoring as call_start_scoring_lowfreq,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.a_score.new_user.main import (
    call_start_scoring as call_start_scoring_newuser,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.act_score.generic.main import (
    call_start_scoring as call_start_scoring_generic_act_score,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.b_score.generic.main import (
    call_start_scoring as call_start_scoring_generic_b,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.auto_scoring.b_score.upgrades.main import (
    call_upgrading as call_upgrading_generic_b,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.credit_commitee.python.main import (
    call_reflect_commitee_decisions,
)
from data_science.talabat.projects.post_paid.ae.auto_decisioning.dag.dag_configuration import (
    DagConfiguration,
)


class AutoDecisioningTasks:
    def __init__(self, dag):
        self.dag = dag
        self.dag_config = DagConfiguration()
        # self.run_step = self.dag.params.dump()["run_step"]

    def auto_decisioning(self):
        # scoring
        start_scoring_tasks = EmptyOperator(task_id="start_scoring_tasks")
        # end_scoring_tasks = EmptyOperator(task_id="end_scoring_tasks")

        score_postpaid_ae_actscore_generic = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-act-score-generic-python",
            requirements=self.dag_config.req_postpaid_ae_ascore_generic,
            system_site_packages=True,
            python_callable=call_start_scoring_generic_act_score,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        score_postpaid_ae_ascore_lowfreq = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-ascore-lowfreq-python",
            requirements=self.dag_config.req_postpaid_ae_ascore_lowfreq,
            system_site_packages=True,
            python_callable=call_start_scoring_lowfreq,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        score_postpaid_ae_ascore_newuser = PythonVirtualenvOperator(
            task_id=f"postpaid-a{DagConfiguration().country}-ascore-newuser-python",
            requirements=self.dag_config.req_postpaid_ae_ascore_newuser,
            system_site_packages=True,
            python_callable=call_start_scoring_newuser,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        score_postpaid_ae_ascore_generic = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-ascore-generic-python",
            requirements=self.dag_config.req_postpaid_ae_ascore_generic,
            system_site_packages=True,
            python_callable=call_start_scoring_generic_ascore,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        combine_postpaid_ae_ascore = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-ascore-combine-python",
            requirements=self.dag_config.req_postpaid_ae_ascore_combine,
            system_site_packages=True,
            python_callable=call_combine_scores_a,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
        )

        score_postpaid_ae_bscore_generic = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-bscore-generic-python",
            requirements=self.dag_config.req_postpaid_ae_bscore_generic,
            system_site_packages=True,
            python_callable=call_start_scoring_generic_b,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        upgrade_postpaid_ae_bscore_generic = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-bscore-generic-upgrade-python",
            requirements=self.dag_config.req_postpaid_ae_bscore_generic,
            system_site_packages=True,
            python_callable=call_upgrading_generic_b,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"metrics_ref_date": "{{ params.metrics_ref_date }}"},
        )

        # credit policy
        start_credit_policy = EmptyOperator(task_id="start_credit_policy")
        # end_credit_policy = EmptyOperator(task_id="end_credit_policy")

        cp_travelers = BigQueryOperator(
            task_id=f"postpaid-{DagConfiguration().country}-cp-travelers",
            sql=self.dag_config.credit_policy_sql_path_travelers,
            use_legacy_sql=False,
            params={
                "bq_dataset": self.dag_config.bq_dataset,
                "bq_schema": self.dag_config.bq_schema,
            },
        )

        cp_intl_card_holders = BigQueryOperator(
            task_id=f"postpaid-{DagConfiguration().country}-cp-intl-card-holders",
            sql=self.dag_config.credit_policy_sql_path_intl_card_holders,
            use_legacy_sql=False,
            params={
                "bq_dataset": self.dag_config.bq_dataset,
                "bq_schema": self.dag_config.bq_schema,
            },
        )

        cp_population = BigQueryOperator(
            task_id=f"postpaid-{DagConfiguration().country}-cp-population",
            sql=self.dag_config.credit_policy_sql_path_initial_pop,
            use_legacy_sql=False,
            params={
                "bq_dataset": self.dag_config.bq_dataset,
                "bq_schema": self.dag_config.bq_schema,
            },
        )

        cp_input = BigQueryOperator(
            task_id=f"postpaid-{DagConfiguration().country}-input-credit-policies",
            sql=self.dag_config.credit_policy_sql_path_input_pop,
            use_legacy_sql=False,
            params={
                "bq_dataset": self.dag_config.bq_dataset,
                "bq_schema": self.dag_config.bq_schema,
            },
        )

        # credit committee
        start_commitee_tasks = EmptyOperator(task_id="start_commitee_tasks")
        end = EmptyOperator(task_id="end")

        postpaid_ae_commitee = PythonVirtualenvOperator(
            task_id=f"postpaid-{DagConfiguration().country}-credit-committee",
            requirements=self.dag_config.req_postpaid_ae_commitee,
            system_site_packages=True,
            python_callable=call_reflect_commitee_decisions,
            executor_config=self.dag_config.exec_config_big,
            dag=self.dag,
            op_kwargs={"commitee_date": "{{ params.dt_cmmtt }}"},
        )

        (
            start_scoring_tasks
            >> score_postpaid_ae_actscore_generic
            >> score_postpaid_ae_ascore_lowfreq
            >> score_postpaid_ae_ascore_newuser
            >> score_postpaid_ae_ascore_generic
            >> combine_postpaid_ae_ascore
            >> score_postpaid_ae_bscore_generic
            >> upgrade_postpaid_ae_bscore_generic
            >> start_credit_policy
        )

        (
            start_credit_policy
            >> cp_travelers
            >> cp_intl_card_holders
            >> cp_population
            >> cp_input
            >> start_commitee_tasks
        )

        (start_commitee_tasks >> postpaid_ae_commitee >> end)

    def render(self):
        self.auto_decisioning()