import os

from airflow import configuration


class DagConfiguration:
    def __init__(self):
        self.metric_reference_date = "2022-10-01"
        # self.run_step = "full"  # tb_postpaid_ae_ascore_lowfrequser, #tb_postpaid_ae_ascore_newuser (aligned with model_config.model_name)
        self.time_intervals = [1, 3, 6, 12]
        self.script_folder = f'{configuration.get("core", "dags_folder")}'
        self.country = "ae"
        self.AIRFLOW_ENV = os.getenv("AIRFLOW__DATAHUB__ENVIRONMENT")

        self.exec_config_big = {
            "KubernetesExecutor": {
                "request_cpu": "6",
                "limit_cpu": "20",
                "request_memory": "20Gi",
                "limit_memory": "80Gi",
            }
        }

        self.exec_config_small = {
            "KubernetesExecutor": {
                "request_cpu": "2",
                "limit_cpu": "10",
                "request_memory": "20Gi",
                "limit_memory": "80Gi",
            }
        }

        # Requirements for auto scoring
        self.req_postpaid_ae_ascore_lowfreq = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "a_score/low_frequency/requirements.txt"
        )

        self.req_postpaid_ae_ascore_newuser = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "a_score/new_user/requirements.txt"
        )

        self.req_postpaid_ae_ascore_generic = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "a_score/generic/requirements.txt"
        )

        self.req_postpaid_ae_ascore_combine = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "a_score/combine_scores/requirements.txt"
        )

        self.req_postpaid_ae_bscore_generic = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "b_score/generic/requirements.txt"
        )

        self.req_postpaid_ae_bscore_generic_upgrade = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/"
            + "b_score/upgrades/requirements.txt"
        )

        # Credit Policy sql paths:
        self.credit_policy_sql_path_travelers = self.__get_sql_from_file(
            f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/credit_policy/sql/"
            + "travelers.sql"
        )

        self.credit_policy_sql_path_intl_card_holders = self.__get_sql_from_file(
            f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/credit_policy/sql/"
            + "int_card_holders.sql"
        )

        self.credit_policy_sql_path_initial_pop = self.__get_sql_from_file(
            f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/credit_policy/sql/"
            + "bnpl_initial_cp_population_historical.sql"
        )

        self.credit_policy_sql_path_input_pop = self.__get_sql_from_file(
            f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/credit_policy/sql/"
            + "input_credit_policy_historical.sql"
        )

        # Credit Commitee requirements
        self.req_postpaid_ae_commitee = self.__parse_list(
            path=f"{self.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/credit_commitee/"
            + "python/"
            + "requirements.txt"
        )

        # BQ operator projects:
        if "prod" in self.AIRFLOW_ENV:
            self.bq_schema = "fulfillment-dwh-production"
            self.bq_dataset = "ds_fintech"
        else:
            self.bq_schema = "dhub-fintech"
            self.bq_dataset = "ds_lending"

    def __get_sql_from_file(self, file_path):
        with open(file_path) as f:
            sql = f.read()
        return sql

    # def get_data_load_query(self, report_type):
    #     path = f"{self.script_folder}/data_science/talabat_auto_scoring/sql/{report_type}.sql"
    #     return self.__get_sql_from_file(path)

    def __parse_list(sef, path="requirements.txt"):
        my_file = open(path, "r")
        content = my_file.read()
        content_list = content.split("\n")
        my_file.close()
        return content_list