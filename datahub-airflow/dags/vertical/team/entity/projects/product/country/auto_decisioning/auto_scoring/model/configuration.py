import os
from typing import Dict

import numpy as np
import pandas as pd
from google.cloud import bigquery

from helpers.gcp_connection import GCP


class ModelConfiguration:
    def __init__(self):
        # model configuration
        self.RUN_LOCAL_ENV = False
        self.RUN_AIRFLOW = not self.RUN_LOCAL_ENV
        if self.RUN_AIRFLOW:
            self.AIRFLOW_ENV = os.getenv(
                "AIRFLOW__DATAHUB__ENVIRONMENT"
            )  # development OR production
            from data_science.talabat.projects.post_paid.ae.auto_decisioning.dag.dag_configuration import (
                DagConfiguration,
            )

            dag_config = DagConfiguration()

        self.model_name = "tb_postpaid_ae_ascore_generic"
        self.model_version = "v16.0.0"
        self.saved_model_object_type = "onnx"
        self.model_object_name = (
            self.model_name
            + "_"
            + self.model_version
            + "_"
            + "model."
            + self.saved_model_object_type
        )

        # bq configuration
        self.bq_conn = GCP("bigquery")
        self.bq_project = "dhub-fintech"
        self.bq_location = "US"
        self.bq_client = bigquery.Client(
            project=self.bq_project, location=self.bq_location
        )
        self.gcs_conn = GCP("storage")

        # gcs configuration, for seeing an example gcs folder structure see under fintech-data-science/talabat_postpaid_models/ae/ascore/low_freq_user
        self.gcs_bucket = "fintech-data-science"
        self.gcs_folder_name = "talabat_postpaid_models"
        self.gcs_region = "ae"
        self.gcs_model_type = "ascore"
        self.gcs_model_scope = "generic"
        self.gcs_artefacts_path = (
            self.gcs_bucket
            + "/"
            + self.gcs_folder_name
            + "/"
            + self.gcs_region
            + "/"
            + self.gcs_model_type
            + "/"
            + self.gcs_model_scope
            + "/"
            + self.model_version
            + "/"
            + "artefacts"
        )
        self.gcs_blob_model_source_path = (
            self.gcs_folder_name
            + "/"
            + self.gcs_region
            + "/"
            + self.gcs_model_type
            + "/"
            + self.gcs_model_scope
            + "/"
            + self.model_version
            + "/"
            + "artefacts/model_object/"
            + self.model_object_name
        )

        self.gcs_model_object_path = (
            self.gcs_artefacts_path + "/" + "model_object" "/" + self.model_object_name
        )
        self.gcs_feature_files_path = self.gcs_artefacts_path + "/features"

        self.gcs_feature_file_paths_dict = {
            "table_features": self.gcs_feature_files_path + "/Credit_data_schema_v2.csv"
        }

        self.gcs_data_path = (
            self.gcs_bucket
            + "/"
            + self.gcs_folder_name
            + "/"
            + self.gcs_region
            + "/"
            + self.gcs_model_type
            + "/"
            + self.gcs_model_scope
            + "/"
            + self.model_version
            + "/"
            + "data"
        )

        if self.RUN_AIRFLOW and ("prod" in self.AIRFLOW_ENV):
            self.bq_schema = "fulfillment-dwh-production"
            self.bq_dataset = "ds_fintech"
        else:
            self.bq_schema = "dhub-fintech"
            self.bq_dataset = "ds_lending"

        # bq tables configuration, any input tables used in the queries go here
        self.bq_input_tablenames_dict = {
            "fct_order_info": "tlb-data-prod.data_platform.fct_order_info",
            "fct_bnpl_order": "tlb-data-prod.data_platform.fct_bnpl_order",
            "bnpl_total_population": f"{self.bq_schema}.{self.bq_dataset}.test_score_bnpl_total_population",
            "dim_bnpl_customer_cohort": "tlb-data-prod.data_platform.dim_bnpl_customer_cohort",
            "stg_payment_method_spending_extended_6m": f"{self.bq_schema}.{self.bq_dataset}.stg_payment_method_spending_extended_6m",
            "stg_payment_method_spending_extended_12m": f"{self.bq_schema}.{self.bq_dataset}.stg_payment_method_spending_extended_12m",
            "stg_payment_method_spending_extended_3m": f"{self.bq_schema}.{self.bq_dataset}.stg_payment_method_spending_extended_3m",
            "stg_payment_method_spending_extended_1m": f"{self.bq_schema}.{self.bq_dataset}.stg_payment_method_spending_extended_1m",
            "stg_cc_db_card_count": f"{self.bq_schema}.{self.bq_dataset}.stg_cc_db_card_count",
            "week3dip_platform": f"{self.bq_schema}.{self.bq_dataset}.week3dip_platform",
            "fraud_feat_platform": f"{self.bq_schema}.{self.bq_dataset}.fraud_feat_platform",
            "activation_scores": f"{self.bq_schema}.{self.bq_dataset}.bnpl_a_score_auto",
        }

        if self.RUN_LOCAL_ENV:
            self.project_path = "./data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/generic/"

        elif self.RUN_AIRFLOW:
            self.project_path = f"{dag_config.script_folder}/data_science/talabat/projects/post_paid/ae/auto_decisioning/auto_scoring/a_score/generic/"

        self.bq_query_paths = {"data_load": f"{self.project_path}queries/data_load.sql"}

        self.bq_ascore_table = f"{self.bq_schema}.{self.bq_dataset}.bnpl_a_score_auto"

        self.bq_ascore_table_dtype_dict = {
            "customer_id": bigquery.enums.SqlTypeNames.INT64,
            "metrics_ref_date": bigquery.enums.SqlTypeNames.DATE,
            "predict": bigquery.enums.SqlTypeNames.FLOAT64,
            "Segment": bigquery.enums.SqlTypeNames.STRING,
            "a_score": bigquery.enums.SqlTypeNames.FLOAT64,
            "Model": bigquery.enums.SqlTypeNames.STRING,
            "country_id": bigquery.enums.SqlTypeNames.INT64,
        }
        self.bq_ascore_table_pandas_dict = {
            "customer_id": int,  # takes type value in astype()
            "metrics_ref_date": "%Y-%m-%d",  # takes format value for pd.to_datetime()
            "predict": float,  # takes type value in astype()
            "Segment": str,  # takes type value in astype()
            "a_score": float,  # takes type value in astype()
            "Model": str,  # takes type value in astype()
            "country_id": int,  # takes type value in astype()
        }

        # define score cuts to generate segments
        self.late_activators_score_cuts = [0, 0.067, 0.09436, 0.125, 0.3, np.inf]
        self.late_activators_score_cut_labels = ["A&B", "C", "D+", "D++", "E"]
        self.early_activators_score_cuts = [0, 0.05, 0.0770416259765625, np.inf]
        self.early_activators_score_cut_labels = ["A&B", "D++", "E"]
        self.generic_activation_cutoffs = [0, 0.174817, np.inf]
        self.generic_activation_labels = ["late", "early"]

    def gcs_load_feature_types(self):
        """
        This function loads the feature lists stored in csv located in gcs buckets.
        And returns a dictionary of lists with column names.
        The lists of columns it returns do not necessarily equal to to list of actual model feature names,
        as this list is used as an input to prepare the select columns from bq query.
        """
        table_feature_df = pd.read_csv(
            f"gs://{self.gcs_feature_file_paths_dict['table_features']}"
        )
        table_feature_df = table_feature_df[["feature_name", "feature_data_type"]]

        return table_feature_df

    def gsc_read_json_as_dict(self, gcs_path) -> Dict:
        """
        This function reads the json files from gcs and turns into a dictionary
        """

        dict_to_return = pd.read_json(f"gs://{gcs_path}").to_dict()

        return dict_to_return

    def parse_list(sef, path="requirements.txt"):
        my_file = open(path, "r")
        content = my_file.read()
        content_list = content.split("\n")
        my_file.close()
        return content_list