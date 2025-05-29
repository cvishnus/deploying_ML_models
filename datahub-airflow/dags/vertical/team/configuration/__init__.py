import os

from airflow.configuration import conf
from datahub.common.configuration import Config
from datahub.common.helpers import create_pool

from configuration.constructors.environment import DbtConfig, EnvironmentConfig

common_config = Config()
common_config.add_config("config", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config("config_analytics", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config(
    "config_analytics_data_science", f"{os.path.dirname(__file__)}/yaml"
)
common_config.add_config(
    "config_analytics_payment", f"{os.path.dirname(__file__)}/yaml"
)
common_config.add_config(
    "config_analytics_product", f"{os.path.dirname(__file__)}/yaml"
)
common_config.add_config("config_analytics_risk", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config("config_data_eng", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config("config_talabat", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config("config_dag_dependency", f"{os.path.dirname(__file__)}/yaml")
common_config.add_config(
    "config_dag_dependency_data_science", f"{os.path.dirname(__file__)}/yaml"
)
common_config.add_config("acl", f"{os.path.dirname(__file__)}/yaml/acl")

# DWH Imports
common_config.add_config("dwh_imports", f"{os.path.dirname(__file__)}/yaml/dwh_imports")
common_config.add_config(
    "dwh_imports_psp", f"{os.path.dirname(__file__)}/yaml/dwh_imports"
)
common_config.add_config(
    "dwh_imports_alfred", f"{os.path.dirname(__file__)}/yaml/dwh_imports"
)
common_config.add_config(
    "dwh_imports_antifraud", f"{os.path.dirname(__file__)}/yaml/dwh_imports"
)
common_config.add_config(
    "dwh_imports_gsheet", f"{os.path.dirname(__file__)}/yaml/dwh_imports"
)

common_config.add_config(
    "dwh_imports_payments_revamp", f"{os.path.dirname(__file__)}/yaml/dwh_imports"
)

# PSP fetchers config
common_config.add_config("config_psp", f"{os.path.dirname(__file__)}/yaml/psp")

# Alfred
common_config.add_config(
    "config_alfred", f"{os.path.dirname(__file__)}/yaml/db_fetchers/alfred"
)

# Realtime to Batch
common_config.add_config(
    "config_realtime_to_batch", f"{os.path.dirname(__file__)}/yaml"
)

# Payments
common_config.add_config(
    "config_payment", f"{os.path.dirname(__file__)}/yaml/db_fetchers/payment"
)

# Control Plane
common_config.add_config(
    "config_control_plane",
    f"{os.path.dirname(__file__)}/yaml/db_fetchers/control_plane",
)

# Antifraud
common_config.add_config(
    "config_antifraud", f"{os.path.dirname(__file__)}/yaml/antifraud"
)


# Curated Data
common_config.add_config(
    "curated_data", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config(
    "curated_data_alfred", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config(
    "curated_data_antifraud", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config(
    "curated_data_psp", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config(
    "curated_data_recon", f"{os.path.dirname(__file__)}/yaml/curated_data"
)
common_config.add_config(
    "curated_data_platform_orders", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_payments_revamp", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_payments_datamodeling",
    f"{os.path.dirname(__file__)}/yaml/curated_data",
)

common_config.add_config(
    "curated_data_da_fraud", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_da_lending", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_da_payment", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_da_wallet", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_ds_fraud", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_ds_lending", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

common_config.add_config(
    "curated_data_datamarts", f"{os.path.dirname(__file__)}/yaml/curated_data"
)

# RL/DS data
common_config.add_config("da_fraud", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")

common_config.add_config("da_lending", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")

common_config.add_config("da_payment", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")

common_config.add_config("da_wallet", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")

common_config.add_config("ds_fraud", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")

common_config.add_config("ds_lending", f"{os.path.dirname(__file__)}/yaml/rl_ds_data")


config = common_config.config
config_instance = common_config
config_analytics = config.get("config_analytics")
config_analytics_data_science = config.get("config_analytics_data_science")
config_analytics_payment = config.get("config_analytics_payment")
config_analytics_product = config.get("config_analytics_product")
config_analytics_risk = config.get("config_analytics_risk")
config_data_eng = config.get("config_data_eng")
config_talabat = config.get("config_talabat")
config_dag_dependency = config.get("config_dag_dependency")
config_dag_dependency_data_science = config.get("config_dag_dependency_data_science")
config_alfred = config.get("config_alfred")
config_realtime_to_batch = config.get("config_realtime_to_batch")
config_antifraud = config.get("config_antifraud")
config_payments = config.get("config_payment")
config_control_plane = config.get("config_control_plane")

for pool_name, pool_attributes in config["pools"].items():
    create_pool(
        pool_attributes["name"],
        pool_attributes["slots"],
        pool_attributes["description"],
    )

# dbt env configs
ENVIRONMENT = conf.get("datahub", "environment")
IS_PRODUCTION = ENVIRONMENT == "production"
IS_STAGING = ENVIRONMENT == "staging"
IS_DEVELOPMENT = ENVIRONMENT == "development"

CONFIG = EnvironmentConfig(
    dbt_config=DbtConfig(auth_method="oauth", target="dev"),
)

if IS_STAGING:
    CONFIG = EnvironmentConfig(
        dbt_config=DbtConfig(auth_method="oauth", target="stag"),
    )

if IS_PRODUCTION:
    CONFIG = EnvironmentConfig(
        dbt_config=DbtConfig(auth_method="oauth", target="prod"),
    )