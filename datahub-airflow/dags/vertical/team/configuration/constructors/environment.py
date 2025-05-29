from dataclasses import dataclass


@dataclass
class DbtConfig:
    auth_method: str
    target: str


@dataclass
class EnvironmentConfig:
    dbt_config: DbtConfig