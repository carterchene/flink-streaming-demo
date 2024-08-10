from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import streaming_project_demo_dbt_assets, s3_to_redshift
from .project import streaming_project_demo_project
from .schedules import schedules

defs = Definitions(
    assets=[streaming_project_demo_dbt_assets, s3_to_redshift],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=streaming_project_demo_project),
    },
)