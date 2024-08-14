from dagster import Definitions, EnvVar, load_assets_from_modules, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource
from dagster_aws.redshift import RedshiftClientResource
from dagster_aws.s3 import S3Resource
from . import assets
from .project import streaming_project_demo_project
from .schedules import schedules

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=streaming_project_demo_project),
        'redshift': RedshiftClientResource( host=EnvVar('REDSHIFT_HOST'),
                                            port=5439,
                                            user=EnvVar('DB_USER'),
                                            password=EnvVar('DB_PASSWORD'),
                                            database='dev'),
        "s3": S3Resource(region_name='ca-central-1')
    },
)