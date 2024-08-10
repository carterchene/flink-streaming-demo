from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import streaming_project_demo_project


@dbt_assets(manifest=streaming_project_demo_project.manifest_path)
def streaming_project_demo_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    