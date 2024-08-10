from pathlib import Path

from dagster_dbt import DbtProject

streaming_project_demo_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "..", "dbt", "streaming_project_demo").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
streaming_project_demo_project.prepare_if_dev()