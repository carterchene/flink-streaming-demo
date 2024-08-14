"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection
from dagster import load_assets_from_modules, define_asset_job, ScheduleDefinition
from . import assets

all_assets = load_assets_from_modules([assets])

adhoc_dim_location_job = define_asset_job("adhoc_dim_location", selection=['*dim_location'])
adhoc_dim_artists_job = define_asset_job("adhoc_dim_artists", selection=['*dim_artists'])
adhoc_dim_date_job = define_asset_job("adhoc_dim_date", selection=['*dim_date'])
adhoc_dim_songs_job = define_asset_job("adhoc_dim_songs", selection=['*dim_songs'])
adhoc_dim_users_job = define_asset_job("adhoc_dim_users", selection=['*dim_users'])
adhoc_fact_streams_job = define_asset_job("adhoc_fact_streams", selection=['*fact_streams'])

hourly_streams_load = define_asset_job("hourly_streams_load", selection=['*one_big_table']) # * notation means everything upstream of the asset



hourlys_dim_location_schedule = ScheduleDefinition(
    job=hourly_streams_load,
    cron_schedule = '0 * * * *' # every hour
)

schedules = [
    hourlys_dim_location_schedule
]