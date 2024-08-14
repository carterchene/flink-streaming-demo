from dagster import AssetExecutionContext, MaterializeResult, asset, op, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_aws.redshift import RedshiftClientResource
from dagster_aws.s3 import S3Resource
from pathlib import Path
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import io
import pandas as pd

from .project import streaming_project_demo_project

load_dotenv()
S3_BUCKET =  EnvVar('S3_BUCKET').get_value()
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
REDSHIFT_IAM = os.getenv('REDSHIFT_IAM')

def last_hour_file_path() -> str:
    """
    Generates a string of the partitioned file path for the last hour

    """
    hour_ago = datetime.now() - timedelta(hours=1)
    path = f'year={hour_ago.year}/month={hour_ago.month:02}/day={hour_ago.day:02}/hour={hour_ago.hour:02}'
    return path

def generate_copy_statement(table: str, directory: str) -> str:
    """
    Generates a COPY statement for the last hour for a given table

    table: table to copy
    directory: first level directory for given table. clean or outputs
    """

    path = last_hour_file_path()
    copy_from_s3 = f"""COPY dev.stage.{table}
                FROM 's3://{S3_BUCKET}/{directory}/{table}/{path}'
                IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_IAM}'
                FORMAT AS PARQUET;
                """

    return copy_from_s3

@asset
def clean_parquet(s3: S3Resource):
    """
        For some reason the parquets generated for auth_events are not working with the COPY statement in redshift.
        This function just takes those parquets and rewrites them, that seems to fix the issue. 
        I could've troubleshooted on the Flink side but the cloud costs were getting too expensive. 
    """
    s3_client = s3.get_client()
    path = last_hour_file_path()
    key = f'outputs/auth_events/{path}'
    
    response = s3_client.get_object(Bucket='streaming-demo-project', Key=key)
    file_content = response['Body'].read()
    parquet_file = io.BytesIO(file_content)
    df = pq.read_table(parquet_file).to_pandas()
    
    # write parquet back to file 
    output_key = f'clean/auth_events/{path}.parquet'
    output_buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), output_buffer)
    
    s3_client.put_object(
        Bucket='streaming-demo-project',
        Key=output_key,
        Body=output_buffer.getvalue()
    )
    
    return f"{output_key} uploaded successfully."

@dbt_assets(manifest=streaming_project_demo_project.manifest_path)
def streaming_project_demo_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(description='Load listen_events s3 parquet to redshift')
def stage_listen_events(context, redshift: RedshiftClientResource):

    copy_from_s3 = generate_copy_statement('listen_events', directory='outputs')
    
    result = redshift.get_client().execute_query(copy_from_s3, fetch_results=True)
    
    return result


@asset(description='Load auth_events s3 parquet to redshift')
def stage_auth_events(context, redshift: RedshiftClientResource):

    copy_from_s3 = generate_copy_statement('auth_events', directory='clean')
    
    result = redshift.get_client().execute_query(copy_from_s3, fetch_results=True)
    
    return result

@asset(description='Load page_view_events s3 parquet to redshift')
def stage_page_view_events(context, redshift: RedshiftClientResource):

    copy_from_s3 = generate_copy_statement('page_view_events', directory='outputs')
    
    result = redshift.get_client().execute_query(copy_from_s3, fetch_results=True)
    
    return result

@asset(description='Load status_change_events s3 parquet to redshift')
def stage_status_change_events(context, redshift: RedshiftClientResource):

    copy_from_s3 = generate_copy_statement('status_change_events', directory='outputs')
    
    result = redshift.get_client().execute_query(copy_from_s3, fetch_results=True)
    
    return result

