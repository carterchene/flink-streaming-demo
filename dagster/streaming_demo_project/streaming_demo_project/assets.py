from dagster import AssetExecutionContext, MaterializeResult, asset
from dagster_dbt import DbtCliResource, dbt_assets
import boto3 
import os
import psycopg2

from .project import streaming_project_demo_project

S3_BUCKET = os.getenv('S3_BUCKET')
ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID')
REDSHIFT_IAM = os.getenv('REDSHIFT_IAM')

@dbt_assets(manifest=streaming_project_demo_project.manifest_path)
def streaming_project_demo_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@asset(description='Load from s3 parquet to redshift')
def s3_to_redshift() -> MaterializeResult:
    conn = psycopg2.connect(
        dbname='dev',
        user='your_username',
        password='your_password',
        host='your_redshift_cluster_endpoint',
        port='5439'
    )
    cursor = conn.cursor()

    copy_from_s3 = f"""COPY dev.public.listen_events
                FROM 's3://{S3_BUCKET}/outputs/listen_events'
                IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_IAM}'
                FORMAT AS PARQUET;
                """

    try:
        cursor.execute(copy_from_s3)
        conn.commit()
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
