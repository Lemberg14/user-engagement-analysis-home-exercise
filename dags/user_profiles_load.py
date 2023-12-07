import logging
import os
from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteTableOperator,
)

from schemas.user_profiles_schema import (
    user_profiles_schema_raw,
    user_profiles_schema_master,
)
from sql import user_profiles_sql


logger = logging.getLogger(__name__)
airflow_bucket = Variable.get("airflow_bucket")
report_date = "{{ ds }}"
file_name = f"user_profiles.csv"
blob_name = "data/test/"

dag_args = {
    'owner': 'Yurii',
    'name': 'user_profiles_load',
    'description': 'user_profiles_load',
    'tags': ['user_profiles'],
    "start_date": datetime(2023, 12, 7),
    "end_date": None,
    "interval": "@once",
    'retries': 2,
    'max_active_runs': 1,  # number of running dag a same time
    'task_concurrency': 1,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


default_conf = {
    "run_ds": "{{ ds }}",
    "path": os.path.join("/home/airflow/gcs", blob_name),
}

table_params = {
    "name": "user_profiles",
    "clean_table": "master.user_profiles",
    "raw_table": "raw.user_profiles",
    "source_object": os.path.join(blob_name),
    "raw_dataset": "raw",
    "clean_dataset": "master",
    "cluster_field": "user_id",
}


dag = DAG(
    dag_id=dag_args["name"],
    default_args=dag_args,
    description=dag_args["description"],
    schedule_interval=dag_args["interval"],
    tags=dag_args["tags"],
    catchup=dag_args["catchup"],
    max_active_runs=dag_args["max_active_runs"],
    start_date=dag_args["start_date"],
    concurrency=dag_args["concurrency"],
)

begin = DummyOperator(task_id="begin", dag=dag)

end = DummyOperator(task_id="end", dag=dag)


create_partitioned_table_task = BigQueryCreateEmptyTableOperator(
    task_id="create_partitioned_table",
    dataset_id=table_params["clean_dataset"],
    table_id=table_params["name"],
    schema_fields=user_profiles_schema_master,
    cluster_fields=table_params["cluster_field"],
    exists_ok=True,
    dag=dag,
)

gcs_raw_to_bq_task = GCSToBigQueryOperator(
    task_id=f'{table_params.get("name")}_gcs_to_bq',
    bucket=airflow_bucket,
    schema_fields=user_profiles_schema_raw,
    source_objects=f'{table_params.get("source_object")}{file_name}',
    destination_project_dataset_table=table_params.get("raw_table"),
    field_delimiter=",",
    skip_leading_rows=1,
    source_format="CSV",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

merge_to_persistent_table_task = BigQueryInsertJobOperator(
    task_id=f'{table_params.get("name")}_merge',
    configuration={
        "query": {
            "query": user_profiles_sql.format(
                clean_table=table_params.get("clean_table"),
                raw_table=table_params.get("raw_table"),
                report_date=report_date,
            ),
            "useLegacySql": False,
        },
    },
    dag=dag,
)

begin >> create_partitioned_table_task  >> gcs_raw_to_bq_task >> merge_to_persistent_table_task >> end