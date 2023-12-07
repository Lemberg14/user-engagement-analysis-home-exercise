# user-engagement-analysis-home-exercise
User Engagement Analysis Data Engineer Home Exercise

Task 1 Data Cleaning and Transformation
GCP ETL using Airflow for user_profiles csv file


I created pipline based on Airflow DAG's which run's on GCP's Composer, data warehouse will be based on BigQuery with raw dataset as staged and master dataset as clean and calculated dataset ready for use

So firstly user_profiles_load DAG runs which one creates master.user_profiles table, after upload's user_profiles.csv from Google Cloud Storage to BigQuery raw csv using GCSToBigQueryOperator and in the end it select's needed data from raw.user_profiles, add's engagement_duration collumn, does ifnull function for last_login_date and finishes with deduplication for user_id which is unique distinct field for this table

Notice! For Visualizations of reports on user engagement insights we can use Google Data Studios, for this i have created requested by task views based on master table's from data warehouse but in this documenatation only presents examples of running those views directly in BigQuery. The same result will be on Data Studio charts which will directly connect to reporting data set which includes all the views

```python
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
from sql.user_profiles_sql import upsert_user_profiles_sql


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
```
Schema for raw
```python
user_profiles_schema_raw = [
    {"name": "user_id", "type": "INTEGER"},
    {"name": "signup_date", "type": "DATE"},
    {"name": "last_login_date", "type": "DATE"},
]
```
Schema for master
```python
user_profiles_schema_master = [
    {"name": "user_id", "type": "INTEGER", "description": "id of the user"},
    {"name": "signup_date", "type": "DATE", "description": "signup date of the user"},
    {"name": "last_login_date", "type": "DATE", "description": "last login date of the user"},
    {"name": "engagement duration", "type": "DATE", "description": "time between signup and last login"},
    {"name": "etl_batch_date", "type": "DATE", "description": "airflow run date"},
]
```
SQL merge query from raw to master with adding engagement_duration collumn, deduplication removal, plus ifnull checks 
```python
upsert_user_profiles_sql = """
MERGE {master_table} AS T
USING (
    SELECT
        user_id,
        signup_date,
        IFNULL(last_login_date, signup_date) AS last_login_date,
        DATE_DIFF(last_login_date, signup_date, DAY) AS engagement_duration,
        DATE('{report_date}') AS etl_batch_date
    FROM
    (
        SELECT  *,
            ROW_NUMBER() OVER(PARTITION BY user_id
            ORDER BY signup_date desc) AS rnk
        FROM {raw_table}
        ) t
    WHERE rnk = 1 AND user_id is not null AND signup_date is not null
    ) AS S
ON T.user_id = S.user_id
WHEN MATCHED AND T.etl_batch_date <= S.etl_batch_date THEN
    UPDATE SET
        T.signup_date = S.signup_date,
        T.last_login_date = S.last_login_date,
        T.engagement_duration = S.engagement_duration,
        T.etl_batch_date = S.etl_batch_date
WHEN NOT MATCHED THEN
    INSERT (
        user_id,
        signup_date,
        last_login_date,
        engagement_duration,
        etl_batch_date
    )
    VALUES(
        S.user_id,
        S.signup_date,
        S.last_login_date,
        S.engagement_duration,
        S.etl_batch_date
    )
"""
```
GCP ETL using Airflow for user_events csv file

In paralel user_events_load DAG runs, firstly waiting for user_profiles_load DAG to be done using ExternalTaskSensor, after it the workflow kinda same, it creates master.user_events table, after upload's user_events.csv from Google Cloud Storage to BigQuery raw csv using GCSToBigQueryOperator and in the end it select's needed data from raw.user_profiles, add's engagement_duration collumn and finishes with deduplication for event_id which is unique distinct field for this table
```python
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

from airflow.sensors.external_task import ExternalTaskSensor

from schemas.user_events_schema import (
    user_events_schema_raw,
    user_events_schema_master,
)
from sql.user_events_sql import upsert_user_events_sql


logger = logging.getLogger(__name__)
airflow_bucket = Variable.get("airflow_bucket")
report_date = "{{ ds }}"
file_name = f"user_events.csv"
blob_name = "data/test/"

dag_args = {
    'owner': 'Yurii',
    'name': 'user_events_load',
    'description': 'user_events_load',
    'tags': ['user_events'],
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
    "name": "user_events",
    "master_table": "master.user_events",
    "raw_table": "raw.user_events",
    "source_object": os.path.join(blob_name),
    "raw_dataset": "raw",
    "clean_dataset": "master",
    "cluster_field": "event_id",
    "users_master_table: "master.user_profiles"
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

wait_for_user_load = ExternalTaskSensor(
    task_id="wait_for_user_load",
    external_dag_id="user_profiles_load",
    check_existence=True,
    timeout=9 * 60 * 60,
    mode="reschedule",
    dag=dag,
)

begin = DummyOperator(task_id="begin", dag=dag)

end = DummyOperator(task_id="end", dag=dag)


create_partitioned_table_task = BigQueryCreateEmptyTableOperator(
    task_id="create_partitioned_table",
    dataset_id=table_params["clean_dataset"],
    table_id=table_params["name"],
    schema_fields=user_events_schema_master,
    cluster_fields=table_params["cluster_field"],
    exists_ok=True,
    dag=dag,
)

gcs_raw_to_bq_task = GCSToBigQueryOperator(
    task_id=f'{table_params.get("name")}_gcs_to_bq',
    bucket=airflow_bucket,
    schema_fields=user_events_schema_raw,
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
            "query": user_events_sql.format(
                master_table=table_params.get("master_table"),
                users_master_table=table_params.get("users_master_table"),
                raw_table=table_params.get("raw_table"),
                report_date=report_date,
            ),
            "useLegacySql": False,
        },
    },
    dag=dag,
)

begin >> create_partitioned_table_task  >> gcs_raw_to_bq_task >> merge_to_persistent_table_task >> end
```
Schema for raw
```python
user_events_schema_raw = [
    {"name": "event_id", "type": "INTEGER"},
    {"name": "user_id", "type": "INTEGER"},
    {"name": "event_type", "type": "STRING"},
    {"name": "event_date", "type": "DATETIME"},
    {"name": "page", "type": "STRING"},
]
```
Schema for master
```python
user_profiles_schema_master = [
    {"name": "event_id", "type": "INTEGER", "description": "id of the event"},
    {"name": "user_id", "type": "INTEGER", "description": "id of the user"},
    {"name": "event_type", "type": "STRING", "description": "event type name"},
    {"name": "event_date", "type": "DATETIME", "description": "datetime of the event"},
    {"name": "page", "type": "STRING", "description": "place of the event"},
    {"name": "etl_batch_date", "type": "DATE", "description": "airflow run date"},
]
```
SQL merge query from raw to master with deduplication removal
```python
upsert_user_events_sql = """
MERGE {master_table} AS T
USING (
    SELECT
        event_id,
        user_id,
        event_type,
        event_date,
        page,
        DATE('{report_date}') AS etl_batch_date
    FROM
    (
        SELECT  *,
            ROW_NUMBER() OVER(PARTITION BY ues.event_id
            ORDER BY ues.event_date desc) AS rnk
        FROM {raw_table} ues
        LEFT JOIN {users_master_table} up ON ues.user_id = up.user_id
        WHERE DATE(ues.event_date) >= DATE(up.signup_date)
        ) t
    WHERE rnk = 1
    ) AS S
ON T.event_id = S.event_id AND T.user_id = S.user_id
WHEN MATCHED AND T.etl_batch_date <= S.etl_batch_date THEN
    UPDATE SET
        T.event_type = S.event_type,
        T.event_date = S.event_date,
        T.event_date = S.event_date,
        T.page = S.page,
        T.etl_batch_date = S.etl_batch_date
WHEN NOT MATCHED THEN
    INSERT (
        event_id,
        user_id,
        event_type,
        event_date,
        page,
        etl_batch_date
    )
    VALUES(
        S.event_id,
        S.user_id,
        S.event_type,
        S.event_date,
        S.page,
        S.etl_batch_date
    )
"""
```
Task 2
For Engagement Analysis i would prefer to use Bigquery views to analis the data

2.1 - Calculate the daily active users (DAU) and monthly active users (MAU)

DAU
```sql
CREATE OR REPLACE VIEW reporting.dau
AS
SELECT
  DATE(event_date) AS date,
  COUNT(DISTINCT user_id) AS dau
FROM master.user_events
GROUP BY date
ORDER BY date;
```
![Screenshot 2023-12-07 at 17 35 52](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/d761781a-f742-4c83-8b55-7cd532caa0ab)

MAU
```sql
CREATE OR REPLACE VIEW reporting.mau
AS
SELECT
  EXTRACT(YEAR FROM event_date) AS year,
  EXTRACT(MONTH FROM event_date) AS month,
  COUNT(DISTINCT user_id) AS mau
FROM master.user_events
GROUP BY year, month
ORDER BY year, month;
```
![Screenshot 2023-12-07 at 17 35 01](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/a5ad85e5-fa2a-4730-920a-22df6ddbac8d)

2.2 - Identify the top 5 most engaged users based on the number of events triggered.
```sql
CREATE OR REPLACE VIEW reporting.most_engaged_users
AS
SELECT user_id,
       Count(event_id) AS events_count
FROM master.user_events
GROUP BY user_id
ORDER BY events_count DESC
limit 5;
```
![Screenshot 2023-12-07 at 17 34 10](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/13b9404e-1b28-4301-a158-d5aa3d370106)

2.3 - Determine the most common event types and their distribution over time.
```sql
CREATE OR REPLACE VIEW reporting.most_common_events
AS
SELECT
  event_type,
  COUNT(*) AS event_count,
  EXTRACT(YEAR FROM event_date) AS year,
  EXTRACT(MONTH FROM event_date) AS month
FROM master.user_events
GROUP BY event_type, year, month
ORDER BY event_count DESC;
```
![Screenshot 2023-12-07 at 17 33 05](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/50cf3bdf-ed3c-4805-93c3-2f8e3af1f271)

Task 3 Timeline Analysis

3.1 Plot a timeline showing user signups and their engagement over a period (e.g., number of events per week after signup).
```sql
CREATE OR REPLACE VIEW reporting.number_of_events_per_week_after_signup
AS
WITH UserEngagementTimeline AS (
  SELECT
    ue.user_id,
    ue.event_date,
    DATE_DIFF(DATE(ue.event_date), up.signup_date, WEEK) AS weeks_since_signup,
    COUNT(ue.event_id) AS events_count
  FROM master.user_events ue
  JOIN master.user_profiles up ON ue.user_id = up.user_id
  GROUP BY ue.user_id,  up.signup_date, ue.event_date
)

SELECT
  up.user_id,
  weeks_since_signup,
  IFNULL(SUM(events_count), 0) AS events_count
FROM
  (SELECT DISTINCT user_id, signup_date FROM raw.user_profiles) up
LEFT JOIN
  UserEngagementTimeline uet ON up.user_id = uet.user_id
GROUP BY weeks_since_signup, up.user_id
ORDER BY user_id, weeks_since_signup DESC;
```
![Screenshot 2023-12-07 at 19 23 52](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/adfa40c5-2717-4014-9c9b-d5791a93e50b)


3.2 - Identify any patterns or anomalies in user engagement, such as drop-offs or spikes in activity.
```sql
CREATE OR REPLACE VIEW reporting.spikes_in_activity
AS
WITH UserActivity AS (
  SELECT
    ue.user_id,
    ue.event_id,
    ue.event_date,
    ue.event_type,
    up.signup_date,
    up.last_login_date
  FROM master.user_events ue
  JOIN master.user_profiles up ON ue.user_id = up.user_id
),

CountedUserActivity AS (
SELECT
  DATE_TRUNC(event_date, DAY) AS date,
  COUNT(DISTINCT ua.user_id) AS active_users,
  COUNT(ua.event_id) AS total_events
FROM UserActivity ua
GROUP BY date
ORDER BY date  
)

SELECT
  date,
  active_users,
  total_events,
FROM CountedUserActivity cua
WHERE active_users > total_events OR total_events >= 3 * active_users
GROUP BY date, active_users, total_events
ORDER BY date, active_users, total_events;
```
![Screenshot 2023-12-07 at 17 29 54](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/dc30e731-cf5c-4bed-b5a0-50b1b8ae2738)

TASK 4 Behavioral Analysis

4.1 - Analyze the sequence of event types for users. For instance, do users typically "view" before they "click" or "purchase"?
```sql
CREATE OR REPLACE VIEW reporting.sequence_of_event_types 
AS
WITH UserEventSequence AS (
  SELECT
    ue.user_id,
    ue.event_type,
    ue.event_date,
    LEAD(ue.event_type) OVER (PARTITION BY ue.user_id ORDER BY ue.event_date) AS next_event_type
  FROM master.user_events ue
  ORDER BY ue.user_id, ue.event_date
)

SELECT
  ues.event_type,
  ues.next_event_type,
  COUNT(*) AS event_count
FROM UserEventSequence ues
GROUP BY ues.event_type, ues.next_event_type
ORDER BY event_count DESC;
```
![Screenshot 2023-12-07 at 17 28 23](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/65c06fae-3f60-4efc-8a3e-244e32c84fd6)

4.2 - Determine the most popular pages in the application based on event data.
```sql
CREATE OR REPLACE VIEW reporting.most_popular_pages
AS
WITH UserPageViews AS (
  SELECT
    ue.user_id,
    ue.page,
    COUNT(ue.event_id) AS page_views
  FROM master.user_events ue
  GROUP BY ue.user_id, ue.page
)

SELECT
  upv.page,
  SUM(upv.page_views) AS total_page_views
FROM UserPageViews upv
GROUP BY upv.page
ORDER BY total_page_views DESC;
```
![Screenshot 2023-12-07 at 17 27 28](https://github.com/Lemberg14/user-engagement-analysis-home-exercise/assets/48790931/79cbdc9e-ab05-4479-97d8-130c482309c5)
