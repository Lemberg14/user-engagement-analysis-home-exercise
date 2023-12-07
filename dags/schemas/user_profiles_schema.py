user_profiles_schema_raw = [
    {"name": "user_id", "type": "INTEGER"},
    {"name": "signup_date", "type": "DATE"},
    {"name": "last_login_date", "type": "DATE"},
]

user_profiles_schema_master = [
    {"name": "user_id", "type": "INTEGER", "description": "id of the user"},
    {"name": "signup_date", "type": "DATE", "description": "signup date of the user"},
    {"name": "last_login_date", "type": "DATE", "description": "last login date of the user"},
    {"name": "engagement duration", "type": "DATE", "description": "time between signup and last login"},
    {"name": "etl_batch_date", "type": "DATE", "description": "airflow run date"},
]