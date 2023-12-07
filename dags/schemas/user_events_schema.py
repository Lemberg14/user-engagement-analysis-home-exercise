user_events_schema_raw = [
    {"name": "event_id", "type": "INTEGER"},
    {"name": "user_id", "type": "INTEGER"},
    {"name": "event_type", "type": "STRING"},
    {"name": "event_date", "type": "DATETIME"},
    {"name": "page", "type": "STRING"},
]

user_profiles_schema_master = [
    {"name": "event_id", "type": "INTEGER", "description": "id of the event"},
    {"name": "user_id", "type": "INTEGER", "description": "id of the user"},
    {"name": "event_type", "type": "STRING", "description": "event type name"},
    {"name": "event_date", "type": "DATETIME", "description": "datetime of the event"},
    {"name": "page", "type": "STRING", "description": "place of the event"},
    {"name": "etl_batch_date", "type": "DATE", "description": "airflow run date"},
]