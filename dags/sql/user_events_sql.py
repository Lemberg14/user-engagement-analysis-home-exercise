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