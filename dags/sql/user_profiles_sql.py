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