SELECT
    person_id,
    channel_bucket_lt
FROM {{ref('person_source_xf')}}
WHERE DATE_TRUNC('month',mql_most_recent_date) = '2023-03-01'