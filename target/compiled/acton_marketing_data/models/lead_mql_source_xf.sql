

SELECT
    person_id,
    email,
    is_hand_raiser,
    mql_created_date,
    mql_most_recent_date,
    working_date,
    person_status,
    country,
    company,
    first_name,
    last_name,
    title,
    person_owner_id,
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    industry_bucket,
    channel_bucket
FROM "acton"."dbt_actonmarketing"."person_source_xf"
WHERE mql_most_recent_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
--AND lead_source = 'Marketing'