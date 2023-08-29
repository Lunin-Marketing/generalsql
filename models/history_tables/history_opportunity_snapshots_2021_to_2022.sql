SELECT
    id AS opportunity_id,
    account_id,
    name AS opportunity_name,
    stage_name,
    amount,
    close_Date,
    type,
    lead_source,
    is_closed,
    is_won,
    owner_id,
    created_date,
    marketing_channel_c AS marketing_channel,
    acv_deal_size_usd_stamp_c AS acv_deal_size_usd_stamp,
    dbt_valid_from
FROM {{ref('sfdc_opportunity_snapshots')}}
WHERE 1=1
AND (dbt_valid_from LIKE '2021%'
OR dbt_valid_from LIKE '2022%')