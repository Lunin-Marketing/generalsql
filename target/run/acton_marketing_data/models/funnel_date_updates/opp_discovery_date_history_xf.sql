

  create  table "acton"."dbt_actonmarketing"."opp_discovery_date_history_xf__dbt_tmp"
  as (
    

WITH base AS (

    SELECT 
        opportunity_history_xf.opportunity_id,
        opportunity_history_xf.field_modified_at,
        opportunity_history_xf.field,
        opportunity_history_xf.old_value,
        opportunity_history_xf.new_value
    FROM "acton"."dbt_actonmarketing"."opportunity_history_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."opp_source_xf" ON 
    opportunity_history_xf.opportunity_id=opp_source_xf.opportunity_id
    WHERE discovery_date IS null
    AND type IN ('New Business','UpSell','Renewal')
    AND field_modified_at >= '2021-01-01'

), final AS (

    SELECT
        opportunity_id,
        old_value,
        new_value,
        field_modified_at,
        ROW_NUMBER() OVER(PARTITION BY opportunity_id ORDER BY field_modified_at) AS event_number
    FROM base 
    WHERE field = 'StageName'
    AND new_value = 'Discovery'
    ORDER BY opportunity_id,field_modified_at

)

SELECT 
    opportunity_id,
    old_value,
    new_value,
    field_modified_at
FROM final
WHERE event_number = 1
  );