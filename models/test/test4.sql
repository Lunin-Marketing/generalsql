SELECT 
    mql_id,
    mql_date
FROM funnel_report_all_time_mqls
WHERE mql_date >= '2023-08-01'
AND is_current_customer = FALSE