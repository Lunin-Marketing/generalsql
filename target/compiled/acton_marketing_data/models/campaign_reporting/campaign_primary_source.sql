

WITH base AS (

    SELECT DISTINCT
    --Campaign Info
        campaign_member.campaign_id,
        campaign.campaign_name,
        campaign.campaign_start_date,
        campaign.campaign_end_date,
        campaign.budgeted_cost,
        campaign.actual_cost,
        campaign.campaign_type,
        campaign.campaign_status,
        campaign.is_active_campaign,

    --Parent Campaign Info
        campaign.parent_campaign_id,
        campaign.parent_campaign_name,
        campaign.parent_campaign_start_date,
        campaign.parent_campaign_end_date,
        campaign.parent_budgeted_cost,
        campaign.parent_actual_cost,
        campaign.parent_campaign_type,
        campaign.parent_campaign_status,
        campaign.is_active_parent_campaign,

    --Campaign Member Info
        campaign_member.lead_or_contact_id,
        campaign_member.campaign_member_id,
        campaign_member.campaign_member_status,
        campaign_member.campaign_member_type,
        campaign_member.campaign_member_has_responded,
        campaign_member.campaign_member_created_date,
        campaign_member.campaign_member_first_responded_date,

    --Person Info
        person.email,
        person.person_status,
        person.lead_score,
        person.mql_most_recent_date,
        person.working_date,

    --Account Info
        person.lean_data_account_id AS account_id,
        COALESCE(account.account_name,person.company) AS account_name,
        account.account_owner_name,
        account.account_csm_name,
        COALESCE(account.global_region,person.global_region) AS global_region,
        account.executive_sponsor,
        account.assigned_account_tier,
        account.customer_since,
        account.renewal_date,
        account.business_model,
        account.contract_type,
        account.delivered_support_tier,
        COALESCE(account.industry,person.industry) AS industry,
        COALESCE(account.industry_bucket,person.industry_bucket) AS industry_bucket,
        COALESCE(account.company_size_rev,person.company_size_rev) AS company_size_rev,
        COALESCE(account.segment,person.segment) AS segment,
        account.account_type,
        account.is_current_customer,

    --Opportunity Info
        campaign_influence.influence_opportunity_id AS opportunity_id,
        opp.opportunity_name,
        opp.acv AS acv_deal_size_usd,
        opp.created_date,
        opp.discovery_date,
        opp.close_date,
        opp.type AS opp_type,
        opp.is_closed,
        opp.is_won,
        CASE 
            WHEN is_won = true THEN influence_opportunity_id
            END AS won_opportunity_id,
        CASE 
            WHEN is_won = true THEN acv
            END AS won_opportunity_acv
FROM "acton"."dbt_actonmarketing"."campaign_member_source_xf" campaign_member
LEFT JOIN "acton"."dbt_actonmarketing"."campaign_source_xf" campaign
ON campaign_member.campaign_id=campaign.campaign_id
LEFT JOIN "acton"."dbt_actonmarketing"."person_source_xf" person
ON campaign_member.lead_or_contact_id=person.person_id
LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" account
ON person.lean_data_account_id=account.account_id
LEFT JOIN "acton"."dbt_actonmarketing"."campaign_influence_xf" campaign_influence
ON campaign_member.campaign_id=campaign_influence.influence_campaign_id
AND campaign_influence.influence_contact_id=person.person_id
LEFT JOIN "acton"."dbt_actonmarketing"."opp_source_xf" opp
ON campaign_influence.influence_opportunity_id=opp.opportunity_id

)

SELECT *
FROM base