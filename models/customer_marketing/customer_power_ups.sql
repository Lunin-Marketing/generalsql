{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        campaign_member.campaign_id,
        campaign.campaign_name,
        campaign_member.lead_or_contact_id,
        campaign_member.campaign_member_id,
        campaign_member.campaign_member_status,
        campaign_member.campaign_member_created_date,
        person.lean_data_account_id AS account_id,
        person.created_date,
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
        COALESCE(account.company_size_rev,person.company_size_rev) AS company_size_rev,
        COALESCE(account.segment,person.segment) AS segment,
        account.account_type,
        account.is_current_customer,
        campaign_influence.influence_opportunity_id AS opportunity_id,
        opp.acv_deal_size_usd,
        opp.opportunity_name,
        opp.is_won,
        CASE 
            WHEN is_won = true THEN influence_opportunity_id
            END AS won_opportunity_id,
        CASE 
            WHEN is_won = true THEN acv_deal_size_usd
            END AS won_opportunity_acv
FROM {{ref('campaign_member_source_xf')}} campaign_member
LEFT JOIN {{ref('campaign_source_xf')}} campaign
ON campaign_member.campaign_id=campaign.campaign_id
LEFT JOIN {{ref('person_source_xf')}} person
ON campaign_member.lead_or_contact_id=person.person_id
LEFT JOIN {{ref('account_source_xf')}} account
ON person.lean_data_account_id=account.account_id
LEFT JOIN {{ref('campaign_influence_xf')}} campaign_influence
ON campaign_member.campaign_id=campaign_influence.influence_campaign_id
AND campaign_influence.influence_contact_id=person.person_id
LEFT JOIN {{ref('opp_source_xf')}} opp
ON campaign_influence.influence_opportunity_id=opp.opportunity_id
WHERE campaign.parent_campaign_id LIKE '7015Y000002UBBi%'
-- WHERE campaign.parent_campaign_id LIKE '7015Y000003jbcvQAA%'

AND campaign_member_status NOT IN ('Initial','Sent')
)

SELECT *
FROM base