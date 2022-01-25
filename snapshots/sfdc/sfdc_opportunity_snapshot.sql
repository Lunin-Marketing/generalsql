{% snapshot sfdc_opportunity_snapshot %}

{{
    config (
        unique_key = 'opportunity_id',
        strategy='timestamp',
        updated_at='last_modified_date',
    )
}}

SELECT *
FROM {{ref('opp_source_xf')}}

{% endsnapshot %}