{% snapshot sfdc_opportunity_snapshot2 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'opportunity_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ref('opp_source_xf')}}
LIMIT 10000

{% endsnapshot %}