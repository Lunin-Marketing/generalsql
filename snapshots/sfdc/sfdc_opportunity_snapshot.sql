{% snapshot sfdc_opportunity_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'opportunity_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'opportunity') }}

{% endsnapshot %}