{% snapshot sfdc_opportunity_snapshots %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'opportunity') }}

{% endsnapshot %}