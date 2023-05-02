{% snapshot sfdc_contract_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'contract_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'contract') }}

{% endsnapshot %}