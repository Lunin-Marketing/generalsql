{% snapshot sfdc_contract_snapshot2 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'contract_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ref('contract_source_xf')}}

{% endsnapshot %}