{% snapshot sfdc_account_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'account_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
    
}}

SELECT DISTINCT
account_source_xf.*
FROM {{ref('account_source_xf')}}

{% endsnapshot %}