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
WHERE account_id NOT IN ('0011400001aqWNkAAM','0013000000vZhqOAAS')

{% endsnapshot %}