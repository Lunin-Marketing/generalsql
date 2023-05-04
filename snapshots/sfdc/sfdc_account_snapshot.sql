{% snapshot sfdc_account_snapshots %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
    
}}

SELECT *
FROM {{ source('salesforce', 'account') }}
-- WHERE account_id NOT IN ('0011400001aqWNkAAM','0013000000vZhqOAAS','0011O000023pnveQAA','0013000000XBbreAAD')

{% endsnapshot %}