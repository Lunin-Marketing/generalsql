{% snapshot sfdc_account_snapshot2 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'unique_account_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
    
}}

SELECT DISTINCT
account_source_xf.*
FROM {{ source('salesforce', 'account') }}
-- WHERE account_id NOT IN ('0011400001aqWNkAAM','0013000000vZhqOAAS','0011O000023pnveQAA','0013000000XBbreAAD')

{% endsnapshot %}