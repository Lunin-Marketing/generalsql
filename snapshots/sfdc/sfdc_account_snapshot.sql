{% snapshot sfdc_account_snapshot3 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'account_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ref('account_source_xf')}}

{% endsnapshot %}