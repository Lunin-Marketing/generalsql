{% snapshot sfdc_account_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'account_id',
        strategy='timestamp',
        updated_at='last_modified_date',
    )
}}

SELECT *
FROM {{ref('account_source_xf')}}

{% endsnapshot %}