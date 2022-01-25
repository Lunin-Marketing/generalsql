{% snapshot sfdc_lead_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'lead_id',
        strategy='timestamp',
        updated_at='last_modified_date',
    )
}}

SELECT *
FROM {{ref('lead_source_xf')}}

{% endsnapshot %}