{% snapshot sfdc_contact_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'contact_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT DISTINCT
contact_source_xf.*
FROM {{ref('contact_source_xf')}}
WHERE contact_id != '00314000029F6xxAAC'

{% endsnapshot %}