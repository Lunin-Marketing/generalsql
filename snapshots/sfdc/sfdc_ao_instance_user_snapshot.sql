{% snapshot sfdc_ao_instance_user_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'ao_user_id',
        strategy='timestamp',
        updated_at='systemmodstamp',
    )
}}

SELECT *
FROM {{ref('ao_instance_user_source_xf')}}

{% endsnapshot %}