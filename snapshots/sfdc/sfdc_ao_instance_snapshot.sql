{% snapshot sfdc_ao_instance_snapshot2 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'ao_instance_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ref('ao_instance_source_xf')}}

{% endsnapshot %}