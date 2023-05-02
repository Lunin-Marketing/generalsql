{% snapshot sfdc_ao_instance_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'ao_instance_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'act_on_instance_c') }}

{% endsnapshot %}