{% snapshot sfdc_ao_instance_user_snapshots %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'act_on_instance_user_c') }}

{% endsnapshot %}