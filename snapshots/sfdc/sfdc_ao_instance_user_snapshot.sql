{% snapshot sfdc_ao_instance_user_snapshot %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'ao_user_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'act_on_instance_user_c') }}

{% endsnapshot %}