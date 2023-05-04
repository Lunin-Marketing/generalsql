{% snapshot sfdc_contact_snapshots %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
}}

SELECT *
FROM {{ source('salesforce', 'contact') }}
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY null) = 1
-- WHERE contact_id NOT IN ('00314000029F6xxAAC','00314000029Ev7wAAC','0035Y00005BUtF2QAL','00314000029FngnAAC')

{% endsnapshot %}