{% snapshot sfdc_contact_snapshot2 %}

{{
    config (
        target_schema='snapshots',
        unique_key = 'unique_contact_id',
        strategy='timestamp',
        updated_at='systemmodstamp'
    )
}}

SELECT
contact_source_xf.*
FROM {{ source('salesforce', 'contact') }}
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY null) = 1
-- WHERE contact_id NOT IN ('00314000029F6xxAAC','00314000029Ev7wAAC','0035Y00005BUtF2QAL','00314000029FngnAAC')

{% endsnapshot %}