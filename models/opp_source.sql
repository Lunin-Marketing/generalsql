select *
from {{ ref('opportunity_source2') }}
limit 10