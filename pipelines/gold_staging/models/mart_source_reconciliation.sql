-- Business question: which source observations contributed, and which remain separate?
-- Grain: source x accounting status. Never interpret source rows as distinct stores.
select source, accounting_status, count(*) as source_observation_count
from {{ ref('source_accounting') }}
group by source, accounting_status
