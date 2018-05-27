DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_migration_destinations;
CREATE MATERIALIZED VIEW dwh.v_top10_migration_destinations AS (

Select
name,
sum(case when source_id = 217 THEN migration_numbers END) as immigrants

from dwh.migration fact
JOIN dwh.receiver_countries rid ON fact.receiver_id = rid.id
WHERE rid.id NOT IN (217)
Group By 1
Order by 2 desc
limit 10);

DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_migration_origins;
CREATE MATERIALIZED VIEW dwh.v_top10_migration_origins AS (

Select
name,
sum(CASE WHEN receiver_id = 217 THEN migration_numbers END) as total_emigrants
from dwh.migration fact
JOIN dwh.source_countries sid ON fact.source_id = sid.id
WHERE sid.id NOT IN (217)
Group By 1
Order by 2 desc
limit 10);




DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_country_to_country_migration;
CREATE MATERIALIZED VIEW dwh.v_top10_country_to_country_migration as (

Select
sid.name as source_country,
rid.name as receiving_country,
migration_numbers as migration_flows
from dwh.migration fact
JOIN dwh.source_countries sid ON fact.source_id = sid.id
JOIN dwh.receiver_countries rid ON fact.receiver_id = rid.id

WHERE rid.id NOT IN (217)
AND sid.id NOT IN (217)

Order By 3 desc
Limit 10);

DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_country_to_country_remittances;
CREATE MATERIALIZED VIEW dwh.v_top10_country_to_country_remittances as (

Select
sid.name as source_country,
rid.name as receiving_country,
remittance_value as remittances_millions
from dwh.remittance_value fact
JOIN dwh.source_countries sid ON fact.source_id = sid.id
JOIN dwh.receiver_countries rid ON fact.receiver_id = rid.id

WHERE rid.id NOT IN (217)
AND sid.id NOT IN (217)

Order By 3 desc
Limit 10);

DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_net_inflows;
CREATE MATERIALIZED VIEW dwh.v_top10_net_inflows as (

Select
name,
inflows - outflows as net_flow

FROM dwh.net_movements
order by 2 desc
limit 10);

DROP MATERIALIZED VIEW IF EXISTS dwh.v_top10_net_outflows;
CREATE MATERIALIZED VIEW dwh.v_top10_net_outflows as (

Select
name,
inflows -outflows as net_flow

FROM dwh.net_movements
order by 2 asc
limit 10)
