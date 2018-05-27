DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;
DROP TABLE IF EXISTS dwh.source_countries;
CREATE TABLE dwh.source_countries as (
Select

id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

DROP TABLE IF EXISTS dwh.receiver_countries;
CREATE TABLE dwh.receiver_countries as (
Select id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

DROP TABLE IF EXISTS dwh.remittance_value;
CREATE TABLE dwh.remittance_value as (
Select source_id,receiver_id,amount as remittance_value
FROM load.remittance m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

DROP TABLE IF EXISTS dwh.migration;
CREATE TABLE dwh.migration as (
Select source_id,receiver_id,amount as migration_numbers
FROM load.migration m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

DROP TABLE IF EXISTS dwh.net_movements;
CREATE TABLE dwh.net_movements as (

Select
sub1.id,
sub1.name,
inflows,
outflows

FROM (

Select
sid.id,
name,
sum(CASE WHEN receiver_id NOT IN (217) THEN migration_numbers END) as outflows

from dwh.migration fact
JOIN dwh.source_countries sid ON fact.source_id = sid.id
WHERE sid.id NOT IN (217)
Group By 1,2
Order by 3 desc)sub1

JOIN (Select
rid.id,
name,
sum(CASE WHEN source_id NOT IN (217) THEN migration_numbers END) as inflows

from dwh.migration fact
JOIN dwh.receiver_countries rid ON fact.receiver_id = rid.id
WHERE rid.id NOT IN (217)
Group By 1,2)sub2 ON sub1.id = sub2.id);


CREATE INDEX ON dwh.migration (source_id);
CREATE INDEX ON dwh.migration (receiver_id);
CREATE INDEX ON dwh.remittance_value (source_id);
CREATE INDEX ON dwh.remittance_value (receiver_id);


