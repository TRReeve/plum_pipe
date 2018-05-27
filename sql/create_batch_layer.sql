DROP SCHEMA IF EXISTS dwh CASCADE;
CREATE SCHEMA dwh;

CREATE TABLE dwh.countries as (
Select

id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

CREATE TABLE dwh.remittance_value as (
Select source_id,receiver_id,amount as remittance_value
FROM load.remittance m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

CREATE TABLE dwh.migration as (
Select source_id,receiver_id,amount as migration_numbers
FROM load.migration m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

CREATE INDEX ON dwh.migration (source_id);
CREATE INDEX ON dwh.migration (receiver_id);
CREATE INDEX ON dwh.remittance_value (source_id);
CREATE INDEX ON dwh.remittance_value (receiver_id);


