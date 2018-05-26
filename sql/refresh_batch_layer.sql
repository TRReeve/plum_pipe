TRUNCATE TABLE dwh.source_countries;
INSERT INTO dwh.source_countries  (
Select

id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.receiver_countries;
INSERT INTO dwh.receiver_countries  (
Select id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.remittance_value;
INSERT INTO dwh.remittance_value  (
Select source_id,receiver_id,amount  remittance_value
FROM load.remittance m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.migration;
INSERT INTO dwh.migration  (
Select source_id,receiver_id,amount  migration_numbers
FROM load.migration m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.net_movements;
INSERT INTO dwh.net_movements  (

Select
sub1.id,
sub1.name,
inflows,
outflows

FROM (

Select
sid.id,
name,
sum(migration_numbers)  outflows

from dwh.migration fact
JOIN dwh.source_countries sid ON fact.source_id = sid.id
WHERE sid.id NOT IN (217,216,217)
Group By 1,2
Order by 3 desc)sub1

JOIN (Select
rid.id,
name,
sum(migration_numbers)  inflows

from dwh.migration fact
JOIN dwh.receiver_countries rid ON fact.receiver_id = rid.id
WHERE rid.id NOT IN (217,216,217)
Group By 1,2)sub2 ON sub1.id = sub2.id);


