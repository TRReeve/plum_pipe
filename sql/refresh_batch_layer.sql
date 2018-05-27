TRUNCATE TABLE dwh.countries;
INSERT INTO  dwh.countries  (
Select

id,name
FROM load.countries c

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.remittance_value;
INSERT INTO  dwh.remittance_value  (
Select source_id,receiver_id,amount  remittance_value
FROM load.remittance m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

TRUNCATE TABLE dwh.migration;
INSERT INTO  dwh.migration  (
Select source_id,receiver_id,amount  migration_numbers
FROM load.migration m
JOIN load.countries c ON m.source_id = c.id

Group By 1,2
Order By 1,2);

CREATE INDEX ON dwh.migration (source_id);
CREATE INDEX ON dwh.migration (receiver_id);
CREATE INDEX ON dwh.remittance_value (source_id);
CREATE INDEX ON dwh.remittance_value (receiver_id);


