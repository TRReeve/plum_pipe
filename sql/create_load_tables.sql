DROP SCHEMA IF EXISTS load CASCADE;
CREATE SCHEMA load;

DROP TABLE IF EXISTS load.migration;
CREATE TABLE load.migration
(source_id integer,
receiver_id integer,
amount bigint,
CONSTRAINT migration_pkey PRIMARY KEY (source_id,receiver_id));

DROP TABLE IF EXISTS load.remittance;
CREATE TABLE load.remittance
(source_id integer,
receiver_id integer,
amount bigint,
CONSTRAINT remittances_pkey PRIMARY KEY (source_id,receiver_id));

DROP TABLE IF EXISTS load.countries;
CREATE TABLE load.countries (
id integer,
name text,
CONSTRAINT countries_pkey PRIMARY KEY (id));
