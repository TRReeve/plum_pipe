# plum_pipe


NOTE: Deployment Assumes a unix system with a postgres cluster available to the user through psql.

--Folder Structure


--etl
    -Data
    - Contains the data sources and CSVs (once generated by running the ETL process)

    - matrix_to_relational.py: Parses the matrix stored data in a CSV into a relational form.
    - ingest_csv.py takes a csv file under the argument --source and inserts to a table --target_table
    
--sql
    -Contains SQLs for creating the dwh model


INSTRUCTIONS

--TO CREATE DB AND TABLES

./init_database

--TO RUN WHOLE ETL JOB AS ONE PROCESS AND CREATE CSVS

./run_etl

--TO RUN TEST ON MISMATCHED SCHEMAS

./run_schema_tesh

--TO GET BACK THE REPORTS CONTAINING ALL ANSWERS TO QUESTIONS

./get_reports
