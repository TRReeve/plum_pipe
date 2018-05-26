# plum_pipe

Folder Structure

Data
    - Contains the data sources and CSVs

ETL

    - matrix_to_relational.py: Parses the matrix stored data in a CSV into a relational form.
    - ingest_csv.py takes a csv file under the argument --source and inserts to a table --target_table

SQL 
    -Contains SQLs for creating the dwh model


TO CREATE DB AND TABLES

./init_database

TO RUN WHOLE ETL JOB AS ONE PROCESS

./run_etl

TO RUN TEST ON MISMATCHED SCHEMAS

./run_schema_tesh
