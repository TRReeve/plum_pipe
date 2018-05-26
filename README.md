# plum_pipe

The main goal of this implementation is to roughly follow the principles of lambda architecture,be resilient to massive increases in dataloads (aka due to chunking
of insertions it will not suffer any ram failures). There is an immutable layer of CSVs all built off of one source of truth in the country_json files. 
which is fed into an incorruptible load layer (in theory) which would then be recalculated periodically in full so that user errors are rectifiable. 
After this point the reporting layer of views gives us the direct answer to the business questions we want at any particular moment. 
If the data was much larger we could also introduce more tables like the table net movements that could theoretically be done as a view, 
but lends itself to some pre processing before we generate the view (aka splitting the tables and rejoining on common country id. In a production environment
this job would be split into its seperate components with ETL loads happening far more rapidly and batch jobs being run in background without dropping the old table 
till the last possible moment to maximise availability


Assumes a unix system
--Folder Structure


--etl
    -Data
    - Contains the data sources and CSVs

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
