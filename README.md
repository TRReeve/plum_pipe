# plum_pipe

The main goal of this implementation is to roughly follow the principles of lambda architecture (sans Speed/Streaming Layer) ,be resilient to massive increases in dataloads (aka chunking
of insertions to avoid any chance of topping out memory) and maximise availability and throughput while still providing useful calculations and aggregations

There is an immutable layer of CSVs all built off of one source of truth in the country_json files. 
which is fed into an incorruptible load layer (in theory) which would then be recalculated periodically in full so that user errors and shitty data inputs are 
rectifiable. After this point the reporting layer of materalized views caches the data (until refreshed by a new batch generation) that then gives us the 
direct answer to the business questions we want at any particular moment. 

If the data was much larger we could also introduce more tables like the table net 
movements that could theoretically be done as a materalized view as well, the role of this table  lends itself to some pre processing before we generate the 
view (aka splitting the tables and rejoining on common country id. 

In a production environment this monolithic etl job would be split into its seperate components 
with ETL loads happening far more rapidly and batch jobs being run in background without dropping the old table till the last possible moment to maximise 
availability rather than the simplistic truncate and recalulate approach here.


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
