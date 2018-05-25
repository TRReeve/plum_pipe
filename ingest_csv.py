import os
import argparse
import csv
import psycopg2
from psycopg2 import sql

CURRENT_DIR = os.getcwd()
dbname = 'plum_pipe'
username = 'tom'

def get_connection():

    """
    get db connection
    """
    conn = psycopg2.connect(dbname=dbname,user=username)

    return conn


def infer_table_schema(schema,table):

    """
    returns a list of column names in the target load table
    """
    conn = get_connection()
    cur = conn.cursor()

    # construct dynamic SQL query to get columns
    cur.execute(sql.SQL("""Select column_name 
    from information_schema.columns 
    where table_name = {0} AND table_schema = {1}""")
                .format(sql.Literal(table),
                        sql.Literal(schema)))
    query_return = cur.fetchall()
    conn.close()

    # convert returned tuples to list
    table_schema = [i[0] for i in query_return]

    return table_schema


def infer_csv_columns(target_csv):

    """
    returns the top row of the csv file as a list to be compared with the table schema
    """

    with open(target_csv,newline='\n',encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        csv_schema = next(reader)

    # convert csv headers to lower case
    lowered = list(map(lambda x : x.lower(),csv_schema))

    return lowered


def process_csv(data_source,schema,target,table_columns):
    """
    generator for inserting data from csv file, async is
    slightly overkill but could be another approach
    """

    #how many lines we'll hold in memory
    max_chunk_size = 1000

    with open(data_source,'r') as f:

        next(f)

        chunkholder = []

        for line in f:

            #create tuple from csv line split to insert
            chunkholder.append((tuple(line.split(","))))

            """waits for list to reach a certain size before 
            inserting and clearing list, avoids RAM overflows"""

            if len(chunkholder) > max_chunk_size:

                insert_to_table(chunkholder,schema,target,table_columns)
                # empties list object while keeping variable allocated
                chunkholder.clear()

def insert_to_table(data_object,schema,table,table_columns):

    conn = get_connection()
    cur = conn.cursor()


    cur.execute(sql.SQL("INSERT INTO {}.{} ({}) VALUES {} ").format(sql.Identifier(schema),
                                                                sql.Identifier(table),
                                                                sql.SQL(', ').join(map(sql.Identifier, table_columns)),
                                                                sql.SQL(',').join(map(sql.Literal, data_object))))

    conn.commit()
    conn.close()




if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    parser.add_argument('--target_table', help='target table')
    args = parser.parse_args()
    csv_file = os.path.join(CURRENT_DIR, args.source)

    # assuming all loads go to this schema
    load_schema = "load"
    target_table = args.target_table

    csv_columns = infer_csv_columns(csv_file)
    table_columns = infer_table_schema(load_schema,target_table)

    if sorted(csv_columns) == sorted(table_columns):
        print("schemas match")
        process_csv(csv_file,load_schema,target_table,table_columns)

    else:
        print("schema of target table and insert csv are not matching, create or alter columns as necessary")
