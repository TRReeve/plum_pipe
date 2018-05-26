import os
import argparse
import csv
import psycopg2
from psycopg2 import sql
import time
import getpass

start_time = time.clock()
CURRENT_DIR = os.getcwd()
DATA_DIR = os.path.join(CURRENT_DIR,"data")
dbname = "plum_pipe"
username = getpass.getuser()

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

def get_primary_key(schema,table):

    """
    get name of primary key for relevant load table
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(sql.SQL("""Select constraint_name from information_schema.table_constraints 
    where table_schema = {0}
    and table_name = {1}
    and constraint_type = 'PRIMARY KEY'""")
                .format(sql.Literal(schema),
                        sql.Literal(table)))
    query_return = cur.fetchall()
    conn.close()

    # return result
    primary_key = query_return[0][0]

    return primary_key


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
    generator for inserting data from potentially memory overloading 
    csv file, async is slightly overkill but could be another approach
    """

    #how many lines to be held in memory at a time maximum, trade off
    #between overall throughput, RAM useage and write speed on postgres
    #returns started to diminish > 20k rows so backed off to 20k
    max_chunk_size = 20000

    #insertion counter
    inserted = 0

    with open(data_source,'r') as f:

        csvline = next(f)
        

        chunkholder = []

        for line in f:

            #create tuple from csv line split to insert
            chunkholder.append((tuple(line.split(","))))

            """waits for list to reach a certain size before 
            inserting and clearing list, avoids RAM overflows and large inserts"""

            if len(chunkholder) == max_chunk_size:

                result = insert_to_table(chunkholder, schema, target, table_columns)
                inserted = inserted + int(result)
                # empties list object while keeping variable allocated
                chunkholder.clear()

        #insert remainder of chunkholder in reaching end of csv if it hasnt met max size
        if len(chunkholder) > 0:
            result = insert_to_table(chunkholder, schema, target, table_columns)
            inserted = inserted + int(result)
            chunkholder.clear()


        return inserted


def insert_to_table(data_object,schema,table,table_columns):

    """inserts batch tuples and data to corresponding load layer table"""

    conn = get_connection()
    cur = conn.cursor()
    primary_key = get_primary_key(schema, table)


    cur.execute(sql.SQL("INSERT INTO {0}.{1} ({2}) VALUES {3} "
                        "ON CONFLICT ON CONSTRAINT {4} DO NOTHING").format(sql.Identifier(schema),
                                                                sql.Identifier(table),
                                                                sql.SQL(', ').join(map(sql.Identifier, table_columns)),
                                                                sql.SQL(',').join(map(sql.Literal, data_object)),
                                                                           sql.Identifier(primary_key)))
    result = tuple(cur.statusmessage.split(" "))
    conn.commit()
    conn.close()

    return result[2]


if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    parser.add_argument('--target_schema',default="load", help='target schema')
    parser.add_argument('--target_table', help='target table')
    args = parser.parse_args()

    csv_file = os.path.join(CURRENT_DIR, args.source)
    csv_columns = infer_csv_columns(csv_file)
    table_columns = infer_table_schema(args.target_schema,args.target_table)

    if sorted(csv_columns) == sorted(table_columns):
        insert_count = process_csv(csv_file,args.target_schema,args.target_table,table_columns)
        print("\n ---{0} records inserted from {1} to {2}.{3} in {4} seconds---\n".format(insert_count, args.source,args.target_schema, args.target_table,
                                                                      time.clock() - start_time))

    else:
        print("USER ERROR \n schema of target table and insert csv are not matching, create or alter columns as necessary")
        print("csv schema = {0}".format(csv_columns))
        print("table_schema = {0}".format(table_columns))



