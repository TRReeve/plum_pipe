import os
import argparse
import csv
import json

CURRENT_DIR = os.getcwd()
DATA_DIR = os.path.join(CURRENT_DIR,"data")


def get_country_ids():

    """returns dictionary that maps country id to name"""

    with open("countries.json", 'r') as r:
        jobj = json.load(r)
        return jobj

def get_column_heads(country_id_map,target_csv):

    """removes commas and new lines and lowers case etc 
       and returns column names of top row"""
    # get x axis
    with open(target_csv, 'r') as f:
        column_headers = next(f)
        clean_commas = column_headers.replace(",","")
        remove_new_line = clean_commas.replace("\n","")
        split_to_list = remove_new_line.split(";")[1:]
        column_heads = list(map(lambda x: x.lower(), split_to_list))

        return column_heads

def parse_to_integer(object):

    """ensures objects being passed are integers"""

    remove_commas = object.replace(",","")

    try:
        object = int(remove_commas)
    except:
        print("Warning " + remove_commas + " not a valid integer, value coalesced to 0, check data recommended")
        object = 0
    return object

def get_rows_info(country_id_map,column_heads,target_csv):

    with open(target_csv, 'r') as f:

        next(f)
        rows_object = [['source_id', 'receiver_id', 'amount']]

        # get y axis dimensions

        for line in f:
            counter = 0

            remittance_source = line.split(";")[0].replace(",","").lower()

            if remittance_source in country_id_map:

                for column in line.split(";")[1:]:

                    rows_object.append([country_id_map[remittance_source],
                                        country_id_map[column_heads[counter]],
                                        parse_to_integer(column)])
                    counter = counter + 1





        return rows_object


def write_to_csv(data_object,output_csv):

    with open(output_csv, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data_object:
            writer.writerow(line)

if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    parser.add_argument('--target_csv', help='target table')
    args = parser.parse_args()
    source_file = os.path.join(DATA_DIR, args.source)
    csv_file = os.path.join(DATA_DIR,args.target_csv)

    json_map = get_country_ids()
    column_heads = get_column_heads(json_map,source_file)
    mapped_data = get_rows_info(json_map,column_heads,source_file)
    write_to_csv(mapped_data,csv_file)
    print("\n ---matrix mapped to csv located at {0}---\n".format(csv_file))
