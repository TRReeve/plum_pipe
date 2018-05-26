import os
import argparse
import json
import csv

CURRENT_DIR = os.getcwd()
DATA_DIR = os.path.join(CURRENT_DIR,"data")


def map_country_ids(target_csv):
    """takes the dataset and maps country names to an id field
    ensuring future consistency"""

    countries_dict = {}

    with open(target_csv, 'r') as f:
        column_headers = next(f)
        clean_commas = column_headers.replace(",","")
        remove_new_line = clean_commas.replace("\n","")
        split_to_list = remove_new_line.split(";")
        lowered = list(map(lambda x: x.lower(), split_to_list))[1:]

        for country,id in enumerate(lowered,1):
            countries_dict[id] = country

    return countries_dict


def dump_json_file(data):

    with open('countries.json','w') as write:
        json.dump(data,write)

def dump_csv_file(data):


    try:
        with open(os.path.join(DATA_DIR,'countries.csv'), 'w') as f:
            writer = csv.writer(f)

            writer.writerow(['id','name'])
            for key, value in data.items():
                writer.writerow([value, key])
    except IOError:
        print("I/O error", csv_file)
    return



if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    args = parser.parse_args()
    countries_json = map_country_ids(os.path.join(DATA_DIR,args.source))
    dump_json_file(countries_json)
    dump_csv_file(countries_json)
