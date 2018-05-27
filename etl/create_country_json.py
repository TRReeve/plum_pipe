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


def dump_json_file(data,outfile):

    """"....dumps a json file?..."""

    with open(outfile + '.json','w') as write:
        json.dump(data,write)

def json_to_csv_dump(json_data,outfile,columns):

    """converts JSON to csv file format with column headers defined
      in arguments and written in first row"""

    with open(os.path.join(DATA_DIR,outfile + '.csv'), 'w') as f:
        writer = csv.writer(f)

        writer.writerow(columns)
        for key, value in json_data.items():
            writer.writerow([value, key])


if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    parser.add_argument('--outfile',default='countries',help='default output name')
    args = parser.parse_args()
    countries_json = map_country_ids(os.path.join(DATA_DIR,args.source))
    dump_json_file(countries_json,args.outfile)

    #columns to go into the json file
    columns = ['id','name']
    json_to_csv_dump(countries_json,args.outfile,columns)
