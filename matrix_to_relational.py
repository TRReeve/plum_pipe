import os
import argparse
import csv

CURRENT_DIR = os.getcwd()

"""
Takes semicolon delimited csv files in a matrix form and converts to relational data
"""


def map_matrix(target_csv):

    """takes first row and maps against y axis and corresponding value and returns data object"""



    with open(target_csv, 'r') as f:

        columns = next(f)
        clean_commas = columns.replace(",","")
        remove_new_line = columns.replace("\n","")
        split_to_list = remove_new_line.split(";")

        y_axis = [['source','receiver','amount']]
        for line in f:
            counter = 0
            # create tuple from csv line split to insert
            remittance_source = line.split(";")[0]
            for column in line.split(";"):
                y_axis.append([remittance_source.lower(),split_to_list[counter].lower(),column.replace(",","").replace("\n","")])
                counter = counter + 1

        for x in y_axis:
            print(x)


        return y_axis


def write_to_csv(data_object,output_csv):


    with open( output_csv, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for line in data_object:
            writer.writerow(line)


if __name__ == '__main__':

    # create parser object
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',help='comma seperated data source')
    parser.add_argument('--target_csv', help='target table')
    args = parser.parse_args()
    source_file = os.path.join(CURRENT_DIR, args.source)
    csv_file = os.path.join(CURRENT_DIR,args.target_csv)

    print("parsing csv")
    mapped_data = map_matrix(source_file)
    write_to_csv(mapped_data,csv_file)