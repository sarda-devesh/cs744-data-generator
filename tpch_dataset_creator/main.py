import os
import json
import argparse
import subprocess
import shutil
import dask.dataframe as dd

from encode import *
from create_columnar import *
from special_cols_creator import *

def generate_data(args, tables_dir, schema):
    # Check if we need to generate the data
    if os.path.exists(tables_dir) and not args.overwrite:
        return

    # Create the table
    os.chdir("dbgen")
    subprocess.check_output("make", shell = True)
    command_to_run = f"./dbgen -s {args.scale_factor} -f"
    subprocess.check_output(command_to_run, shell = True)

    # Move the data to the dir
    os.makedirs(tables_dir, exist_ok = True)
    move_command = f"mv *.tbl {tables_dir}"
    subprocess.check_output(move_command, shell = True)

    # Get the clean tables
    tables_parent_dir = os.path.dirname(tables_dir)
    tables_dir_name = os.path.basename(tables_dir)
    clean_dir_name = "clean_" + tables_dir_name
    clean_tables_dir = os.path.join(tables_parent_dir, clean_dir_name)
    if os.path.exists(clean_tables_dir):
        shutil.rmtree(clean_tables_dir)
    os.makedirs(clean_tables_dir, exist_ok = True)
    
    for table_name in schema:
        if "_" in table_name:
            continue
        
        # Load the table
        table_read_path = os.path.join(tables_dir, table_name + ".tbl")
        col_names = schema[table_name]["columns"] + ["empty"]
        table_df = dd.read_csv(table_read_path, delimiter = "|", header = None, names = col_names)
        
        # Remove the last column and write it
        table_df = table_df.drop(columns=["empty"])
        table_write_path = os.path.join(clean_tables_dir, table_name + ".tbl")
        table_df.to_csv(table_write_path, single_file = True, sep = "|", index = False, header = False)
        print("Writing table", table_name)
    
    # Swap the clean
    shutil.rmtree(tables_dir)
    os.rename(clean_tables_dir, tables_dir)

def main(args):
    # Load the schema
    os.makedirs(args.save_dir, exist_ok = True)
    with open("schema.json", "r") as reader:
        schema = json.load(reader)
        
    # Generate the tables
    tables_dir = os.path.join(args.save_dir, "tables")
    generate_data(args, tables_dir, schema)

    # Encode the tables
    encoding_dir =  os.path.join(args.save_dir, "encoded_tables")
    encode_maps_dir = os.path.join(args.save_dir, "encoded_maps")
    perform_encoding(schema, tables_dir, encoding_dir, encode_maps_dir, args.overwrite)

    # Convert to columnar
    columnar_dir = os.path.join(args.save_dir, "all_columns")
    create_columnar(schema, encoding_dir, columnar_dir, args.overwrite)
    create_special_cols(schema, tables_dir, columnar_dir, args.overwrite)

def read_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--save_dir", type=str, help="The directory to save the results to", required=True)
    parser.add_argument("--overwrite", action='store_true', help="Overwrite if results already exists")
    parser.add_argument("--scale_factor", type=int, default = 1, help="The SF of the data to generate")
    return parser.parse_args()

if __name__ == "__main__":
    main(read_args())