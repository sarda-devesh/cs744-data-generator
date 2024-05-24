import struct
import sys
import json
import os
import humanfriendly

from utils import *

def convert_table_to_columnar(table_schema, table_name, encoded_dir, columnar_dir):
    table_path = os.path.join(encoded_dir, table_name + ".tbl")
    table_cols = table_schema["columns"]
    table_df = dd.read_csv(table_path, delimiter="|", header=None, names=table_cols)
    table_df = table_df.repartition(npartitions=os.cpu_count() - 1)

    for col_name in table_cols:
        # Format the column
        column_type = table_schema[col_name]
        if "decimal" in column_type:
            table_df[col_name] = table_df[col_name].astype('float32')
        else:
            table_df[col_name] = table_df[col_name].astype('int32')
        
        # Save the column
        save_path = os.path.join(columnar_dir, table_name + "_" + col_name)
        save_col_to_file(table_df[col_name], save_path)

def create_columnar(schema, encoded_dir, columnar_dir, overwrite):
    if os.path.exists(columnar_dir) and not overwrite:
        return
    os.makedirs(columnar_dir, exist_ok = True)

    for table_name in schema:
        if "_" in table_name:
            continue
        
        print("Converting table", table_name, "to columnar")
        convert_table_to_columnar(schema[table_name], table_name, encoded_dir, columnar_dir)