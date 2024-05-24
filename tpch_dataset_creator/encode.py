import os
import json
import datetime
import time
import re
import dask.dataframe as dd

start_date = datetime.datetime(1970, 1, 1)
def encode_date_col(dataframe, col_name):
    dataframe[col_name] = dd.to_datetime(dataframe[col_name], format = "%Y-%m-%d")
    dataframe[col_name] = (dataframe[col_name] - start_date).dt.days

def encode_str_col(dataframe, col_name):
    dataframe[col_name] = dataframe[col_name].astype('category')
    dataframe['id'] = dataframe[col_name].cat.as_known().cat.codes
    mappings_df = dataframe[[col_name, 'id']]
    mappings_df = mappings_df.rename(columns = {col_name : "entity"})
    dataframe[col_name] = dataframe['id']
    return mappings_df

def encode_table(table_schema, table_name, tables_dir, encoding_dir, encode_maps_dir):
    table_path = os.path.join(tables_dir, table_name + ".tbl")
    table_cols = table_schema["columns"]
    table_df = dd.read_csv(table_path, delimiter="|", header=None, names=table_cols)
    table_df = table_df.repartition(npartitions=os.cpu_count() - 1)
    
    for col_name in table_cols:
        column_type = table_schema[col_name]
        if "str" in column_type:
            mappings_df = encode_str_col(table_df, col_name)
            table_df = table_df.drop(columns=['id'])

            # Save the mappings df
            map_save_path = os.path.join(encode_maps_dir, table_name + "_" + col_name + ".map")
            mappings_df.to_csv(map_save_path, single_file=True, sep="|", index=False, header=False)
        elif "date" in column_type:
            encode_date_col(table_df, col_name)
    
    encode_path = os.path.join(encoding_dir, table_name + ".tbl")
    table_df.to_csv(encode_path, single_file=True, sep="|", index=False, header=False)

def perform_encoding(schema, tables_dir, encoding_dir, encode_maps_dir, overwrite):
    # See if encoding should be performed
    if os.path.exists(encoding_dir) and not overwrite:
        return

    # Create the necessary tables
    os.makedirs(encoding_dir, exist_ok = True)
    os.makedirs(encode_maps_dir, exist_ok = True)

    for table_name in schema:
        if "_" in table_name:
            continue
        
        encode_table(schema[table_name], table_name, tables_dir, encoding_dir, encode_maps_dir)