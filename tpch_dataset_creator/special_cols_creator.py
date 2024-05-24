import os
import argparse
import json
import pandas as pd
import struct
import sys
import subprocess
import humanfriendly

from utils import *

def load_table(schema, tables_dir, table_name):
    table_path = os.path.join(tables_dir, table_name + ".tbl")
    table_cols = schema[table_name]["columns"]
    table_df = dd.read_csv(table_path, delimiter="|", header=None, names=table_cols)
    table_df = table_df.repartition(npartitions=os.cpu_count() - 1)
    return table_df

def create_customer_phone_start_code(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "customer")
    table_df["start_code"] = table_df["c_phone"].astype(str)
    table_df["start_code"] = table_df["start_code"].str.split("-").str[0]
    table_df["start_code"] = table_df["start_code"].astype('int32')
    save_col_to_file(table_df["start_code"], save_path)

def create_orders_comment_is_special(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "orders")
    table_df['is_special_request'] = table_df['o_comment'].str.contains('special.*requests', case=False, regex=True)
    table_df['is_special_request'] = table_df['is_special_request'].astype('int32')
    save_col_to_file(table_df["is_special_request"], save_path)

def create_part_forest_in_name(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["starts_with_forest"] = table_df["p_name"].str.lower()
    table_df["starts_with_forest"] = table_df["starts_with_forest"].str.startswith("forest")
    table_df["starts_with_forest"] = table_df["starts_with_forest"].astype('int32')
    save_col_to_file(table_df["is_special_request"], save_path)

def create_part_green_in_name(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["contains_green"] = table_df["p_name"].str.lower()
    table_df["contains_green"] = table_df["contains_green"].str.contains("green")
    table_df["contains_green"] = table_df["contains_green"].astype('int32')
    save_col_to_file(table_df["contains_green"], save_path)

def create_part_is_brand_45(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["is_brand"] = table_df["p_brand"] == "Brand#45"
    table_df["is_brand"] = table_df["is_brand"].astype('int32')
    save_col_to_file(table_df["is_brand"], save_path)

def create_part_is_brass_type(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["is_brass"] = table_df["p_type"].str.lower()
    table_df["is_brass"] = table_df["is_brass"].str.endswith("brass")
    table_df["is_brass"] = table_df["is_brass"].astype('int32')
    save_col_to_file(table_df["is_brass"], save_path)

def create_part_is_promo(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["is_promo"] = table_df["p_type"].str.lower()
    table_df["is_promo"] = table_df["is_promo"].str.startswith("promo")
    table_df["is_promo"] = table_df["is_promo"].astype('int32')
    save_col_to_file(table_df["is_promo"], save_path)

def create_part_type_medium(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "part")
    table_df["is_med"] = table_df["p_type"].str.lower()
    table_df["is_med"] = table_df["is_med"].str.startswith("medium polished")
    table_df["is_med"] = table_df["is_med"].astype('int32')
    save_col_to_file(table_df["is_med"], save_path)

def create_supplier_customer_complaint(schema, tables_dir, save_path):
    table_df = load_table(schema, tables_dir, "supplier")
    table_df['is_complaint'] = table_df['s_comment'].str.contains('.*Customer.*Complaints.*', case=False, regex=True)
    table_df['is_complaint'] = table_df['is_complaint'].astype('int32')
    save_col_to_file(table_df["is_complaint"], save_path)

create_functions = {
    "customer_phone_start_code" : create_customer_phone_start_code,
    "orders_comment_is_special" : create_orders_comment_is_special,
    "part_forest_in_name" : create_orders_comment_is_special,
    "part_green_in_name" : create_part_green_in_name,
    "part_is_brand_45" : create_part_is_brand_45,
    "part_is_brass_type" : create_part_is_brass_type,
    "part_is_promo" : create_part_is_promo,
    "part_type_medium" : create_part_type_medium,
    "supplier_customer_complaint" : create_supplier_customer_complaint
}

def create_special_cols(schema, tables_dir, columns_dir, overwrite):
    for col_name in schema["custom_cols"]:
        # Determine the save path
        save_path = os.path.join(columns_dir, col_name)
        if os.path.exists(save_path) and not overwrite:
            continue

        create_functions[col_name](schema, tables_dir, save_path)