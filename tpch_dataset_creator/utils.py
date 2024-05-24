import os
import humanfriendly
import dask.dataframe as dd

CHUNK_SIZE_HUMAN = "5GB"
CHUNK_ELEMENTS = int(humanfriendly.parse_size(CHUNK_SIZE_HUMAN)/4)

def save_col_to_file(column, column_save_path):
    col_arr = column.to_dask_array()
    col_arr.compute_chunk_sizes()
    col_arr = col_arr.rechunk(CHUNK_ELEMENTS)

    with open(column_save_path, 'wb+') as writer:
        for curent_chunk in col_arr.blocks:
            result = curent_chunk.compute()
            result.tofile(writer)