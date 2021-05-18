import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run
import trips
import prepare_dataset

import azureml.core

import pyarrow as pa
import pyarrow.parquet as pq

def work(ds, output_path:str, sample_size=1000000, n_rows = None):
    """Do the work in this pipeline:
    1) Load data into a Dask dataframe
    2) Divide into numbered trips
    3) Save the data with numbered trips ("trip_no" column) into a parquet file.

    Parameters
    ----------
    ds : azureml.core.Dataset
        Dataset from AML
    output_path : str
        Path to pipeline output file (parquet file)
    sample_size : int, optional
        The dask dataframe is partitioned, as it is larger than the memory can handle, by default 100000 rows.
    n_rows : int, default: None --> All rows used.
        Number of rows used in this pipeline
    """
    
    ds_filtered = prepare_dataset.filter(dataset=ds, n_rows = n_rows)
    df = ds_filtered.to_dask_dataframe(sample_size=sample_size, dtypes=None, on_error='null', out_of_range_datetime='null')
    
    save_numbered_trips(df=df, output_path=output_path)
    
def save_numbered_trips(df, output_path:str, max_skip=3):
    """Divide the data into trips and give each trip a unique number "trip_no"
    The data is saved to a parquet file.
    
    Parameters
    ----------
    df : dask.dataframe.DataFrame
        Large dataframe with data from operation
    output_path : str
        Path to the parquet file that is created with the numbered trips 
    """
    
    current_trip_no = 0
    
    parquet_schema = None
    
    skips=0
    for i,partition in enumerate(df.partitions):

        try:
            df_raw = partition.compute()
        except ValueError:
            if skips<max_skip:
                print(f'skipping partition:{i}')
                skips+=1
                continue
            else:
                print(f'max_skip={max_skip} breaking...')
                break

        df_ = prepare_dataset.prepare(df_raw=df_raw)
        trips.numbering(df=df_, start_number=current_trip_no)
        current_trip_no = df_.iloc[-1]['trip_no']

        df_['time'] = df_.index.astype(str)
        df_.reset_index(inplace=True, drop=True)

        # Write df partiotion to the parquet file
        if parquet_schema is None:
            parquet_schema = pa.Table.from_pandas(df=df_).schema
            parquet_writer = pq.ParquetWriter(output_path, parquet_schema, compression='snappy')

        table = pa.Table.from_pandas(df_, schema=parquet_schema)
        parquet_writer.write_table(table)

    parquet_writer.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_data_with_id',
        type=str,
        help='Path to the output data'
    )
    parser.add_argument(
        '--n_rows',
        type=int,
        default=0,
        help='Max number of rows to load'
    )
    args = parser.parse_args()
    
    run = Run.get_context()
    # get input dataset by name
    dataset = run.input_datasets['blue_flow_raw']

    n_rows = args.n_rows
    if n_rows==0:
        n_rows=None

    if not (args.output_data_with_id is None):
        os.makedirs(args.output_data_with_id, exist_ok=True)
        print("%s created" % args.output_data_with_id)
        output_path = args.output_data_with_id + "/processed.parquet"
        
        work(ds=dataset, output_path=output_path, n_rows=n_rows)
    

    