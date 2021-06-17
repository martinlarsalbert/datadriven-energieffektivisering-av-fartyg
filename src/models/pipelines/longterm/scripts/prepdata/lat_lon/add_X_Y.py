import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Run
import scipy.integrate

def work(dataset, path:str, sample_size=1000000):
         
    df = dataset.to_dask_dataframe(sample_size=sample_size, dtypes=None, on_error='null', out_of_range_datetime='null')
    process(df=df, path=path)
    
def process(df,path:str):

    df_with_XY = add_XY(df=df)
    save_to_parquet(df=df_with_XY, path=path)     
    
    return df_with_XY

def save_to_parquet(df:pd.DataFrame, path:str):
    
    df_save = df.copy()
    df_save['start_time'] = df_save['start_time'].astype(str)
    df_save['end_time'] = df_save['end_time'].astype(str)   
    df_save.to_parquet(path)

def add_XY(df):
    pass

    

def load_output_as_pandas_dataframe(path:str):
    df_stat = pd.read_parquet(path)
    
    df_stat['start_time'] = pd.to_datetime(df_stat['start_time'])
    df_stat['end_time'] = pd.to_datetime(df_stat['end_time'])
    
    df_stat.sort_values(by=['start_time'], inplace=True)
        
    assert (pd.TimedeltaIndex(df_stat['start_time'].diff().dropna()).total_seconds() > 0).all()  # assert that trips are ordered in time

    return df_stat


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_trip_statistics',
        type=str,
        help='Path to the output data'
    )
    
    args = parser.parse_args()
    
    run = Run.get_context()
    # get input dataset by name
    dataset = run.input_datasets['data_with_id']
        
    

    if not (args.output_trip_statistics is None):
        os.makedirs(args.output_trip_statistics, exist_ok=True)
        print("%s created" % args.output_trip_statistics)
        path = args.output_trip_statistics + "/processed.parquet"
                
        df = work(dataset=dataset, path=path)