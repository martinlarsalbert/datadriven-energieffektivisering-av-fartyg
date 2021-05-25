import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run

def work(dataset, path:str, sample_size=1000000, min_trip_time=400):
         
    df = dataset.to_dask_dataframe(sample_size=sample_size, dtypes=None, on_error='null', out_of_range_datetime='null')
    process(df=df, path=path, min_trip_time=min_trip_time)
    
def process(df,path:str, min_trip_time=400):

    df_cleaned = clean(df_stat=df, min_trip_time=min_trip_time)
    save_to_parquet(df=df_cleaned, path=path)     
    
    return df_cleaned

def clean(df_stat:pd.DataFrame, min_trip_time=400)->pd.DataFrame:
    """Clean the statistics, removing outliers with unrealistic trip times.
    
    min_trip_time : float
        minimum realistic trip time

    Returns:
        pd.DataFrame: [description]
    """

    mask = ((df_stat['trip_time'] > min_trip_time) )
    df_cleaned = df_stat.loc[mask].copy()
    mask = df_stat['trip_time'] < df_stat['trip_time'].quantile(0.99)
    df_cleaned = df_cleaned.loc[mask].copy()

    return df_cleaned

def save_to_parquet(df:pd.DataFrame, path:str):
    
    df_save = df.copy()
    df_save['start_time'] = df_save['start_time'].astype(str)
    df_save['end_time'] = df_save['end_time'].astype(str)   
    df_save.to_parquet(path)

    return

def load_output_as_pandas_dataframe(path:str):
    df_stat = pd.read_parquet(path)
    
    df_stat['start_time'] = pd.to_datetime(df_stat['start_time'])
    df_stat['end_time'] = pd.to_datetime(df_stat['end_time'])
    
    df_stat.sort_values(by=['start_time'], inplace=True)
        
    assert (pd.TimedeltaIndex(df_stat['start_time'].diff().dropna()).total_seconds() > 0).all()  # assert that trips are ordered in time

    df_stat['trip_direction'] = df_stat['trip_direction'].apply(lambda x : 'Helsingør-Helsingborg' if x==0 else 'Helsingborg-Helsingør')

    return df_stat


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_clean_trip_statistics',
        type=str,
        help='Path to the output data'
    )
    parser.add_argument(
        '--min_trip_time',
        type=float,
        default=400.0,
        help='Minimum realistic trip time'
    )

    args = parser.parse_args()

    run = Run.get_context()
    # get input dataset by name
    dataset = run.input_datasets['trip_statistics']
    

    if not (args.output_clean_trip_statistics is None):
        os.makedirs(args.output_clean_trip_statistics, exist_ok=True)
        print("%s created" % args.output_clean_trip_statistics)
        path = args.output_clean_trip_statistics + "/processed.parquet"
                
        work(dataset=dataset, path=path, min_trip_time=args.min_trip_time)