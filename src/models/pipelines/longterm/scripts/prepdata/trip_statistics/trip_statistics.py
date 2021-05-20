import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run

def work(dataset, path:str, sample_size=1000000):
         
    df = dataset.to_dask_dataframe(sample_size=sample_size, dtypes=None, on_error='null', out_of_range_datetime='null')
    process(df=df, path=path)
    
def process(df,path:str):

    df_statistics = statistics(df=df)
    
    df_save = df_statistics.copy()
    df_save['start_time'] = df_save['start_time'].astype(str)
    df_save['end_time'] = df_save['end_time'].astype(str)
        
    df_save.to_parquet(path)
    
    return df_statistics

def statistics(df):

    trips = df.groupby(by='trip_no')
    
    meta = meta=dict(df.dtypes)
    meta.pop('time')
    meta['trip_time'] = int
    meta['start_time'] = str
    meta['end_time'] = str
    
    return trips.apply(func=trip_statistics, meta=meta).compute()

def load_output_as_pandas_dataframe(path:str):
    df_stat = pd.read_parquet(path)
    df_stat['start_time'] = pd.to_datetime(df_stat['start_time'])
    df_stat['end_time'] = pd.to_datetime(df_stat['end_time'])
    df_stat.sort_index(inplace=True)
    assert (pd.TimedeltaIndex(df_stat['start_time'].diff().dropna()).total_seconds() > 0).all()  # assert that trips are ordered in time

    return df_stat

def trip_statistics(trip):

    assert isinstance(trip, pd.DataFrame)
    
    trip['time'] = pd.to_datetime(trip['time'])
    trip['trip_time'] = pd.TimedeltaIndex(trip['time'] - trip['time'].min()).total_seconds()

    df_statistics = trip.mean()
    
    df_statistics['start_time'] = trip['time'].min()
    df_statistics['end_time'] = trip['time'].max()
    df_statistics['trip_direction'] = trip.iloc[0]['trip_direction']

    return df_statistics


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