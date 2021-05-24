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
    save_to_parquet(df=df_statistics, path=path)     
    
    return df_statistics

def save_to_parquet(df:pd.DataFrame, path:str):
    
    df_save = df.copy()
    df_save['start_time'] = df_save['start_time'].astype(str)
    df_save['end_time'] = df_save['end_time'].astype(str)   
    df_save.to_parquet(path)

def statistics(df):

    trips = df.groupby(by='trip_no')
    
    meta = meta=dict(df.dtypes)
    meta.pop('time')
    meta['trip_time'] = int
    meta['start_time'] = str
    meta['end_time'] = str
    meta['start_index'] = int
    meta['end_index'] = int
        
    return trips.apply(func=trip_statistics, meta=meta).compute()

def trip_statistics(trip):

    assert isinstance(trip, pd.DataFrame)
    
    trip['time'] = pd.to_datetime(trip['time'])
    
    assert (pd.TimedeltaIndex(trip['time'].diff().dropna()).total_seconds() > 0).all()  # assert that rows are ordered in time
    
    start_time = trip.iloc[0]['time']
    trip['trip_time'] = pd.TimedeltaIndex(trip['time'] - start_time).total_seconds()

    df_statistics = trip.mean()

    df_statistics['start_time'] = start_time
    df_statistics['end_time'] = trip.iloc[-1]['time']
    df_statistics['start_index'] = trip.index[0]
    df_statistics['end_index'] = trip.index[-1]
    
    df_statistics['trip_direction'] = trip.iloc[0]['trip_direction']

    return df_statistics

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