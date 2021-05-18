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
    df_statistics.to_parquet(path)
    return df_statistics

def statistics(df):

    trips = df.groupby(by='trip_no')
    
    meta = meta=dict(df.dtypes)
    meta.pop('time')
    meta['trip_time'] = int
    return trips.apply(func=trip_statistics, meta=meta).compute()

def trip_statistics(trip):

    assert isinstance(trip, pd.DataFrame)
    
    trip['time'] = pd.to_datetime(trip['time'])
    trip['trip_time'] = pd.TimedeltaIndex(trip['time'] - trip['time'].min()).total_seconds()

    return trip.mean()


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