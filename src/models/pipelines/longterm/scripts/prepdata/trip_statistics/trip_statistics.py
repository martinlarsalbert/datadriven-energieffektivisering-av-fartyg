import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run
import scipy.integrate

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
    meta['trip_time'] = float
    
    meta['start_time'] = str
    meta['end_time'] = str
    meta['E1'] = 'float64'
    meta['E2'] = 'float64'
    meta['E3'] = 'float64'
    meta['E4'] = 'float64'
    meta['E'] = 'float64'
    meta['distance'] = 'float64'
        
    return trips.apply(func=trip_statistics, meta=meta).compute()

def trip_statistics(trip):

    assert isinstance(trip, pd.DataFrame)
    
    trip['time'] = pd.to_datetime(trip['time'])
    
    assert (pd.TimedeltaIndex(trip['time'].diff().dropna()).total_seconds() > 0).all()  # assert that rows are ordered in time
    
    start_time = trip.iloc[0]['time']
    end_time = trip.iloc[-1]['time']
    trip['trip_time'] = pd.TimedeltaIndex(trip['time'] - start_time).total_seconds()
    
    integrated = integrate_time(trip=trip)
    trip.drop(columns='time', inplace=True)
    df_statistics = trip.mean()

    df_statistics['start_time'] = str(start_time)
    df_statistics['end_time'] = str(end_time)
            

    ## Additional Integrated features:
    df_statistics['E1'] = integrated['P1']
    df_statistics['E2'] = integrated['P2']
    df_statistics['E3'] = integrated['P3']
    df_statistics['E4'] = integrated['P4']
    df_statistics['E'] = integrated['P']
    #
    df_statistics['distance'] = integrated['sog']

    df_statistics['trip_time'] = trip.iloc[-1]['trip_time'] - trip.iloc[0]['trip_time'] # Total trip time makes more sense.
    df_statistics['trip_no'] = trip.iloc[0]['trip_no']
    df_statistics['reversing'] = trip.iloc[0]['reversing']
    df_statistics['trip_direction'] = trip.iloc[0]['trip_direction']

    return df_statistics

def integrate_time(trip):
    trip_ = trip.copy()
    t = trip_['trip_time']
    assert t.dtype == 'float64'
    
    X = trip_.select_dtypes(include=[np.float64]) 
    integral_trip = scipy.integrate.simps(y=X.T,x=t)
    
    s = pd.Series(data=integral_trip, name='integral', index=X.columns)
    
    return s

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