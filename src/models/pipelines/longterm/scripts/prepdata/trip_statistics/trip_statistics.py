import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run

def get(dataset):
         
    df = dataset.to_pandas_dataframe()

    trips = df.groupby(by='trip_no')
    df_mean = trips.mean()
    return df_mean

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
        
    df = get(dataset=dataset)


    if not (args.output_trip_statistics is None):
        os.makedirs(args.output_trip_statistics, exist_ok=True)
        print("%s created" % args.output_trip_statistics)
        path = args.output_trip_statistics + "/processed.parquet"
        
        write_df = df.to_parquet(path)