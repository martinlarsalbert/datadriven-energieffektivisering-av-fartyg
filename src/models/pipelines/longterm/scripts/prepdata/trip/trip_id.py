import pandas as pd
import numpy as np

import os
import argparse
from azureml.core import Dataset, Run
import trips

def get(dataset, n_rows = 10000):
     
    rename = True
    do_calculate_rudder_angles=True

    #if n_rows == 0:
    #    df_raw = dataset.filter(mask).to_pandas_dataframe()
    #else:
    #    df_raw = dataset.filter(mask).take(n_rows).to_pandas_dataframe()

    mask = dataset['Speed over ground (kts)'] > 0.01
    df_raw = dataset.filter(mask).to_dask_dataframe(sample_size=n_rows, dtypes=None, on_error='null', out_of_range_datetime='null')

    #df_raw = df_raw.set_index('Timestamp [UTC]')
    #df_raw.index = df_raw.index.astype('M8[ns]')
    
    df_raw['time'] = df_raw['Timestamp [UTC]'].astype('M8[ns]')
    df_raw = df_raw.drop(columns=['Timestamp [UTC]'])

    df = df_raw.rename(columns = {
        'Latitude (deg)' : 'latitude',
        'Longitude (deg)' : 'longitude',
        'Course over ground (deg)' : 'cog',
    })
    df.index.name='time'
    df['sog'] = df['Speed over ground (kts)']*1.852/3.6

    df = df.drop(columns=[
        'Speed over ground (kts)',
    ])
    
    if rename:
        df = rename_columns(df)

    if do_calculate_rudder_angles:
        df = calculate_rudder_angles(df=df, drop=False)
    df = df.dropna(how='all')  # remove columns with all NaN
    
    removes = ['power_propulsion_total',  ## Same thing as "power_em_thruster_total"
        ]
    df = df.drop(columns=removes)

    df_2 = df

    #df_2 = trips.divide(df=df, trip_separator='0 days 00:02:00')

    #run.log('rows', len(df_2))  # log loss metric to AML

    return df_2

def prepare(dataset, n_rows = 10000):

    mask = dataset['Speed over ground (kts)'] > 0.01
    
    dataset_filter = dataset.filter(mask)

    #if n_rows == 0:
    #    df_raw = dataset.filter(mask).to_pandas_dataframe()
    #else:
    #    df_raw = dataset.filter(mask).take(n_rows).to_pandas_dataframe()

    df_raw = dataset.to_dask_dataframe(sample_size=n_rows, dtypes=None, on_error='null', out_of_range_datetime='null')




def rename_columns(df:pd.DataFrame)->pd.DataFrame:
    """Rename columns of the data frame

    Parameters
    ----------
    df : pd.DataFrame
        raw data

    Returns
    -------
    pd.DataFrame
        data frame with columns with standard names
    """

    renames = {key:key.replace(' (kW)','').replace(' (deg)','').replace(' ()','').replace(' ','_').lower() for key in df.columns}
    df_ = df.rename(columns=renames)
    return df_

def calculate_rudder_angles(df:pd.DataFrame, inplace=True, drop=False)->pd.DataFrame:
    """Calculate "rudder angles" for the thrusters from cos/sin in the data files

    Parameters
    ----------
    df : pd.DataFrame
        data
    inplace : bool, optional
        [description], by default True

    Returns
    -------
    pd.DataFrame
        DataFrame with rudder angles added and cos/sin removed.
    """

    if inplace:
        df_ = df
    else:
        df_ = df.copy()

    for i in range(1,5):
        sin_key = 'sin_pm%i' % i
        cos_key = 'cos_pm%i' % i
        delta_key = 'delta_%i' % i

        df_[delta_key] = np.arctan2(df_[sin_key],-df_[cos_key])
        #df_[delta_key] = np.unwrap(df_[delta_key])
        
        if drop:
            df_ = df_.drop(columns=[sin_key,cos_key])

    return df_

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
    df2 = get(dataset=dataset, n_rows=n_rows)
    run.log(name='rows', value=len(df2))

    if not (args.output_data_with_id is None):
        os.makedirs(args.output_data_with_id, exist_ok=True)
        print("%s created" % args.output_data_with_id)
        path = args.output_data_with_id + "/processed.parquet"
        
        df_2_save = df2.copy()
        df_2_save.reset_index(inplace=True)
        df_2_save['time'] = df_2_save['time'].astype(str)
        df_2_save['trip_time'] = df_2_save['trip_time'].astype(str)
        df_2_save['trip_no'] = df_2_save['trip_no'].astype(int)
    
        write_df = df_2_save.to_parquet(path)