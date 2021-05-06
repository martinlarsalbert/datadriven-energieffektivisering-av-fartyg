import pandas as pd

import os
from azureml.core import Dataset, Run
#import trips

def get(dataset):

    n_rows = 20000 
    rename = True
    do_calculate_rudder_angles=True

    mask = dataset['Speed over ground (kts)'] > 0.01
    if n_rows is None:
        df_raw = dataset.filter(mask).to_pandas_dataframe()
    else:
        df_raw = dataset.filter(mask).take(n_rows).to_pandas_dataframe()
    #df_raw = dataset.take(n_rows).to_pandas_dataframe()

    df_raw.set_index('Timestamp [UTC]', inplace=True)
    df_raw.index = pd.to_datetime(df_raw.index)
    df = df_raw.rename(columns = {
        'Latitude (deg)' : 'latitude',
        'Longitude (deg)' : 'longitude',
        'Course over ground (deg)' : 'cog',
    })
    df.index.name='time'
    df['sog'] = df['Speed over ground (kts)']*1.852/3.6

    df.drop(columns=[
        'Speed over ground (kts)',
    ], inplace=True)
    if rename:
        df = rename_columns(df)

    if do_calculate_rudder_angles:
        df = calculate_rudder_angles(df=df, drop=False)
    df.dropna(how='all', inplace=True, axis=1)  # remove columns with all NaN
    removes = ['power_propulsion_total',  ## Same thing as "power_em_thruster_total"
        ]
    df.drop(columns=removes, inplace=True)

    #df_2 = trips.divide(df=df, trip_separator='0 days 00:02:00')

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

    renames = {key:key.replace(' (kW)','').replace(' (deg)','').replace(' ()','').replace(' ','_').lower() for key in df.keys()}
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
            df_.drop(columns=[sin_key,cos_key], inplace=True)

    return df_

if __name__ == '__main__':
    run = Run.get_context()
    # get input dataset by name
    dataset = run.input_datasets['blueflow_raw']