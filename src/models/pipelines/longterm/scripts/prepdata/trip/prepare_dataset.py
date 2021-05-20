# azureml-core of version 1.0.72 or higher is required
# azureml-dataprep[pandas] of version 1.1.34 or higher is required
from azureml.core import Workspace, Dataset
import pandas as pd
import numpy as np

def get_dataset(name='tycho_short_parquet', n_rows = 20000):
    workspace = Workspace.from_config()
    dataset = Dataset.get_by_name(workspace, name=name)

    ds = filter(dataset=dataset, n_rows=n_rows)

    return ds

def filter(dataset,n_rows = 20000):

    mask = dataset['Speed over ground (kts)'] > 0.01
    if n_rows is None:
        ds = dataset.filter(mask)
    else:
        ds = dataset.filter(mask).take(n_rows)

    return ds


def get_pandas(name='tycho_short_parquet', n_rows = 20000):
    
    ds = get_dataset(name=name, n_rows=n_rows)
    df_raw = ds.to_pandas_dataframe()
    
    return df_raw

def get_dask(name='tycho_short_parquet', n_rows = None, sample_size=100000):

    ds = get_dataset(name=name, n_rows=n_rows)
    df_raw = ds.to_dask_dataframe(sample_size=sample_size, dtypes=None, on_error='null', out_of_range_datetime='null')
    
    return df_raw

def prepare(df_raw:pd.DataFrame, rename = True, do_calculate_rudder_angles=False):
    #df_raw = dataset.take(n_rows).to_pandas_dataframe()
    
    df_raw.set_index('Timestamp [UTC]', inplace=True)
    df_raw.index = pd.to_datetime(df_raw.index)
    df_raw.sort_index(inplace=True)

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

    return df

def numbering(df:pd.DataFrame, start_number:int, trip_separator='0 days 00:00:20')->pd.DataFrame:
    """Add a trip number to each row in df

    Parameters
    ----------
    df : pd.DataFrame
        data (all data or partition of data)
    start_number : 
        start of the numbering (not 0 if this is not the first dask partion)

    Returns
    -------
    pd.DataFrame
        df
    """
    
    df_starts, df_ends = get_starts_and_ends(df=df, trip_separator=trip_separator)

    end_number = start_number + len(df_starts)
    df_starts['trip_no'] = np.arange(start_number, end_number,dtype=int)
    for (start_time, start), (end_time, end) in zip(df_starts.iterrows(), df_ends.iterrows()):
        
        mask = ((start_time <= df.index) & 
                (df.index <= end_time)
               )
        
        df.loc[mask,'trip_no'] = start['trip_no']
        
    #df = df.dropna(subset=['trip_no'])  # drop unfinnished trips

    return df

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




