# azureml-core of version 1.0.72 or higher is required
# azureml-dataprep[pandas] of version 1.1.34 or higher is required
from azureml.core import Workspace, Dataset
import pandas as pd
import numpy as np

subscription_id = '3e9a363e-f191-4398-bd11-d32ccef9529c'
resource_group = 'demops'
workspace_name = 'D2E2F'

def get(name='tycho_short_id', n_rows = 20000)->pd.DataFrame:
    """Load time series from dataset containing trip_no.

    Parameters
    ----------
    name : str, optional
        [description], by default 'tycho_short_id'
    n_rows : int, optional
        [description], by default 20000

    Returns
    -------
    pd.DataFrame
        [description]
    """


    workspace = Workspace(subscription_id, resource_group, workspace_name)
    dataset = Dataset.get_by_name(workspace, name=name)
    
    df_raw = dataset.take(n_rows).to_pandas_dataframe()
    df_raw = prepare(df_raw=df_raw)
    
    return df_raw

def prepare(df_raw:pd.DataFrame)->pd.DataFrame:
    """Fix time as indec in loaded data frame.

    Parameters
    ----------
    df_raw : pd.DataFrame
        [description]

    Returns
    -------
    pd.DataFrame
        [description]
    """

    df_raw.set_index('time', inplace=True)
    df_raw.index = pd.to_datetime(df_raw.index)
    df_raw['trip_time'] = pd.to_timedelta(df_raw['trip_time'])
    
    return df_raw

def get_trip(trip_no:int,n_rows=None, dataset_name='tycho_short_id')->pd.DataFrame:
    """Get time series for one specific trip

    Parameters
    ----------
    trip_no : int
        the trip number to load
    n_rows : int, optional
        max number of rows to load, by default None
    dataset_name : str, optional
        [description], by default 'tycho_short_id'

    Returns
    -------
    pd.DataFrame
        [description]
    """


    workspace = Workspace(subscription_id, resource_group, workspace_name)
    dataset = Dataset.get_by_name(workspace, name=dataset_name)
    
    mask = dataset['trip_no'] == trip_no

    dataset_trip = dataset.filter(mask)
    if n_rows is None:
        trip = dataset_trip.to_pandas_dataframe()
    else:
        trip = dataset_trip.to_pandas_dataframe()

    trip = prepare(trip)

    return trip



