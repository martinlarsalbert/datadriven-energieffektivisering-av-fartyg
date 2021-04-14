# azureml-core of version 1.0.72 or higher is required
# azureml-dataprep[pandas] of version 1.1.34 or higher is required
from azureml.core import Workspace, Dataset
import pandas as pd

subscription_id = '3e9a363e-f191-4398-bd11-d32ccef9529c'
resource_group = 'demops'
workspace_name = 'D2E2F'

def get_dataset(name='tycho_short', n_rows = 20000):

    workspace = Workspace(subscription_id, resource_group, workspace_name)
    dataset = Dataset.get_by_name(workspace, name=name)
    
    df_raw = dataset.take(n_rows).to_pandas_dataframe()
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

    mask = df['sog'] > 0.01
    df = df.loc[mask].copy()

    return df