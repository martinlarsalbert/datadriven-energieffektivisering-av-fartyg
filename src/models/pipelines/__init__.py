import os
from azureml.dataprep.api import expressions
import pandas as pd
from azureml.pipeline.steps import PythonScriptStep
from azureml.core import Experiment
from azureml.core import Workspace, Dataset

def _data(experiment:Experiment,step_name:str,dataset_name:str)->pd.DataFrame:
    """ (deprecated)!!!!
    Get dataframe from pipeline step 

    Parameters
    ----------
    experiment : Experiment
        [description]
    step_name : str
        [description]
    dataset_name : str
        [description]

    Returns
    -------
    pd.DataFrame
        [description]
    """
    
    step = _get_step(experiment=experiment, step_name=step_name)
    data = _fetch_df(step, dataset_name)
    return data

def data(experiment:Experiment,step_name:str,dataset_name:str)->Dataset:
    """ Get dataset from pipeline step output

    Parameters
    ----------
    experiment : Experiment
        [description]
    step_name : str
        [description]
    dataset_name : str
        [description]

    Returns
    -------
    dataset : azureml.core.Dataset
    """

    step = _get_step(experiment=experiment, step_name=step_name)
    output_data = step.get_output_data(dataset_name) 
    
    workspace = Workspace.from_config()
    datastore = workspace.get_default_datastore()
    ds = Dataset.Tabular.from_parquet_files((datastore,output_data.path_on_datastore))
    return ds

def register_output_to_workspace(experiment:Experiment, step_name:str, dataset_name:str, description='', create_new_version=True):
    """ register dataset from pipeline step output

    Parameters
    ----------
    experiment : Experiment
        [description]
    step_name : str
        [description]
    dataset_name : str
        [description]
    """

    workspace = Workspace.from_config()
    
    register_name = f'{experiment.name}_{dataset_name}'
    
    ds = data(experiment=experiment, step_name=step_name, dataset_name=dataset_name)
    
    print(f'register:{register_name}')
    ds.register(workspace=workspace, name=register_name, description=description, create_new_version=create_new_version)

def _get_step(experiment:Experiment,step_name:str):
    """Get step from experiement pipeline

    Parameters
    ----------
    experiment : Experiment
        [description]
    step_name : str
        [description]
    

    Returns
    -------
    step
    """

    pipeline_run = None
    for run in experiment.get_runs():
        if run.get_status()=='Finished':
            pipeline_run = run
            break

    step = pipeline_run.find_step_run(step_name)[0]
    return step


def _get_download_path(download_path:str, output_name:str)->str:
    """ functions to download output to local and fetch as dataframe

    Parameters
    ----------
    download_path : str
        [description]
    output_name : str
        [description]

    Returns
    -------
    str
        [description]
    """
    output_folder = os.listdir(download_path + '/azureml')[0]
    path =  download_path + '/azureml/' + output_folder + '/' + output_name
    return path

def _fetch_df(step:PythonScriptStep, output_name:str)->pd.DataFrame:
    """[summary]

    Parameters
    ----------
    step : PythonScriptStep
        [description]
    output_name : str
        [description]

    Returns
    -------
    pd.DataFrame
        [description]
    """
    output_data = step.get_output_data(output_name)    
    download_path = './outputs/' + output_name
    output_data.download(download_path, overwrite=True)
    df_path = _get_download_path(download_path, output_name) + '/processed.parquet'
    
    return pd.read_parquet(df_path)




