import os
import pandas as pd
from azureml.pipeline.steps import PythonScriptStep
from azureml.core import Experiment

def data(experiment:Experiment,step_name:str,dataset_name:str)->pd.DataFrame:
    """Get dataframe from pipeline step

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

    pipeline_run = None
    for run in experiment.get_runs():
        if run.get_status()=='Finished':
            pipeline_run = run
            break

    step = pipeline_run.find_step_run(step_name)[0]
    data = _fetch_df(step, dataset_name)
    return data


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