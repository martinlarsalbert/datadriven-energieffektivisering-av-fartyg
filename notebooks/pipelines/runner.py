import papermill as pm
import os
import shutil

def copy_scripts(path_experiment, 
                 script_folders = ['../../../src/models/pipelines/longterm/scripts/prepdata/trip/',
                                   '../../../src/models/pipelines/longterm/scripts/prepdata/trip_statistics'
                                  ]):
    
        
    for script_folder in script_folders:
        
        scripts = [file_name for file_name in os.listdir(script_folder) if os.path.splitext(file_name)[-1]=='.py']
        for script in scripts:
            src = os.path.join(script_folder,script)
            dst = os.path.join(path_experiment,script)
            
            shutil.copyfile(src, dst)

def run_experiment(name:str,parameters:dict, path_steps = 'steps', path_experiments = 'experiments'):
    steps = [file_name for file_name in os.listdir(path_steps) if os.path.splitext(file_name)[-1]=='.ipynb']
    
    path_experiments = 'experiments'
    if not os.path.exists(path_experiments):
        os.mkdir(path_experiments)
        
    experiment = {
        'name' : name,
        'parameters' : parameters,
    }
    
    path_experiment = os.path.join(path_experiments,experiment['name'])
    if not os.path.exists(path_experiment):
        os.mkdir(path_experiment)
        
    copy_scripts(path_experiment=path_experiment)
    
    ## Run the steps:
    for step in steps:
    
        output_path = os.path.join(path_experiment,step)
        if os.path.exists(output_path):
            size = os.path.getsize(output_path)
            if size > 1000:
                print('Already run, skipping...')
                continue
        
        input_notebook_path = os.path.join(path_steps, step)
        
        notebook = pm.execute_notebook(
                    input_path=input_notebook_path,
                    output_path=output_path,
                    parameters=experiment['parameters'],
                    start_timeout=600,
                    cwd=path_experiment)