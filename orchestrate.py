import subprocess

# runs the first workflow to load and stage the data 
subprocess.run(["python" , './load_data_stage_1/workflow_loading_stage.py'])

# runs the second workflow to apply transformations and create a master table
subprocess.run(['python', './data_cleaning_stage_2/workflow_transformation.py'])