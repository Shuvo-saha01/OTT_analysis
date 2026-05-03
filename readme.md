# Goal of the project 
The goal of this project is to compare 4 OTT platforms against each other to find out which OTT platform gives a better value for the buck

# OTT dataset used in this project 
- Netflix
- Amazon prime
- Hulu
- Disney Plus

# Load Stage 1
The goal of this stage is to load the data as it is from the csv to the sql server without any changes 

### Architechture of the pipeline 
- A full load pipeline
- Pandas reads the data from the csv files 
- Create an sql table if not exists 
- truncate the created table for any existing data 
- cleanly insert the new data into the table 

### Workflow function 
- An orchestration function that aims to orchestrate the stage 1
- uses constructor functions for easy maintainance
- Runs on the same architechture as the pipelines 