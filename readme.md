# Goal of the project 
The goal of this project is to compare 4 OTT platforms against each other to find out which OTT platform gives a better value for the buck

# OTT dataset used in this project 
- Netflix
- Amazon prime
- Hulu
- Disney Plus

*NOTE: The jupyter notebooks in this project are either created for developement process or prototyping, No jupyter notebook is running in the production it is just there for developement*

# Stage 1 : Loading raw data
The goal of this stage is to load the data as it is from the csv to the sql server without any changes 

### architechture
- Pandas reads the data from the csv files 
- Create an sql table if not exists 
- truncate the created table for any existing data 
- cleanly insert the new data into the table 


### Workflow function 
- An orchestration function that aims to orchestrate the stage 1
- uses constructor functions for easy maintainance

### Key points
- Idempotent pipeline (does not create redundant data)
- Clean code 
- Optimized single pipeline
- Full load pipeline 

# Stage 2 : applying transformations to the data
The goal of this stage is to apply transformations to the data for further usage 

### Transformations
- Transformed 'cast' column into an ARRAY for postgres
- Transformed 'listed_in' column into an ARRAY for postgres 

### workflow architechture
- retrieves data from SQL tables 
- Apply transformation 1
- Apply transformation 2
- make another table for the stage
- Save the data into the table

### key points 
- Single pipeline
- Idempotent pipeline
- Clean code 
- Full load pipeline

# Stage 3 : Data Analysis
The goal of this stage is to analyze the prepared data under different criterias to find the ott platform which gives the most value for the money.

### Key criterias
- How many shows are present in each platform
- What is the ratio of Web Series VS Movie for each platform 
- What is the geological variation of shows in each platform
- What is the release year variation of shows in each platform

***NOTE: The report is added in the repository inside the report folder***