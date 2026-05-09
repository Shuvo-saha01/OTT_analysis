# Follow this documentation to install and start the project 

***Prerequisites***
- Python 
- Docker 
- Docker Compose

### Setup the env 
```
POSTGRES_URI=postgresql://username:password@localhost:5432/database_name
```
username = postgres   
password = root  
database_name = ott_database

### Start the docker compose 
To start
```
docker compose up
```


To stop 
```
docker compose down
```

### Run the orchestration script 
run the   ***orchestrate.py***   to run the entire pipeline and populate your database

### Use metabase 
use metabase at 
```
http://localhost:3000/
```
