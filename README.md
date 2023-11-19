# ELT_pipeline_mongodb
Use python script to get ETL process which take array of rows from API and push to MongoDB every 12 hours.
Also there is the DAG of Airflow for orchestration

For use this script, you need:

1. API-link with input dataset
2. Change XPATH as needed
3. MongoDB Host + port, cluster name, db name, collection name, login and password
