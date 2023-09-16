# random_users_etl
ETL for Random Users

"Random User Generator" API was used for this assignment project which generated random user data. 

This API endpoint could be accessed at https://randomuser.me/api/

1. Data was extracted from the API: The task involved the use of provided API endpoint to fetch a batch of random user records (min: 100 records). The API response would be in JSON format.

2. Data was transformed: When the JSON response was obtained, it was parsed and relevant information was extracted from the user records in order to create a cleaned and structured dataset with the following columns:
First_Name  
Last_Name  
Gender  
Email   
Date_of_Birth (in YYYY-MM-DD format) 
Country 
Street Address  
City  
State  
Postcode  
Phone_Cell

3. Data was loaded into PostgreSQL database: After transforming the data, a new table(random_user_data) was created in the PostgreSQL database to store the cleaned user data. The task now was to load the transformed data from the previous step into this table.

4. Airflow DAG: To orchestrate the entire ETL process, an Apache Airflow Directed Acyclic Graph (DAG) was created. DAG includes the following tasks:
i. Extract data from the API 
ii. Transform the data 
ii. Load the data into the PostgreSQL database

5. Scheduling: The DAG was scheduled to run daily at a specific time.
