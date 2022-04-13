Dear reviewer,
thanks for taking the time checking my code. It works as follows:
1. main.py is the entry-point. It imports Classes/functions from the connections- & transformation folders 
2. In main.py a Spark Session is generated. It is configured with a MySQL JDBC driver that enables Spark to write and read data from MySQL databases
3. The script calls function get_datasets() from connection/api_con.py, passing the API endpoint as an argument
4. The response from the API is stored locally as JSON file (this would not be necessary for the next steps, but was a requirement in the briefing)
5. Next, the JSON file is read into a Spark dataframe
6. The dataframe is passed as an argument to the class method cleanse_df (imported from transformations/data_cleansing.py). The cleansing does the following:
    a. it transforms column date into date format
    b. it capitalizes the first character of columns first_name, last_name and country
    c. it checks the format of the IP address. If it matches a certain pattern, column "valid_ip" is "yes" (otherwise "no")
7. In a next step, the script instantiates class DataAnalyzing from transformations/data_analyzing and passes the cleansed dataframe into its 2 class methods:
    a. group_by_country ==> How many occurrences per country? How many females/males?
    b. group_by_date ==> How many occurrences per date?
8. Lastly, the script instantiates Class MySql and uses its method to store both results in a MySql database

Optional: there is a Unittest for the class DataCleansing. At the moment, it is rather basic. It tests, that the data_cleansing method 
          in fact capitalizes the first character of column first_name, last_name and country. It also tests if the IP address check works as intended. 
          Unittests can be combined with the Coverage library in order to check how much of the overall code is covered by tests. 
          Instead of running them manually, they can be part of a CI/CD pipeline, so everytime a new build is triggered, these tests are run
          
------------------------------------------------------------------------------------------------------------------------------------------------------------

How can the code be run? ==> Containerized via Airflow (you will need Docker installed on your machine).
I created a Docker Image of the code here: https://hub.docker.com/repository/docker/moxelpeterle/test_repo1. 
The only code that you will need from this repo is etl_dag.py from the dags-folder. Store this file somewhere on your machine.

1. Please store the official Airflow docker-compose.yaml on your machine: 
    https://github.com/apache/airflow/blob/main/docs/apache-airflow/start/docker-compose.yaml
Note: 
    - add the following code under x-airflow-common/volumes: - /var/run/docker.sock:/var/run/docker.sock
    - make sure the user under x-airflow-common/user is permitted to run docker on your local machine (in doubt use root)

2. Open a terminal, go to the directory where the docker-compose.yaml file is stored and run "docker-compose up airflow-init". 
3. After this code is finished with "0", run "docker-compose up"
4. Check localhost 8080 for the UI. PW and USER are "airflow"
5. To copy Dags into Airflow, do the following:
    - in a second terminal, type "docker ps". It will return a list of containers. Copy the id of the container in which the webserver runs
    - Go to the directory where you stored etl_dag.py and use command "docker cp etl_dag.py container-id:/opt/airflow/dags" 
    - Now the dag should be visible in the UI
6. Unpause the DAG and trigger it
7. You can check the logs in order to get a preview of the transformed data. If you would prefer to query the MySQL database yourself, I am happy to give
    you the connection string + credentials, but it does not contain anything more than you can see in the logs
