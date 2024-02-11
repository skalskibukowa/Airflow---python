# Project to practice Airflow with python

## Overview

In the project, several scenarios have been applied that can potentially be used in real-world cases.

## Build and Run:

1. Build the airflowpython-docker image:

   docker build -t airflowpython-docker .

2. Start the Docker containers:

   docker-compose up -d

## Scenarios:

### API to Database (PostgreSQL)

**Script: API_to_DB.py**

**Overall Purpose:**

The given Airflow DAG, named fetch_and_store_animal_facts, automates a daily process that fetches animal facts from an API and stores them in a PostgreSQL database.

**Key Components:**

Data Source: An HTTP endpoint (https://cat-fact.herokuapp.com/facts) providing JSON-formatted animal facts.
Target Database: PostgreSQL database named "airflow" accessible using the connection ID "http_default".
Tasks:
fetch_animal_facts (SimpleHttpOperator):
Fetches animal facts from the API using an HTTP GET request.
Verifies if the response status code is 200 (success).
process_and_store_data (PythonOperator):
Processes the fetched data:
Extracts the "text" field from each fact entry (handling potential absence).
Stores the data in the PostgreSQL database:
Creates the "Animal_text" table if it doesn't exist.
Inserts facts into the table using parameterized queries for security.
Handles potential exceptions using try-except blocks and logs errors.

**Workflow:**

Daily Schedule: The DAG runs every day (@daily).

**Fetching Facts:**
The fetch_animal_facts task executes and retrieves facts from the API.
Processing and Storing:
The process_and_store_data task:
Extracts text from each fact, handling potential absence.
Inserts data into the PostgreSQL table securely using parameterized queries.
Logs any errors that occur.



### CSV to Database (PostgreSQL)

**Script: Load_CSV_to_DB.py**

**Overall Purpose:**
This Airflow DAG automates a process that ingests data from a CSV file into a PostgreSQL database table. It is named LoadCSVtoDB_dag.
Key Components:

**Data Source and Target:**
Input: CSV file dynamically generated (e.g., /opt/airflow/data_sink/sales_data_{current_datetime}.csv)
Output: PostgreSQL table with a dynamic name based on the execution date and time (e.g., Sales_20240212_220121)
Function Definition:

ingest_csv_to_postgres:
Loads the CSV file into a DataFrame.
Creates a unique table name with the current date and time appended.
Adds a new column named Source containing the original CSV file name.
Connects to the PostgreSQL database and creates the table (if it doesn't exist) using an empty DataFrame.
Writes the main DataFrame to the table, appending to existing data.
Prints a success message.

**DAG Definition:**
Name: LoadCSVtoDB_dag
Start date: One day before the current date (start_date=days_ago(1))
Schedule: Every 2 minutes (schedule_interval='*/2 * * * *')
No past instances run (catchup=False)
Tasks:

print_IngestCSV:
PythonOperator that executes the ingest_csv_to_postgres function.
Passes arguments to the function:
csv_path: Path to the CSV file
table_name: Base name for the table
database_url: Connection string to the PostgreSQL database

**Workflow:**
The DAG runs every 2 minutes.
The print_IngestCSV task executes the ingest_csv_to_postgres function.
The function:
Reads data from the specified CSV file.
Creates a unique table name based on the current date and time.
Connects to the PostgreSQL database and creates the table if it doesn't exist.
Adds a Source column for tracking origin.
Appends the data to the table, preserving existing data.
Prints a success message.

### DB (PostgreSQL) to DB (PostgreSQL)

**Script: Load_PQ_to_PQ.py**

**Overall Purpose:**

This Airflow DAG ("etl_sales_data") automates a daily ETL (Extract, Transform, Load) process for sales data using PostgreSQL databases.
Key Components:

**DAG Definition:**
Name: etl_sales_data
Default arguments:
Owner: Team 1
No dependency on past runs
Start date: November 18, 2023 (can be adjusted as needed)
No email notifications on failure or retry
1 retry with a 5-minute delay
Schedule: Daily (@daily)
Description: "A simple ETL process for the sales table"

**Data Sources and Engine Connections:**
Source database: postgres port 5432, "airflow" user, "airflow" password, schema "airflow"
Target database: postgres port 5432, "airflow" user, "airflow" password, schema "airflow_2"

**Tasks:**
transform_data:
Transform function:
Inputs: execution_date from Airflow context
Constructs dynamic source table name based on execution date and time
Creates connections to source and target database engines
Reads data from the source table (Sales_20231119_191010) using a placeholder query (needs customization)
Transforms data by multiplying the price column by 1.25
Inserts transformed data into the target table (sales) in the target database, replacing existing data

**PythonOperator configuration:**
Task ID: transform_data
Python function to execute: transform_data
Provides execution date context to the function
Belongs to the etl_sales_data DAG

**Workflow:**
The DAG runs daily based on the @daily schedule.
The transform_data task executes the transform_data function.
The function:
Constructs the source table name dynamically using the execution date and time.
Establishes connections to the source and target databases.
Reads data from the source table (query needs to be adapted based on your actual table structure).
Transforms the data by increasing price by 25%.
Inserts the transformed data into the target table, replacing any existing data.

### CSV to PQ (Parquet)

**Script: Transform_CSV_to_Parq.py**

**Overall Purpose:**

The code defines an Airflow DAG that transforms a CSV file into Parquet format, offering performance and storage advantages for large datasets.
It runs every 30 minutes to automate the conversion process.
Key Components:

**DAG Definition:**

The DAG is named TransfromCSVtoParq_dag.
It starts one day before the current date (start_date=days_ago(1)).
It runs every 30 minutes (schedule_interval='*/30 * * * *').
It doesn't run for past instances (catchup=False).

**Tasks:**
transform_csv_to_parq: This single task performs the CSV to Parquet conversion.
Function Details:

transfrom_csv_to_parq():
Reads a CSV file from a specified path using PyArrow's CSV reader.
Writes the data as a Parquet file using PyArrow's Parquet writer, replacing ".csv" with ".parquet" in the output path.
Prints a success message.

**Additional Notes:**

The code leverages PyArrow for efficient data reading and writing in both CSV and Parquet formats.
It assumes the input CSV file is located in /opt/airflow/data_source and saves the output Parquet file in /opt/airflow/data_sink.
It uses a PythonOperator to execute the transformation function within the Airflow workflow.

### MIX

**Script: Welcome_dag.py**

**Overall Purpose:**
The DAG defines a workflow that processes two CSV files (customer_data.csv and sales_data.csv) and ultimately merges them into a combined file.
It includes basic tasks for printing messages, fetching a random quote, and demonstrating task dependencies.
Key Components:

**DAG Definition:**
The DAG is named welcome_dag.
It starts one day before the current date (start_date=days_ago(1)).
It runs every 2 minutes (schedule_interval='*/2 * * * *').
It doesn't run for past instances (catchup=False).

**Tasks:**
print_welcome: Prints a welcome message and the current working directory.
print_date: Prints the current date.
print_random_quote: Fetches and prints a random quote from an API.
print_csv_task: Processes the customer_data.csv file by doubling the values in the "age" column and saving it with a new name.
print_csv_task_sales: Processes the sales_data.csv file by doubling the "quantity" column, multiplying the "price" column by 1.25, and saving it with a new name.
print_csv_taks_CustomerSales: Merges the processed customer and sales CSV files based on "customer_id" and saves the merged file.
Task Dependencies:

**The tasks run in the following order:**
print_welcome
print_date
(concurrently) print_csv_task and print_csv_task_sales
print_random_quote
print_csv_taks_CustomerSales

**Additional Notes:**
The code uses PythonOperator to define tasks that execute Python functions.
It imports necessary libraries like pandas for data manipulation and requests for fetching the random quote.
It assumes the input CSV files are located in a specific directory (/opt/airflow/data_source) and saves output files to another directory (/opt/airflow/data_sink).
