
# Airflow Data Pipeline: Load Employee Data from S3 to Snowflake

This project demonstrates an Apache Airflow data pipeline for loading employee data from an S3 bucket to Snowflake. The pipeline automates the process of extracting data from PostgreSQL, uploading it to S3, joining and detecting new or changed rows, and finally loading the data into Snowflake.

## Prerequisites

Before running the pipeline, ensure that the following requirements are met:

- Apache Airflow is installed. You can refer to the Airflow documentation for installation instructions: [Apache Airflow Documentation](https://airflow.apache.org/docs/)

- Access to a PostgreSQL database with the required tables (`finance.emp_sal` and `hr.emp_details`), and Snowflake database for loading the data.

- Proper configuration of connections in Airflow:
  - `postgres_connection`: PostgreSQL connection details for reading data from the database.
  - `aws_connection`: AWS S3 connection details for uploading data to S3.
  - `snowflake_conn`: Snowflake connection details for loading data into Snowflake.

## Project Structure

The project structure is organized as follows:

- `dags/`: Directory containing the Airflow DAG definition file.
- `includes/`: Directory containing custom Python modules used in the DAG.
- `queries/`: Directory containing SQL queries used in the DAG.
- `README.md`: This file providing an overview of the project.

## DAG Description

The DAG (`LoadEmpDataFromS3ToSnowflake`) consists of the following tasks:

1. `load_emp_sal_to_s3`: Reads data from the `finance.emp_sal` table in PostgreSQL and uploads it to the specified S3 bucket.
2. `load_emp_details_to_s3`: Reads data from the `hr.emp_details` table in PostgreSQL and uploads it to the specified S3 bucket.
3. `join_and_detect_changes`: Executes a custom Python function to join the data and detect new or changed rows.
4. `decide_insert_or_update`: Branches the workflow based on the presence of new or changed rows.
5. `insert_new_rows`: Inserts new rows into the `DWH_EMP_DIM` table in Snowflake.
6. `update_changed_rows`: Updates changed rows in the `DWH_EMP_DIM` table in Snowflake.
7. `insert_new_rows_2`: Inserts new rows into the `DWH_EMP_DIM` table in Snowflake.
8. `end_task`: A BashOperator task that marks the completion of the data loading process.

## Usage

1. Set up Apache Airflow and ensure all required connections are configured properly.
2. Copy the DAG file `load_emp_data.py` from the `dags/` directory to your Airflow DAGs folder.
3. Place the custom Python module `emp_dim_insert_update.py` inside the `includes/` directory.
4. Create a `queries/` directory and populate it with the necessary SQL query files (`INSERT_INTO_DWH_EMP_DIM.sql` and `UPDATE_DWH_EMP_DIM.sql`).
5. Update the DAG file and connection details as per your environment.
6. Start Airflow's scheduler and webserver to run the DAG.


