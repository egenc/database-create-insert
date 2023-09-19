# PostgreSQL Database Table Creation and CSV Data Insertion

## Description
This repo will create a table in a PostgreSQL database and insert data from a CSV file.

## Prerequisites
Before using this script, make sure you have the following prerequisites installed:

- PostgreSQL database
- Python 3.8
- Python packages:
  - psycopg2: `pip install psycopg2`
  - pandas: `pip install pandas`
  - Watchdog: `pip install watchdog`

Or, simply run: `pip install -r requirements.txt`


## Setup
1. Ensure that you have a PostgreSQL database running, and you have the necessary credentials (host, database name, username, and password).

2. Update the script with your database configuration and CSV file details in the `DB_CONFIG` and `FILE_NAME` variables at the beginning of the script:

```python
DB_CONFIG = {
    'host': 'localhost',          # Replace with your PostgreSQL host
    'database': 'postgres',      # Replace with your database name
    'user': 'postgres',          # Replace with your database username
    'password': '123'            # Replace with your database password
}
FILE_NAME = 'data/data.csv'      # Replace with the path to your CSV file
```

## Usage
Run the script to create the table and insert data from the specified CSV file:

```bash
python main.py
```
The script will create a table named `BioSequenceData` in your PostgreSQL database and insert the data from the CSV file into this table.

If you want to automatically process newly created CSV files in a directory, you can keep the script running, and it will use `Watchdog` to monitor the data directory for new CSV files. When a new CSV file is detected, it will be processed automatically.

To stop the script, press `Ctrl + C` - (`Cmd + C` for Mac) in the terminal where it's running.

## Logging
The script logs its activities to a file named `database.log` and also prints log messages to the console. You can review the log file for any errors or information about the script's execution.

## Sample Query
```sql
SELECT *
FROM biosequencedata
WHERE date_created >= '2020-01-01'
ORDER BY project_name ASC;
```

# Assumptions & Notes
## 1. Columns for Machine Learning
This info relies on personal research and it is prone to errors.
- **'seq'**: The sequence itself is vital for various types of sequence analysis models.
- **'origin'**: Source of sequence could be a factor for classification or clustering tasks.
- **'Asn - deamidation risk Cnt'**: Might be useful for predicting sequence stability or interactions.
- **'Cys Cnt'**: Cysteine count could be related to the formation of disulfide bonds, affecting structure.
- **'Isomerization Cnt'**: Could be useful for predicting potential changes in structural conformation.
- **'Met Cnt'**: Methionine count could influence oxidation susceptibility.
- **'N-Glycosylation Cnt'**: Important for post-translational modifications.
- **'Pro Cnt'**: High count could suggest loops or turns in the protein structure.
- **'Strong Deamidation Cnt'**: May indicate stability or degradation over time.
- **'Weak Deamidation Cnt'**: Similar to above but less impactful.
- **'SEQUENCE_TYPE'**: Different sequences (DNA, RNA, protein) may need different model architectures.
- **'STOICHIOMETRY'**: Could be useful for modeling multicomponent systems.
- **'Isotype'**: If immunoglobulins are involved, this could affect function and therefore be an important feature.

## 2. Docker Usage
Adding Docker to this task provides a way to package this application and its dependencies into a portable and isolated container. This ensures that the application runs consistently across different systems, simplifies deployment, and makes it easier to manage dependencies.

```bash
docker create --name my-container my-app-image ./my-custom-command arg1 arg2
...
docker run -d --name my-container -p 8080:80 -e MY_ENV_VAR=my-value my-app-image
```

## 3. Adding Scheduler
Apache Airflow is a powerful workflow orchestration tool suitable for managing complex, multi-step data pipelines and ETL processes, whereas Watchdog excels at real-time file system monitoring tasks.

In this task, Watchdog is preferred due to simplicity of the task.

## 4. Connecting Source Folder to Cloud
CSV files can be accessed from a cloud storage like Azure Blob Storage. For instance:
```python
blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
```
