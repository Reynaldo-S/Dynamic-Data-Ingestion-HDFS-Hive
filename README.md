# Dynamic Data Ingestion and Storage in HDFS with Automated Hive Integration

## Project Overview

This project implements a data pipeline that fetches data from a provided URL, stores it in Hadoop's HDFS, and integrates the data with Hive for analysis. The pipeline includes fetching CSV data, inferring schema, creating Hive tables, and loading the data. The process can be automated for regular data refreshes.

### Technologies Used
- **Python**: For automating tasks and interacting with HDFS and Hive.
- **HDFS**: For storing data in the Hadoop Distributed File System.
- **Hive**: For data analysis and query execution.
- **API (requests)**: For fetching data from the URL.
- **Subprocess**: For interacting with Hadoop and Hive via Docker.

## Project Workflow

1. **Check URL Accessibility**: Verifies if the provided data URL is accessible.
2. **Download Data**: Uses `wget` to download data to a local system.
3. **Upload to HDFS**: Moves the data to HDFS.
4. **Infer Hive Schema**: Extracts schema from the CSV file using pandas.
5. **Create Hive Table**: Automatically generates a Hive table based on the schema.
6. **Load Data into Hive**: Loads data from HDFS into the Hive table.
7. **Validate Data**: Validates by running sample queries on the Hive table.

## Installation & Setup

### Prerequisites:
1. **Hadoop & Hive** installed and configured.
2. **Docker** (for running Hive in a container).
3. **Python 3.x** and pip.

### Steps:

1. Clone the repository:
    ```bash
    git clone https://github.com/Reynaldo-S/Dynamic-Data-Ingestion-HDFS-Hive.git
    cd Dynamic-Data-Ingestion-HDFS-Hive
    ```

2. Install Python dependencies:

3. Configure your Hadoop & Hive environments in Docker:
    - Ensure Docker containers for Hadoop and Hive are running.

4. Update the script with your relevant paths (local and HDFS) and table names.

5. Run the script:
    ```bash
    python src/data_pipeline.py
    ```

### Example Output:

```bash
Downloaded data from https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/national/totals/nst-est2020-alldata.csv to /opt/sample/population_data.csv
Created HDFS directory at /user/hadoop/population_data/
Uploaded /opt/sample/population_data.csv to HDFS at /user/hadoop/population_data/
Hive table 'population_data' created successfully.
Loaded data into Hive table population_data
Validation Data:
REGION  DIVISION  STATE  NAME
... (output rows from Hive query)
