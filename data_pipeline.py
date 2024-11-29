import subprocess
import requests
import pandas as pd
from io import StringIO
import os

# Configuration variables
url = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/national/totals/nst-est2020-alldata.csv"
hdfs_path = "/user/hadoop/population_data/"
filename = "population_data.csv"
hive_table_name = "population_data"
local_path = "/opt/sample/population_data.csv" 

# Step 1: Check URL accessibility
def check_url_accessibility(url):
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        print(f"Error checking the URL: {e}")
        return False

# Step 2: Download data inside the Namenode container
def download_data_locally(url, local_path):
    try:
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "wget", "-O", local_path, url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(f"Downloaded data from {url} to {local_path}")
        else:
            print(f"Failed to download data: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to download data: {e.stderr}")

# Step 3: Upload the local file to HDFS
def upload_to_hdfs(local_path, hdfs_path):
    try:
        # Ensure the HDFS directory exists
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "hdfs", "dfs", "-mkdir", "-p", hdfs_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(f"Created HDFS directory at {hdfs_path}")
        else:
            print(f"Failed to create HDFS directory: {result.stderr}")
        
        # Upload the file from the local file path inside the Namenode container to HDFS
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "hdfs", "dfs", "-put", local_path, hdfs_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(f"Uploaded {local_path} to HDFS at {hdfs_path}")
        else:
            print(f"Failed to upload data to HDFS: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload data to HDFS: {e.stderr}")

# Step 4: Infer Hive schema using Pandas from the local file inside Docker
def infer_hive_schema(file_path):
    try:
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "cat", file_path], capture_output=True, text=True, check=True)
        df = pd.read_csv(StringIO(result.stdout), on_bad_lines='skip')
        print(f"Dataframe Columns: {df.dtypes}")
        
        # Map Python types to Hive types
        type_mapping = {
            "int64": "INT",
            "float64": "FLOAT",
            "object": "STRING",
            "bool": "BOOLEAN"
        }

        hive_schema = ",\n".join(
            f"`{column}` {type_mapping.get(str(dtype), 'STRING')}"
            for column, dtype in df.dtypes.items()
        )
        return hive_schema
    except Exception as e:
        print(f"Error inferring schema: {e}")
        return None

# Step 5: Create Hive table
def create_hive_table(hive_schema, table_name):
    if not hive_schema:
        print("Failed to infer Hive schema.")
        return
    hive_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {hive_schema}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    """
    try:
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", hive_query], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(f"Hive table '{table_name}' created successfully.")
        else:
            print(f"Failed to create Hive table: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create Hive table: {e.stderr}")

# Step 6: Load data into Hive table
def load_data_into_hive(table_name, hdfs_path):
    hive_query = f"""
    LOAD DATA INPATH '{os.path.join(hdfs_path, filename)}' INTO TABLE {table_name};
    """
    try:
        result = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", hive_query], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            print(f"Loaded data into Hive table {table_name}")
        else:
            print(f"Failed to load data into Hive table: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to load data into Hive table: {e.stderr}")

# Step 7: Validate data in Hive table
def validate_data(table_name):
    hive_query = f"SELECT REGION, DIVISION, STATE, NAME FROM population_data LIMIT 5;"
    try:
        process = subprocess.run(["docker", "exec", "docker-hive-hive-server-1", "beeline", "-u", "jdbc:hive2://localhost:10000/default", "-e", hive_query], capture_output=True, text=True, check=True)
        print("Validation Data:")
        for line in process.stdout.splitlines()[1:]:
            print(line)
    except subprocess.CalledProcessError as e:
        print(f"Failed to validate data: {e.stderr}")

# Main function to execute the data pipeline
def main():
    if check_url_accessibility(url):
        download_data_locally(url, local_path)
        upload_to_hdfs(local_path, hdfs_path)
        hive_schema=infer_hive_schema(local_path)
        create_hive_table(hive_schema, hive_table_name)
        load_data_into_hive(hive_table_name, hdfs_path)
        validate_data(hive_table_name)
    else:
        print("URL is not accessible")

if __name__ == "__main__":
    main()

