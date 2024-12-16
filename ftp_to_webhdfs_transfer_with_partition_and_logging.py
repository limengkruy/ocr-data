from airflow import DAG
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.hive.operators.hive import HiveOperator  # Import HiveOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import uuid
import pandas as pd  # Import pandas for data manipulation
import happybase  # HBase library

# Function to log the process to HBase (instead of CSV)
def log_transfer_status_to_hbase(file_name, status, table_name='transfer_logs'):
    # Connect to HBase using HappyBase
    connection = happybase.Connection('sandbox-hdp', port=9090)  # HBase master and ZK port
    connection.open()
    
    # Define the table and insert log into it
    table = connection.table(table_name)
    print(f"{file_name}-{status} Logging")
    # Prepare log data
    row_key = str(uuid.uuid4())  # Generate a unique row key
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_data = {
        'log_data:timestamp': timestamp,
        'log_data:file_name': file_name,
        'log_data:status': status
    }
    
    # Insert the log data into HBase
    table.put(row_key, log_data)
    connection.close()
    print(f"Log inserted into HBase for {file_name} with status: {status}")

# Function to upload file to WebHDFS
def upload_to_hdfs(local_file_path, hdfs_file_path):
    # Create a WebHDFSHook object
    webhdfs_hook = WebHDFSHook(webhdfs_conn_id='hdfs_default')

    # Use the hook to upload the file to HDFS
    webhdfs_hook.load_file(local_file_path, hdfs_file_path, overwrite=True)
    print(f"File {local_file_path} uploaded to {hdfs_file_path}")
    os.remove(local_file_path)
    print(f"File {local_file_path} deleted")

# Function to download file from FTP and filter by folder name and filename
def download_file_from_ftp(folder_name):
    ftp_hook = FTPHook(ftp_conn_id='ftp-server-connection')  # Airflow connection ID for your FTP server
    remote_folder_path = f'/userfile/{folder_name}/'  # Path to the folder on FTP server
    local_folder_path = f'/home/airflow/ftp_download/{folder_name}/'  # Local path to save file

    # Create local folder if it doesn't exist
    os.makedirs(local_folder_path, exist_ok=True)

    # List files in the folder
    remote_files = ftp_hook.list_directory(remote_folder_path)
    print(f"Found files in {remote_folder_path}: {remote_files}")
    
    # Filter files that start with the folder name (e.g., order-123.csv)
    local_files = []
    for remote_file in remote_files:
        file_name = os.path.basename(remote_file)  # Extract only the file name
        print(f"Checking file: {file_name}")
        if file_name.startswith(folder_name):  # Only process files that start with folder_name
            local_file_path = os.path.join(local_folder_path, file_name)
            ftp_hook.retrieve_file(remote_file, local_file_path)
            print(f"Downloaded file from FTP: {remote_file} to {local_file_path}")
            local_files.append(local_file_path)
            # return local_file_path
        else:
            print(f"File: {remote_file} not match for {folder_name}")
    if(local_files):
        return local_files
    print(f"No file found in {remote_folder_path} that starts with {folder_name}")
    return []

# Function to delete file from FTP after successful transfer
def delete_file_from_ftp(folder_name, file_name):
    ftp_hook = FTPHook(ftp_conn_id='ftp-server-connection')  # Airflow connection ID for your FTP server
    remote_file_path = f'/userfile/{folder_name}/{file_name}'  # Full path to the file on FTP server
    print(f"Trying to delete {file_name} in {folder_name}")
    # Delete the file from the FTP server
    ftp_hook.delete_file(remote_file_path)
    print(f"Deleted file from FTP: {remote_file_path}")

# Function to clean up CSV by removing columns without headers (like index)
def clean_csv(file_path):
    df = pd.read_csv(file_path)

    print(f"Start Clean CSV file: {file_path}")
    # Drop columns that don't have a valid header
    valid_columns = [col for col in df.columns if not col.startswith('Unnamed')]
    df = df[valid_columns]

    # Save the cleaned data back to CSV
    cleaned_file_path = file_path.replace('.csv', '-cleaned.csv')
    df.to_csv(cleaned_file_path, index=False)
    
    print(f"Cleaned CSV file saved at: {cleaned_file_path}")
    os.remove(file_path)
    return cleaned_file_path

# Define your DAG
dag = DAG(
    'ftp_to_webhdfs_transfer_with_partition_and_logging',
    start_date=datetime(2024, 12, 6),
    schedule_interval="@daily",  # Runs every minute for testing (can change later to hourly)
    catchup=False,  # Prevent DAG from running for past dates
)

# Function for logging after FTP file retrieval
def log_ftp_retrieve_status(folder_name, local_file_path):
    if local_file_path:
        log_transfer_status_to_hbase(local_file_path, f"retrieved_from_ftp_{folder_name}")
    else:
        log_transfer_status_to_hbase(folder_name, f"no_files_found_{folder_name}")

# Function for logging after file upload to HDFS
def log_hdfs_upload_status(folder_name, local_file_path, hdfs_file_path):
    log_transfer_status_to_hbase(local_file_path, f"uploaded_to_hdfs_{folder_name}:{hdfs_file_path}")

# FTP Task: Download, upload files, and delete from FTP after transfer
def process_folder_file(folder_name):
    local_file_path_s = download_file_from_ftp(folder_name)  # Download the file from FTP
    
    for local_file_path in local_file_path_s:
        if local_file_path:
            # Clean the file (remove unwanted columns)
            cleaned_file_path = clean_csv(local_file_path)
            
            # Upload file to HDFS with timestamp suffix
            upload_date = datetime.now().strftime("%Y%m%d")
            hdfs_file_path = f'/user/hdfs/userfile/{folder_name}/upload_date={upload_date}/{folder_name}-{upload_date}-{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
            upload_to_hdfs(cleaned_file_path, hdfs_file_path)

            # Log the transfer after uploading to HDFS
            log_hdfs_upload_status(folder_name, cleaned_file_path, hdfs_file_path)

            # Log the retrieval status after downloading from FTP
            log_ftp_retrieve_status(folder_name, local_file_path)

            # Delete the file from FTP after successful transfer
            delete_file_from_ftp(folder_name, os.path.basename(local_file_path))
        else:
            # Log if no file is found
            log_ftp_retrieve_status(folder_name, None)
    if(not local_file_path_s):
        log_hdfs_upload_status(folder_name,"No File", "None")

# Create individual tasks for each folder (product, employee, customer, location, order)
for folder_name in ['product', 'employee', 'customer', 'location', 'order']:
    process_task = PythonOperator(
        task_id=f'process_{folder_name}_file',
        python_callable=process_folder_file,
        op_args=[folder_name],
        dag=dag,
    )
    process_task
