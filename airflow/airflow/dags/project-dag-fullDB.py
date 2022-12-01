# -*- coding: utf-8 -*-

"""
Title: OpenCelliD FullDB-DAG 
Author: Marcel Fleck (9611872)
Description: 
DAG zum herunterladen der gesamten Datenbank von OpenCelliD und verschieben auf das HDFS.
Beinhaltet einen PySpark-Job zum verarbeiten der Daten (Partitionieren, Bereinigen) und zum kopieren der Daten
zur MariaDB. Dieser DAG läuft nur ein Mal zur Initialisierung der Datenbank.
"""

from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator

args = {
    'owner': 'airflow'
}

dag = DAG('OpenCelliD_full_db', default_args=args, description='DAG to get full_db Data from OpenCelliD',
          schedule_interval='@once', start_date=datetime(2022, 12, 1),
          catchup=False, max_active_runs=1)

# ----------- erstellen von Pfaden, download und verschieben von Dateien nach Hadoop ----------

# -- Erstellen eines grundsätzlichen Verzeichnises für alle OpenCelliD-Daten --
create_local_import_dir = CreateDirectoryOperator(
    task_id='create_opencellid_dir',
    path='/home/airflow',
    directory='opencellid',
    dag=dag,
)

# -- Erstellen eines Verzeichnises für die unbearbeiteten Daten --
create_local_import_dir_2 = CreateDirectoryOperator(
    task_id='create_opencellid_raw_dir',
    path='/home/airflow/opencellid',
    directory='raw',
    dag=dag,
)

# -- Herunterladen der gesamten Datenbank von OpenCelliD --
download_cell_towers = HttpDownloadOperator(
    task_id='download_cell_towers',
    download_uri='https://onedrive.live.com/download?cid=6CD9C3F4D2E50BCB&resid=6CD9C3F4D2E50BCB%2159290&authkey=AMinp5rC36d7X4k', #https://opencellid.org/ocid/downloads?token=pk.7f4a4726c75c79ade7fc194ae9fb99c0&type=full&file=cell_towers.csv.gz
    save_to='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    dag=dag,
)

# -- Entpacken der gesamten Datenbank von OpenCelliD --
unzip_cell_towers = UnzipFileOperator(
    task_id='unzip_cell_towers',
    zip_file='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    extract_to='/home/airflow/opencellid/raw/cell_towers.csv',
    dag=dag,
)

# -- Erstellen eines Basis-Ordners für alle Roh-Daten von OpenCelliD --
create_hdfs_cell_towers_raw_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_raw_dir',
    directory='/user/hadoop/opencellid/raw',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# -- Erstellen eines Basis-Ordners für die bearbeiteten Daten von OpenCelliD --
create_hdfs_cell_towers_final_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_final_dir',
    directory='/user/hadoop/opencellid/final',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# -- Verschieben der gesamten Daten nach HDFS --
hdfs_put_tower_cells = HdfsPutFileOperator(
    task_id='upload_tower_cells_to_hdfs',
    local_file='/home/airflow/opencellid/raw/cell_towers.csv',
    remote_file='/user/hadoop/opencellid/raw/cell_towers.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# ---------------------------------------------------------------------------------------------

# -- PySpark-Job zum Verarbeiten der gesamten Roh-Daten und verschieben der bearbeiteten Daten zur MariaDB --
pyspark_raw_to_final_fullDB_parquet = SparkSubmitOperator(
    task_id='pyspark_raw_to_final_fullDB_parquet',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_full_db_job.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='raw_to_final_fullDB',
    verbose=True,
    application_args=[
        '--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
        '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
        '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
        '--hdfs_source_dir', '/user/hadoop/opencellid/raw',
        '--hdfs_target_dir', '/user/hadoop/opencellid/final/',
    ],
    dag=dag
)

# -------------------- Ausfuerung/Dag-Ablauf --------------------
create_local_import_dir >> create_local_import_dir_2 
create_local_import_dir_2 >> create_hdfs_cell_towers_raw_dir >> create_hdfs_cell_towers_final_dir
create_local_import_dir_2 >> download_cell_towers >> unzip_cell_towers >> hdfs_put_tower_cells

hdfs_put_tower_cells >> pyspark_raw_to_final_fullDB_parquet
# ---------------------------------------------------------------

