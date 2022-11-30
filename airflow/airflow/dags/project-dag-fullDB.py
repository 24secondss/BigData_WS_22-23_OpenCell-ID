# -*- coding: utf-8 -*-

"""
Title: Project Dag 
Author: Marcel Fleck
Description: 
Dag to create partitioned full db of cell_towers db, runs once
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}

dag = DAG('cell_towers_full_db', default_args=args, description='DAG to get full_db Data from OpenCellID',
          schedule_interval='@once', start_date=datetime(2022, 11, 21),
          catchup=False, max_active_runs=1)

# ----------- erstellen von Pfaden, download und verschieben von Dateien nach Hadoop ----------

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_opencellid_dir',
    path='/home/airflow',
    directory='opencellid',
    dag=dag,
)

create_local_import_dir_2 = CreateDirectoryOperator(
    task_id='create_opencellid_raw_dir',
    path='/home/airflow/opencellid',
    directory='raw',
    dag=dag,
)

download_cell_towers = HttpDownloadOperator(
    task_id='download_cell_towers',
    download_uri='https://onedrive.live.com/download?cid=6CD9C3F4D2E50BCB&resid=6CD9C3F4D2E50BCB%2159290&authkey=AMinp5rC36d7X4k', #muss durch die offizielle URL getauscht werden!
    save_to='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    dag=dag,
)

unzip_cell_towers = UnzipFileOperator(
    task_id='unzip_cell_towers',
    zip_file='/home/airflow/opencellid/raw/cell_towers.csv.gz',
    extract_to='/home/airflow/opencellid/raw/cell_towers.csv',
    dag=dag,
)

create_hdfs_cell_towers_raw_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_raw_dir',
    directory='/user/hadoop/opencellid/raw',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_hdfs_cell_towers_final_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_cell_towers_final_dir',
    directory='/user/hadoop/opencellid/final',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_tower_cells = HdfsPutFileOperator(
    task_id='upload_tower_cells_to_hdfs',
    local_file='/home/airflow/opencellid/raw/cell_towers.csv',
    remote_file='/user/hadoop/opencellid/raw/cell_towers.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# ---------------------------------------------------------------------------------------------

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
