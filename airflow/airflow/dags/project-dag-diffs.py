# -*- coding: utf-8 -*-

"""
Title: Project Dag 
Author: Marcel Fleck
Description: 
Dag to create partitioned dbs of diffs and merges them to full db, runs daily

TO DO: Links zu download von diffs changen: https://onedrive.live.com/download?cid=6CD9C3F4D2E50BCB&resid=6CD9C3F4D2E50BCB%2159291&authkey=ANu-4_qT3NxqPqo
       bei produktiv: original-link mit Accesstoken und OCID-diff-cell-export-{{ ds }}-T000000.csv.gz' hinten fuer richtigen Dateidownload
       https://opencellid.org/ocid/downloads?token=pk.7f4a4726c75c79ade7fc194ae9fb99c0&type=diff&file=OCID-diff-cell-export-2022-11-29-T000000.csv.gz
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

dag = DAG('cell_towers_create_diffs_db', default_args=args, description='DAG to get diff_data from OpenCellID',
          schedule_interval='56 18 * * *',
          start_date=datetime(2022, 11, 21), catchup=False, max_active_runs=1)

# ----------- erstellen von Pfaden, download und verschieben von Dateien nach Hadoop ----------

create_local_diff_dir = CreateDirectoryOperator(
    task_id='create_local_diff_dir',
    path='/home/airflow/opencellid/raw',
    directory='diff',
    dag=dag,
)

clear_local_diff_dir = ClearDirectoryOperator(
    task_id='clear_local_diff_dir',
    directory='/home/airflow/opencellid/raw/diff',
    pattern='*',
    dag=dag,
)

download_diff = HttpDownloadOperator(
    task_id='download_diff',
    download_uri='https://opencellid.org/ocid/downloads?token=pk.7f4a4726c75c79ade7fc194ae9fb99c0&type=diff&file=OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    save_to='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    dag=dag,
)

unzip_diff = UnzipFileOperator(
    task_id='unzip_diff',
    zip_file='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    extract_to='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    dag=dag,
)

create_hdfs_diff_partition_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_diff_partition_dir',
    directory='/user/hadoop/opencellid/raw/diff',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_hdfs_diff_partition_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_diff_partition_dir',
    directory='/user/hadoop/opencellid/final/diff',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_tower_cells = HdfsPutFileOperator(
    task_id='hdfs_put_tower_cells',
    local_file='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    remote_file='/user/hadoop/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# ---------------------------------------------------------------------------------------------

pyspark_raw_to_final_diffs_parquet = SparkSubmitOperator(
    task_id='pyspark_raw_to_final_diffs_parquet',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_daily_job.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='raw_to_final_diffs',
    verbose=True,
    application_args=[
        '--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
        '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
        '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
        '--hdfs_source_dir', '/user/hadoop/opencellid/raw/diff',
        '--hdfs_target_dir', '/user/hadoop/opencellid/final/diff',
    ],
    dag=dag
)


# -------------------- Ausfuerung/Dag-Ablauf --------------------
create_local_diff_dir >> clear_local_diff_dir >> download_diff >> unzip_diff
create_hdfs_diff_partition_dir

unzip_diff >> hdfs_put_tower_cells

hdfs_put_tower_cells >> pyspark_raw_to_final_diffs_parquet

# ---------------------------------------------------------------
