# -*- coding: utf-8 -*-

"""
Title: OpenCelliD Diff-DAG 
Author: Marcel Fleck (9611872)
Description: 
DAG zum Herunterladen von Diff-Dateien von OpenCelliD und verschieben dieser nach HDFS.
Beinhaltet PySpark-Job zum Verarbeiten der Daten (Partitionieren, Bereinigen) und zum Kopieren der Daten
zur MariaDB. Dieser DAG läuft ein Mal pro Tag um 6 Uhr morgens.
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

dag = DAG('OpenCelliD_diffs_db', default_args=args, description='DAG to get diff_data from OpenCelliD',
          schedule_interval='00 06 * * *',
          start_date=datetime(2022, 12, 1), catchup=False, max_active_runs=1)

# ----------- erstellen von Pfaden, download und verschieben von Dateien nach Hadoop ----------

# -- Erstellen eines Verzeichnisses für die täglichen unbearbeiteten DIffs --
create_local_diff_dir = CreateDirectoryOperator(
    task_id='create_local_diff_dir',
    path='/home/airflow/opencellid/raw',
    directory='diff',
    dag=dag,
)

# -- Herunterladen der neuesten Diff-Datei von OpenCelliD --
download_diff = HttpDownloadOperator(
    task_id='download_diff',
    download_uri='https://opencellid.org/ocid/downloads?token=pk.7f4a4726c75c79ade7fc194ae9fb99c0&type=diff&file=OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    save_to='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    dag=dag,
)

# -- Entpacken der Datei --
unzip_diff = UnzipFileOperator(
    task_id='unzip_diff',
    zip_file='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    extract_to='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    dag=dag,
)

# -- Erstellen eines Verzeichnises für unverarbeitete Diffs auf Hadoop --
create_hdfs_diff_partition_dir_raw = HdfsMkdirFileOperator(
    task_id='create_hdfs_diff_partition_dir',
    directory='/user/hadoop/opencellid/raw/diff',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# -- Verschieben der Diff-Dateien auf HDFS --
hdfs_put_diffs = HdfsPutFileOperator(
    task_id='hdfs_put_diffs',
    local_file='/home/airflow/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    remote_file='/user/hadoop/opencellid/raw/diff/OCID-diff-cell-export-{{ ds }}-T000000.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# ---------------------------------------------------------------------------------------------

# -- PySpark-Job zum Verarbeiten der Diff-raw-Daten und verschieben der bearbeiteten Daten zur MariaDB --
pyspark_raw_to_final_diffs_parquet = SparkSubmitOperator(
    task_id='pyspark_raw_to_final_diffs_parquet',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_diff_job.py',
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
create_local_diff_dir >> download_diff >> unzip_diff
create_hdfs_diff_partition_dir_raw

unzip_diff >> hdfs_put_diffs

hdfs_put_diffs >> pyspark_raw_to_final_diffs_parquet
# ---------------------------------------------------------------
