import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.types import *
from pyspark.sql.functions import *

def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='PySpark-Job to partition, filter and copy data to end-user-db (mariaDB)')
    parser.add_argument('--year', help='Partion Year To Process, e.g. 2019', required=True, type=str)
    parser.add_argument('--month', help='Partion Month To Process, e.g. 10', required=True, type=str)
    parser.add_argument('--day', help='Partion Day To Process, e.g. 31', required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/imdb', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/imdb/ratings', required=True, type=str)
    
    return parser.parse_args()

# Parse Command Line Args
args = get_args()

# Initialize Spark Context
sc = pyspark.SparkContext()
spark = SparkSession(sc)

# create DB-Schema for later transport to MariaDB
db_schema = StructType(
        [
            StructField("radio", StringType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("net", IntegerType(), True),
            StructField("area", IntegerType(), True),
            StructField("cell", IntegerType(), True),
            StructField("unit", IntegerType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("range", IntegerType(), True),
            StructField("samples", IntegerType(), True),
            StructField("changeable", IntegerType(), True),
            StructField("created", IntegerType(), True),
            StructField("updated", IntegerType(), True),
            StructField("averageSignal", IntegerType(), True)
        ])

# read cell_towers.csv
full_cell_towers_db = spark.read.format("csv").options(
    header="true",
    delimiter=",",
    nullValue="null",
    inferschema="false"
).schema(db_schema).load(args.hdfs_source_dir + '/cell_towers' + ".csv")

# filter relevant columns
# We only need radiotype, latitude, longitude, range and averageSignal for Task
relevant_full_cell_towers_db = full_cell_towers_db.select("radio", "lat", "lon", "range").filter(full_cell_towers_db.radio != "NR")
relevant_full_cell_towers_db = relevant_full_cell_towers_db.dropna()

# partition by radio-type and write to HDFS
relevant_full_cell_towers_db.repartition('radio').write.format("parquet").mode("append").option(
        "path", args.hdfs_target_dir).partitionBy("radio").saveAsTable("fulltable_lat_lon_range")


partition_GSM = spark.read.parquet(args.hdfs_target_dir + "radio=GSM")
partition_GSM.write.format('jdbc').options(
    url='jdbc:mysql://mariaDB:3306/ocid_cell_tower?permitMysqlScheme',
    driver='org.mariadb.jdbc.Driver',
    dbtable='partition_GSM',
    user='root',
    password='root'
).mode('overwrite').save()

partition_UMTS = spark.read.parquet(args.hdfs_target_dir + "radio=UMTS")
partition_UMTS.write.format('jdbc').options(
    url='jdbc:mysql://mariaDB:3306/ocid_cell_tower?permitMysqlScheme',
    driver='org.mariadb.jdbc.Driver',
    dbtable='partition_UMTS',
    user='root',
    password='root'
).mode('overwrite').save()

partition_CDMA = spark.read.parquet(args.hdfs_target_dir + "radio=CDMA")
partition_CDMA.write.format('jdbc').options(
    url='jdbc:mysql://mariaDB:3306/ocid_cell_tower?permitMysqlScheme',
    driver='org.mariadb.jdbc.Driver',
    dbtable='partition_CDMA',
    user='root',
    password='root'
).mode('overwrite').save()

partition_LTE = spark.read.parquet(args.hdfs_target_dir + "radio=LTE")
partition_LTE.write.format('jdbc').options(
    url='jdbc:mysql://mariaDB:3306/ocid_cell_tower?permitMysqlScheme',
    driver='org.mariadb.jdbc.Driver',
    dbtable='partition_LTE',
    user='root',
    password='root'
).mode('overwrite').save()
