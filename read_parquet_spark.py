#!/usr/bin/env python
# coding: utf-8

import sys
import argparse
import logging
import json
from os.path import join as opj
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window
from yaml import load, CLoader
import swiftclient as sw
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db

# usage: read_parquet.py [-h] [--drop] codes_file data_dir

# Parse NextProcurement parquets from Spark

# positional arguments:
#   codes_file  Columns sanitized names
#   data_dir    Folder containing Parquet dataset

# optional arguments:
#   -h, --help  show this help message and exit
#   --drop      Clean MongoDB collection
#   --config    Configuration file (default:secrets.yml)
#   --debug     Add Debug information
#   -v --verbode Add extra information
#   --container Swift container name
#   --local     Read from local folder


parser = argparse.ArgumentParser(description='Parse NextProcurement parquets')
parser.add_argument('--drop', action='store_true', help="Clean MongoDB collection")
parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
parser.add_argument('--debug', action='store_true', help="Add Debug information")
parser.add_argument('-v --verbose', action='store_true', help="Add Extra information")
parser.add_argument('--container', action='store', help="Remote container", default="ted-data")
parser.add_argument('--local', action='store_true', help="Read from local folder")
parser.add_argument('codes_file', help="Columns sanitized names")
parser.add_argument('data_dir', help="Parquet Folder")
args = parser.parse_args()

logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')
if args.debug:
    logging.getLogger().setLevel(10)
else:
    logging.getLogger().setLevel(20)

# Config file
with open(args.config)  as config_file:
    config = load(config_file, Loader=CLoader)

logging.info(f"Configuration: {args.config}")
logging.info(f"Parquet:       {args.data_dir}")
logging.info(f"Container:     {args.container}")
logging.info(f"Codes:         {args.codes_file}")
logging.info(f"Read local:    {args.local}")

logging.info("Connecting MongoDB")
db_lnk = Mongo_db(
    config['MONGODB_HOST'],
    'nextprocurement',
    False,
    config['MONGODB_AUTH'],
    credentials=config['MONGODB_CREDENTIALS'],
    connect_db=True
)

incoming_col = db_lnk.db.get_collection('incoming')

if not args.local:
    logging.info("Using Swift storage")
    swift_conn = sw.Connection(
        authurl=config['OS_AUTH_URL'],
        auth_version=3,
        os_options={
            'auth_type': config['OS_AUTH_TYPE'],
            'region_name': config['OS_REGION_NAME'],
            'application_credential_id': config['OS_APPLICATION_CREDENTIAL_ID'],
            'application_credential_secret': config['OS_APPLICATION_CREDENTIAL_SECRET'],
            'service_project_name': 'bsc22NextProcurement'
        }
    )
    sw_storage = ntpst.NtpStorageSwift(
        swift_connection=swift_conn,
        swift_container=args.container,
        swift_prefix=''
    )
#Getting parquet files
if args.local:
    spark_dir = args.data_dir
else:
    spark_dir = sw_storage.get_folder(remote_prefix=args.data_dir)

if args.drop:
    logging.info("Dropping previously stored data")
    incoming_col.drop()
    id_num = 0
else:
    max_id_c = incoming_col.aggregate(
                [{'$group':{'_id':'max_id', 'value':{'$max': '$_id'}}}]
            )
    id_num = ntp.parse_ntp_id(list(max_id_c)[0]['value'])

logging.info(f"Last reference found {id_num}")

spark = SparkSession.builder.master("local")\
    .config('spark.driver.bindAddress','127.0.0.1')\
    .config('spark.mongodb.write.host','localhost')\
    .config('spark.mongodb.database','nextprocurement')\
    .config('spark.mongodb.collection','incoming')\
    .config('spark.mongodb.operationType','insert')\
    .config("spark.mongodb.input.sampleSize", 640000)\
    .getOrCreate()

if not args.local:
    logging.info("Downloading spark parquet files...")
else:
    logging.info("Reading spark parquet files...")
df = spark.read.parquet(opj(spark_dir,'*'))
logging.info(f"found {df.count()} documents from spark dataset")

w = Window().orderBy(lit('A'))
df = df.withColumn("_id", row_number().over(w) + id_num)

def load_data_to_db(row):
    data = row.asDict(True)
    new_data = ntp.NtpEntry()
    new_data.load_data(data['id_num'], data)
    with open("ddd.json", "a") as json_file:
        json_file.write(json.dumps(data))


df.printSchema()
df.write.format("mongodb").mode("append").save()
logging.info(f"Completed {id_num} documents")
