#!/usr/bin/env python
# coding: utf-8

import sys
import argparse
import logging
import re
import pandas as pd
from yaml import load, CLoader
import ntp_entry as ntp
from mmb_data.mongo_db_connect import Mongo_db

def main():
    # usage: load_cpv.py cpv.csv
    #   csv_file    Parquet file

    # optional arguments:
    #   -h, --help  show this help message and exit
    #   --drop      Clean MongoDB collection
    #   --config    Configuration file (default:secrets.yml)
    #   --debug     Add Debug information from different components
    #   -v --verbose Add additional information

    parser = argparse.ArgumentParser(description='Parse NextProcurement parquets')
    parser.add_argument('--drop', action='store_true', help="Clean MongoDB collection")
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")

    parser.add_argument('csv_file', help="CSV file")
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
    logging.info(f"CSV:           {args.csv_file}")

    logging.info("Connecting MongoDB")
    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    cpv_col = db_lnk.db.get_collection('cpv')
    with open(args.csv_file,'r') as csv:
        headers = []
        for line in csv:
            line = line.strip()
            if not headers:
                headers = line.split(';')
                print(headers)
                continue
            line = line.replace("'","")
            data = line.split(';')
            data[1] = int(data[1])
            obj = {
                '_id' : data[0],
                'control': data[1],
                'descripcion': data[2]
            }
            cpv_col.insert_one(obj)


        
if __name__ == "__main__":
    main()
