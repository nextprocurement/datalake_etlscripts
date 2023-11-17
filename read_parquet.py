#!/usr/bin/env python
# coding: utf-8

import sys
import argparse
import logging
import re
import pandas as pd
from yaml import load, CLoader
from nextplib import ntp_entry as ntp
from mmb_data.mongo_db_connect import Mongo_db

def main():
    # usage: read_parquet.py [-h] [--drop] codes_file pkt_file

    # Parse NextProcurement parquets

    # positional arguments:
    #   codes_file  Columns sanitized names
    #   pkt_file    Parquet file

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
    parser.add_argument('--group', action='store', help="outsiders|minors|insiders", required=True)
    parser.add_argument('--upsert', action='store_true', help="update existing atom or insert a new one")

    parser.add_argument('codes_file', help="Columns sanitized names")
    parser.add_argument('pkt_file', help="Parquet file")
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
    logging.info(f"Parquet:       {args.pkt_file}")
    logging.info(f"Codes:         {args.codes_file}")
    logging.info(f"Group:         {args.group}")

    logging.info(f"Connecting MongoDB at {config['MONGODB_HOST']}")
    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )

    if args.group in ['outsiders', 'insiders']:
        incoming_col = db_lnk.db.get_collection('place_raw')
    else:
        incoming_col = db_lnk.db.get_collection('place_menores_raw')

    data_table = pd.read_parquet(args.pkt_file, use_nullable_dtypes=True)
    new_cols = pd.read_csv(args.codes_file, sep='\t', index_col='ORIGINAL')

    if args.group in ['outsiders', 'insiders']:
        id_num = 0
    else:
        id_num = 10000000

    if args.drop:
        logging.info("Dropping previously stored data")
        incoming_col.drop()
    else:
        if args.group in ['outsiders', 'insiders']:
            cond = {'_id': {'$regex': 'ntp0'}}
        else:
            cond = {'_id': {'$regex': 'ntp1'}}

        max_id_c = incoming_col.aggregate(
                    [
                        {'$match': cond},
                        {'$group':{'_id':'max_id', 'value':{'$max': '$_id'}}}
                    ]
                )
        max_id = list(max_id_c)
        if max_id:
            id_num = ntp.parse_ntp_id(max_id[0]['value'])
        else:
            logging.info("No records found")

    logging.info(f"Last reference found {id_num}")
    # print(args)
    n_procs = 0
    for i in range(len(data_table.index)):
        data_row = data_table.iloc[i].to_dict().copy()
        new_data = ntp.NtpEntry()
        new_data.load_data(id_num + 1, ntp.parse_parquet(data_row, new_cols))
        tmp_num = new_data.commit_to_db(incoming_col, upsert=args.upsert)
        id_num = max(tmp_num, id_num)
        if args.verbose:
            logging.info(f"Processed {new_data.ntp_id}")
        n_procs += 1
    logging.info(f"Completed {n_procs} documents")

if __name__ == "__main__":
    main()
