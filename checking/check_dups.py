#!/usr/bin/env python
# coding: utf-8
''' Script to prepare a collection of most recent data for each tender (PLACE)
    usage: clean_place.py [-h] [--ini INI] [--fin FIN] [--id ID]
'''
import sys
import argparse
import logging
import os
import time
import json
from datetime import datetime
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_storage as ntpst, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db
from mmb_data.mongo_db_bulk_write import MongoDBBulkWrite, CTS

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')

    args = parser.parse_args()
    # Setup logging
    logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')
    if args.debug:
        logging.getLogger().setLevel(10)
    else:
        logging.getLogger().setLevel(20)

    # Config file
    with open(args.config, 'r')  as config_file:
        config = load(config_file, Loader=CLoader)

    logging.info(f"Connecting to MongoDB at {config['MONGODB_HOST']}")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    mdb_session = db_lnk.client.start_session()
    logging.info("Starting mongodb session")
    place_cols = [db_lnk.db.get_collection(config["outsiders_col_prefix"]), db_lnk.db.get_collection(config["minors_col_prefix"])]
    logging.debug(f"Place collections: {place_cols}")

    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    buffer = MongoDBBulkWrite(files_col, CTS['UPDATE'], 10000)

    valid_fields  = list(config['STORE_DOC_NAMES'].values())
    logging.debug(f"Accepted filenames: {valid_fields}")


    for md5_data in files_col.aggregate([{"$group": {"_id":"$md5", "total":{"$sum":1}}}], session=mdb_session):
        
        if md5_data['total'] == 1:
            continue
        dups = 0
        for doc in files_col.find({"md5": md5_data['_id']}, session=mdb_session):
            if 'duplicate' in doc and doc['duplicate']:
                dups += 1
        if dups == md5_data['total']:
            logging.info(f"{md5_data['_id']} all duplicates")
        if md5_data['total'] - dups > 1:
            logging.info(f"{md5_data['_id']} missing duplicates {md5_data['total'] - dups - 1}")
        if md5_data['total'] - dups == 1:
            logging.info(f"{md5_data['_id']} ok")
        
if __name__ == "__main__":
    main()
