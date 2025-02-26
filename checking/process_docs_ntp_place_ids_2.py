#!/usr/bin/env python
# coding: utf-8
''' Script to prepare a listing of old and place ids from duplicated docs (version 2. adapted to already processed PL docs)
    usage: process_docs_ntp_place_ids.py [-h]
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

    parser = argparse.ArgumentParser(description='Reference documents file names conversion')
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
        config['MONGODB_DB'],
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    mdb_session = db_lnk.client.start_session()
    logging.info("Starting mongodb session")
    place_cols = [
        db_lnk.db.get_collection(config["outsiders_col_prefix"]),
        db_lnk.db.get_collection(config["minors_col_prefix"])
    ]
    logging.debug(f"Place collections: {place_cols}")

    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    backup_files_col = db_lnk.db.get_collection(config['documents_backup_col'] + '.files')
    ids_col = db_lnk.db.get_collection('docs_name_conv')
    buffer = MongoDBBulkWrite(ids_col, CTS['UPSERT'], 10)
    to_process = list(files_col.find({}, projection={'_id':1, 'filename':1, 'md5':1}, no_cursor_timeout=True, session=mdb_session))
    logging.info(f"Found {len(to_process)} files to process")

    for file in to_process:
        print(file)
        place_id, field = file['filename'].split('_', 1)
        place_id = int(place_id[2:])
        col = place_cols[0] # All docs share place collection
        logging.info(f"Processing {file['filename']}")
        ntp_id = None
        for pref in config['uri_prefixes']:
            print(f"{pref}/{place_id}")
            ntp_id = nu.get_active_version(f"{pref}/{place_id}", col)
            if ntp_id:
                logging.info(f"Valid place uri found at {pref}/{place_id}")
                break
        ref_doc = ntp.NtpEntry()
        if ntp_id is None or not ref_doc.load_from_db(col, ntp_id):
            logging.error(f"Document {place_id} not found")
            sys.exit()
            continue
        logging.info(f"Document found at {ntp_id}")
        # Check MD5
        if 'md5' not in file:
            logging.warning("MD5 not found, skipping")
            continue

        same_md5_curr = list(files_col.find({'md5':file['md5']}))
        if len(same_md5_curr) > 1:
            logging.warning(f"Found {len(same_md5_curr)} current filenames")
            for dup_file in same_md5_curr:
                logging.warning(dup_file['filename'])

        same_md5_bck = list(backup_files_col.find({'md5':file['md5']}))
        logging.info(f"Found {len(same_md5_bck)} old_filenames")
        for old_file in same_md5_bck:
            old_ntp_id, fields = old_file['filename'].split('_', 1)
            old_doc = ntp.NtpEntry()
            old_doc.load_from_db(col, old_ntp_id)
            logging.info(old_file['filename'])
            buffer.append(
                {'_id': file['filename']},
                {
                    '$addToSet': {'old_filenames': old_file['filename']},
                }
            )
            buffer.append(
                {'_id': file['filename']},
                {
                    '$addToSet': {'place_ids': old_doc.data['id']}
                }

            )
        buffer.commit_data_if_full()

    buffer.commit_any_data()


if __name__ == "__main__":
    main()
