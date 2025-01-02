#!/usr/bin/env python
# coding: utf-8
''' Script to prepare a listing of old and place ids from duplicated docs
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

    files_col = db_lnk.db.get_collection(config['documents_backup_col'] + '.files')
    ids_col = db_lnk.db.get_collection('docs_name_conv_1')
    buffer = MongoDBBulkWrite(ids_col, CTS['UPSERT'], 10000)
    for file in files_col.find({}, projection={'_id':1, 'filename':1, 'place_filename':1}, no_cursor_timeout=True, session=mdb_session):
        ntp_id, field = file['filename'].split('_', 1)
        col = place_cols[nu.get_group(ntp_id)]
        logging.info(f"Processing {file['filename']}")
        ref_doc = ntp.NtpEntry()
        if not ref_doc.load_from_db(col, ntp_id):
            logging.error(f"Document {ntp_id} not found")
            continue
        if not ref_doc.data['id']:
            logging.error(f"Document {ntp_id} has no place_id")
            continue
        if 'obsolete_version' in ref_doc.data:
            logging.warning(f"Document {ntp_id} is obsolete")

        buffer.append(
            {'_id': file['place_filename']},
            {
                '$addToSet': {'old_filenames': file['filename']},
                '$addToSet': {'place_ids': ref_doc.data['id']}
            }
        )
        buffer.commit_data_if_full()

    buffer.commit_any_data()


if __name__ == "__main__":
    main()
