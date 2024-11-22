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

def is_valid_field(valid_fields, field):
    field = field.split('.')[0].split(':')[0]
    print(field)
    if field in valid_fields:
        return field
    return False

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--drop', action='store_true', help='drop added info')

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
    place_cols = [
        db_lnk.db.get_collection(config["outsiders_col_prefix"]), 
        db_lnk.db.get_collection(config["minors_col_prefix"])
    ]
    logging.debug(f"Place collections: {place_cols}")

    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    buffer = MongoDBBulkWrite(files_col, CTS['UPDATE'], 10000)

    if args.drop:
        query = {}
        logging.info("Dropping place_filenames")
        files_col.update_many({}, {'$unset': {'place_filename': 1, 'duplicate' : 1}})
    else:
        query = {'place_filename': {'$exists': False}}
        logging.info("Adding non existing place_filenames")

    valid_fields  = list(config['STORE_DOC_NAMES'].values())
    logging.debug(f"Accepted filenames: {valid_fields}")

    processed_docs = set()
    for file in files_col.find(query, no_cursor_timeout=True, session=mdb_session):
        if file['filename'] in processed_docs:
            logging.warning(f"File {file['filename']} already processed")
            continue

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
        doc_versions = []
        field_ok = is_valid_field(valid_fields, field)
        if field_ok:
            logging.debug(f"Field {field_ok} is acceptable")
        else: 
            logging.debug(f"Field {field} is not acceptable")
        if file['md5']:
            for duplicate in files_col.find({'md5': file['md5']}):
                if field_ok and duplicate['filename'] == file['filename']:
                    file['duplicate'] = False
                    continue
                logging.info(f"Found duplicated document {duplicate['filename']}")
                dup_id, dup_field = duplicate['filename'].split('_', 1)
                logging.debug(f"checking {dup_field}")
                if not field_ok and is_valid_field(valid_fields, dup_field):
                    field_ok = dup_field
                    logging.debug(f"Accepted field is {field_ok}")
                    duplicate['duplicate'] = False
                    continue
                duplicate['duplicate']=True
                doc_versions.append(duplicate)
        else:
            logging.warning(f"File {file['filename']} has no md5")
            field_ok = field

        if not field_ok:
            logging.error(f"Valid field name not found for {file['filename']}")
            continue

        place_filename = f"PL{format(int(ref_doc.data['id'].split('/')[-1]), '08d')}_{field_ok}"
        logging.info(f"Updating {file['filename']} to {place_filename}")
        buffer.append(
            {'_id': file['_id']},
            {'$set': {'place_filename': place_filename}}
        )
        for duplicate in doc_versions:
            logging.info(f"Updating {duplicate['filename']} to {place_filename}")
            buffer.append(
                {'_id': duplicate['_id']},
                {'$set': {'place_filename': place_filename, 'duplicate': duplicate['duplicate']}}
            )
            processed_docs.add(duplicate['filename'])
        buffer.commit_data_if_full()
        processed_docs.add(file['filename'])

    buffer.commit_any_data()


if __name__ == "__main__":
    main()
