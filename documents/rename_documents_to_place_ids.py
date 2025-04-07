#!/usr/bin/env python
# coding: utf-8
''' Script to purge documents at gridfs from obsolete and duplicated versions, and rename then to PLXXXXXXXXX ids
    usage: purge_documents.py [-h] [--ini INI] [--fin FIN] [--id ID]
                        [--config CONFIG] [-v] [--debug] [--no_backup]
                        [--group GROUP]

Download documents

options:
  -h, --help       show this help message and exit
  --ini INI        Initial document range
  --fin FIN        Final document range
  --id ID          Selected document id
  --config CONFIG  Configuration file (default: secrets.yml)
  -v, --verbose    Extra progress information
  --debug          Extra debug information
'''
import sys
import argparse
import logging
import os
import time
import re
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_storage as ntpst, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db

def main():
    ''' Main '''
    parser = argparse.ArgumentParser(description='Purge documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default:secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--dry_run', action='store_true', help='DO not change files, just check')

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

    logging.info(f"Using GridFS storage at {config['MONGODB_HOST']}")
    storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_col']))
    backup_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_backup_col']))
    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    files_bck_col = db_lnk.db.get_collection(config['documents_backup_col'] + '.files')

    regex = re.compile("^PL")
    processed_docs_raw = list(files_col.find({'filename':regex}, {'filename':1, '_id':0 }))
    processed_docs = set(x['filename'] for x in processed_docs_raw)
    logging.info(f"Found {len(processed_docs)} processed docs")
    docs_to_process = list(files_bck_col.find({}, {'filename':1, 'place_filename':1, 'md5':1, '_id':0 })) 
    logging.info(f"Found {len(docs_to_process)} docs to process")
    num_proc = 0
    num_del = 0
    num_ren = 0
    num_err = 0
    #for file in files_bck_col.find({}, no_cursor_timeout=True, session=mdb_session):
    for file in docs_to_process:
        if 'place_filename' not in file or not file['place_filename']:
            logging.error(f"File {file['_id']} has no place_filename")
            num_err += 1
            continue
        if file['place_filename'] in processed_docs:
            logging.info(f"File {file['place_filename']} already processed")
            continue
        num_proc += 1
        logging.info(f"Processing {file['filename']}")
        num_ren +=1
        if not args.dry_run:
            storage.file_store(
                file['place_filename'], 
                backup_storage.file_read(file['filename']), 
                md5_checksum=file['md5']
            )
            logging.info(f"{file['filename']} stored to {file['place_filename']}")
        else:
            logging.info(f"{file['filename']} would be stored to {file['place_filename']} (--dry_run)")
        processed_docs.add(file['place_filename'])

    if args.dry_run:
        logging.info(f"{num_proc} processed, {num_ren} renamed, {num_del} deleted, {num_err} errors")
    else:
        logging.info(f"{num_proc} processed, {num_ren} would be renamed, {num_del} would be deleted, {num_err} errors")
    logging.info(f"Done")

if __name__ == "__main__":
    main()
