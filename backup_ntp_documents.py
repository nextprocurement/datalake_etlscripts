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
  --no_backup      Do not copy the deleted file on backup bucket
  --group GROUP    insiders|outsiders|minors
  --recover_backup Recover from backup
'''
import sys
import argparse
import logging
import os
import re
import time
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
    backup_files_col = db_lnk.db.get_collection(config['documents_backup_col'] + '.files')

    num_ids = 0
    num_bck = 0
    num_already = 0
    regx = re.compile(r'^ntp')

    for file in files_col.find({'filename':regx}, no_cursor_timeout=True, session=mdb_session):
        if 'place_filename' not in file or not file['place_filename']:
            logging.error(f"File {file['_id']} has no place_filename")
            continue
        logging.info(f"Processing {file['filename']}")
        num_ids += 1
        if backup_storage.file_exists(file['filename']):
            logging.info(f"File {file['filename']} already backed up")
            num_already += 1
            continue
        if not args.dry_run:
            backup_storage.file_store(file['filename'], storage.file_read(file['filename']))
            logging.info(f"Backed up {file['filename']}")
            num_bck += 1            
               
    if args.verbose:
        logging.info(f"Processed {num_ids} entries. {num_bck} backed up, {num_already} already backed up")
if __name__ == "__main__":
    main()
