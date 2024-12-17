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

    print(args)
    for file in files_col.find({}, no_cursor_timeout=True, session=mdb_session):
        if 'place_filename' not in file or not file['place_filename']:
            logging.error(f"File {file['_id']} has no place_filename")
            continue
        logging.info(f"Processing {file['filename']}")
        if file['duplicate']:
            logging.info(f"File {file['filename']} is a duplicate")
            if not args.dry_run:
                backup_storage.file_store(file['filename'], storage.file_read(file['filename']))
                storage.delete_file(file['filename'])
                logging.info(f"Deleted {file['filename']}")
            else:
                logging.info(f"Would delete {file['filename']}")
        else:
            new_file = file.copy()






        # ntp_id = doc['_id']
        # if not doc['obsolete_version']:
        #     logging.warning(f"{ntp_id} is not marked as obsolete")
        #     if args.recover_backup:
        #         for file in backup_storage.file_list_per_doc(backup_files_col, ntp_id):
        #             if not args.dry_run:
        #                 storage.file_store(file['filename'], backup_storage.file_read(file['filename']))
        #             logging.info(f"Recovered {file['filename']}")
        #         continue
        # if args.verbose:
        #     logging.info(f'Processing {ntp_id}')
        # for file in storage.file_list_per_doc(files_col, ntp_id):
        #     if not args.no_backup:
        #         if not args.dry_run:
        #             backup_storage.file_store(file['filename'], storage.file_read(file['filename']))
        #     if not args.dry_run:
        #         storage.delete_file(file['filename'])
        #     logging.info(f"Deleted {file['filename']}")
        #     num_del += 1

        # num_ids += 1


    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
