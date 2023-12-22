#!/usr/bin/env python
# coding: utf-8
''' Script to purge documents at gridfs from obsolete versions
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
    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default:secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--no_backup', action='store_true', help='Do not copy the deleted file on backup bucket')
    parser.add_argument('--recover_backup', action='store_true', help='Recover files from backup')
    parser.add_argument('--group', action='store', help='insiders|outsiders|minors')

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
    incoming_col = db_lnk.db.get_collection(config[f'{args.group}_col_prefix'])
    logging.info(f"Selecting collection {incoming_col.name}")

    logging.info(f"Using GridFS storage at {config['MONGODB_HOST']}")
    storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_col']))
    backup_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_backup_col']))
    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    backup_files_col = db_lnk.db.get_collection(config['documents_backup_col'] + '.files')

    if args.verbose:
        logging.info("Getting obsolete ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not ntp.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{'obsolete_version': {'$exists':1}}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}

    num_ids = 0
    num_del = 0

    for doc in list(incoming_col.find(query, {'_id':1, 'obsolete_version':1})):
        ntp_id = doc['_id']
        if not doc['obsolete_version']:
            logging.warning(f"{ntp_id} is not marked as obsolete")
            if args.recover_backup:
                for file in backup_storage.file_list_per_doc(backup_files_col, ntp_id):
                    storage.file_store(file['filename'], backup_storage.file_read(file['filename']))
                    logging.info(f"Recovered {file['filename']}")
                continue
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        for file in storage.file_list_per_doc(files_col, ntp_id):
            if not args.no_backup:
                backup_storage.file_store(file['filename'], storage.file_read(file['filename']))
            storage.delete_file(file['filename'])
            logging.info(f"Deleted {file['filename']}")
            num_del += 1

        num_ids += 1


    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
