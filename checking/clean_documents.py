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


    place_cols = [db_lnk.db.get_collection(config["outsiders_col_prefix"]), db_lnk.db.get_collection(config["minors_col_prefix"])]
    logging.debug(f"Place collections: {place_cols}")
    
    logging.info(f"Using GridFS storage at {config['MONGODB_HOST']}")
    storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_col']))
    backup_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_backup_col']))
    files_col = db_lnk.db.get_collection(config['documents_col'] + '.files')
    
    if args.drop:
        query = {}
        logging.info("Dropping place_filenames")
        files_col.update_many({}, {'$unset': {'place_filename': 1, 'duplicate' : 1}})
    else:
        query = {'place_filename': {'$exists': False}}        
        logging.info("Adding non exisiting place_filenames")
      


    for file in files_col.find(query, no_cursor_timeout=True):
        if not args.drop and 'place_filename' in file and file['place_filename']:
            logging.warning(f"File {file['filename']} already processed")
            continue
        ntp_id, field = file['filename'].split('_', 1)
        logging.debug(f"Processing {ntp_id}")   
        col = place_cols[nu.get_group(ntp_id)]
        ref_doc = ntp.NtpEntry()
        if not ref_doc.load_from_db(col, ntp_id):
            logging.error(f"Document {ntp_id} not found")
            continue
        if not ref_doc.data['id']:
            logging.error(f"Document {ntp_id} has no place_id")
            continue
        place_filename = f"PL{format(int(ref_doc.data['id'].split('/')[-1]), '08d')}_{field}"
        if file['md5']:
            logging.debug(f"Updating {file['filename']} to {place_filename}")   
            files_col.update_one({'_id': file['_id']}, {'$set': {'place_filename': place_filename}})
            for duplicate in files_col.find({'md5': file['md5']}):
                if duplicate['filename'] == file['filename']:
                    continue    
                logging.debug(f"Found duplicated document {duplicate['filename']}, marking as duplicate")
                files_col.update_one(
                    {'_id': duplicate['_id']}, 
                    {'$set': {'place_filename': place_filename, 'duplicate':True}}
                )
        else:
            logging.warning(f"File {file['filename']} has no md5")



if __name__ == "__main__":
    main()
