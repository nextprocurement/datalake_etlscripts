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
    parser.add_argument('--output',action='store', help='Output filename index')

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
    
    processed_docs = {}
    docs_id_index = {}
    new_files = set()

    for file in files_col.find():
        id, field = file['filename'].split('_', 1)
        if id not in processed_docs:
            logging.debug(f"Processing {id}")
            col = place_cols[nu.get_group(id)]
            ref_doc = ntp.NtpEntry()
            ref_doc.load_from_db(col, id)
            if ref_doc.is_obsolete():
                active_doc_id = nu.get_active_version(ref_doc.data['id'], col)
                logging.warning(f"Document {id} is obsolete, active version is {active_doc_id}")
                ref_doc.load_from_db(col, active_doc_id)
                if not ref_doc:
                    logging.error(f"Active version {active_doc_id} not found")
                    continue
            else:
                logging.info(f"Document {id} is active")
            processed_docs[id] = ref_doc.ntp_id
        else:
            logging.debug(f"Document {id} already found as {processed_docs[id]}")
            ref_doc = ntp.NtpEntry()
            ref_doc.load_from_db(col, processed_docs[id])
    
        if ref_doc.data['id'] in new_ids:
            logging.warning(f"Found Duplicated file at {ref_doc.data['id']}")
        versions = set([x['_id'] for x in nu.get_versions(ref_doc.data['id'], col)])

        new_id = f"PL{format(int(ref_doc.data['id'].split('/')[-1]), '08d')}"
        new_filename = f"{new_id}_{field}"
        if new_filename in new_files:
            logging.warning(f"Found duplicated {new_filename}")
        new_files.add(new_filename)

        print(json.dumps({'id':new_id, 'versions':sorted(list(versions))}))
        docs_id_index[file['filename']] = new_filename


    with open('args.output', "w") as output_file:
        for old_filename, new_filename in docs_id_index.items():
            print(f"{old_filename} {docs_id_index[new_filename]}", file=output_file)


if __name__ == "__main__":
    main()
