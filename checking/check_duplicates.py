#!/usr/bin/env python
# coding: utf-8
''' Script to build historical on place
    usage: process_place.py [-h] [--ini INI] [--fin FIN] [--id ID]
'''
import sys
import argparse
import logging
import os
import time
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Check versioned documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--collection', action='store', help='Collection to check')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--fix',action='store_true', help='Fix duplicates')

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

    incoming_col = db_lnk.db.get_collection(args.collection)
    max_ids = []
    for group in ['insiders','minors', 'DA', 'tmp_DA']:
        max_ids.append(nu.get_last_order(group, incoming_col))
    logging.info(f"Max ids found: {max_ids}")

    dups_list = list(incoming_col.aggregate([
        {'$group': {'_id': '$_id', 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}}
    ], allowDiskUse=True))

    logging.info(f"Found {len(dups_list)} duplicates")

    for dup in dups_list:
        ntp_id = dup['_id']
        if ntp_id is None:
            continue
        count = dup['count']
        logging.info(f"Checking {ntp_id} with {count} entries")
        base_doc = None
        for doc in incoming_col.find({'_id': ntp_id}):
            if not base_doc:
                base_doc = doc
                logging.info(f"Base doc: {ntp_id} with id {doc['id']}")
                continue
            logging.info(f"Generating new doc for {ntp_id} with id {doc['id']}")
            group = nu.get_group(ntp_id)
            logging.info(f"Group: {group}, new id: {max_ids[group] + 1}")
            old_id = doc['_id']
            new_id = 'ntp{:s}'.format(str(max_ids[group] + 1).zfill(8))
            if args.fix:
                doc['_id'] = new_id
                logging.info(f"Inserting new doc for {doc['id']} at {new_id}")
                logging.info(f"Deleting  {doc['id']} at {old_id}")
                incoming_col.insert_one(doc)
                incoming_col.delete_one({'_id': old_id, 'id': doc['id']})
                max_ids[group] += 1
            else:
                logging.info(f"Would insert  {doc['id']} at {new_id}")
                logging.info(f"Would delete {old_id}")





if __name__ == "__main__":
    main()
