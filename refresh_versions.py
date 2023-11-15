#!/usr/bin/env python
# coding: utf-8
''' Script to add pointers to final versions
    usage: refresh_versions.py [-h] [--ini INI] [--fin FIN] [--id ID]
'''
import sys
import argparse
import logging
import os
import time
from datetime import datetime
from yaml import load, CLoader
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db


def print_stats(lits):
    stats = {}
    for l in lits:
        if len(lits[l]) not in stats:
            stats[len(lits[l])] = 1
        else:
            stats[len(lits[l])] += 1
    return stats


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--group', action='store', help='Type of procurement')
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
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.group == 'minors':
        logging.info("Type of procurement set to 'minors'")
        clean_col = db_lnk.db.get_collection('place_menores')
    else:
        clean_col = db_lnk.db.get_collection('place')

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not ntp.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{'id':{'$exists':1}}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}

    for doc in clean_col.find({}, {'_id':1, 'id':1, 'updates_dates_list':1}):
        logging.info(f"Processing {doc['_id']}")
        for item in doc['updates_dates_list']:
            logging.debug(item)
            if item[1] == doc['_id']:
                continue
            logging.debug(f"Found old version {item[1]} at {item[0], adding pointer}")
            clean_col.replace_one(
                {'_id': item[1]},
                {
                    'id': doc['id']
                    'obsolete_version': True
                    'updated_to': doc['_id']
                }
            )




if __name__ == "__main__":
    main()
