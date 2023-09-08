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
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--type', action='store', default='mayores', help='Mayores (no menores) | menores')
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
    IDS = {}
    for coll in ['place', 'place_menores']:
        incoming_col = db_lnk.db.get_collection(coll)
        for doc in incoming_col.find({}, projection={'id':1, '_id':0}):
            place_id = os.path.basename(doc['id'])
            if args.verbose:
                logging.info(f"Processing {place_id} from {doc['id']}")
            if place_id not in IDS:
                IDS[place_id] = set()
            IDS[place_id].add(doc['id'])
            if len(IDS[place_id]) > 1:
                logging.warning(IDS[place_id])
    for place_id in sorted(IDS):
        print(place_id, IDS[place_id])

if __name__ == "__main__":
    main()