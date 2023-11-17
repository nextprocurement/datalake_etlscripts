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
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('fields', help="Fields to replace")

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
    incoming_col = db_lnk.db.get_collection('place')

    if args.verbose:
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not ntp.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}

    with open(args.fields, "r") as fields_file:
        for line in fields_file:
            old, new = line.strip().split('\t')
            logging.info(f"Processing {old} -> {new}")
            for doc in incoming_col.find({old : {'$exists': 1}}):
                logging.info(f"{doc['_id']}: Found old {old}:'{doc[old]}'")
                if new in doc and doc[new]:
                    logging.info(f"{doc['_id']}: Found new {new}='{doc[new]}', no update")
                    incoming_col.update_one(
                        {'_id': doc['_id']},
                        {'$unset': {old: 1}}
                    )
                else:
                    incoming_col.update_one(
                        {'_id': doc['_id']},
                        {'$set': {new: doc[old]}, '$unset': {old: 1}}
                    )



if __name__ == "__main__":
    main()
