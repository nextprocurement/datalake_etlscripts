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
    parser.add_argument('--type', action='store', help='Type of procurement', default="mayores")
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
    if args.type == 'menores':
        logging.info("Type of procurement set to 'menores'")
        incoming_col = db_lnk.db.get_collection('place_menores')
        clean_col = db_lnk.db.get_collection('place_clean_menores')
    else:
        incoming_col = db_lnk.db.get_collection('place')
        clean_col = db_lnk.db.get_collection('place_clean')

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

    chunk_size = 100000
    current = 0
    finished = False
    logging.info("Grouping documents according to place id")
    while not finished:
        logging.info(f"Processing {current} to {current + chunk_size}")
        PLACE_IDS = [
            result['id']
            for result in incoming_col.find(
                query,
                projection={'id':1, '_id':0},
                skip=current,
                limit= chunk_size
                )
        ]
        finished = not PLACE_IDS
        if finished:
            continue
        current += chunk_size
        LICS = {}
        STATS = {}

        for doc in incoming_col.aggregate([
            {'$match' : {'id': {'$in': PLACE_IDS}}},
            {
            '$group':
                {
                    '_id':'$id',
                    'versions': {'$addToSet': {'_id':"$_id", 'updated': "$updated"}}
                }
            }], allowDiskUse=True):
            place_id = os.path.basename(doc['_id'])
            LICS[place_id] = doc['versions']
            if len(doc['versions']) in STATS:
                STATS[len(doc['versions'])] += 1
            else:
                STATS[len(doc['versions'])] = 1

    logging.info(f"Found versioned entries: {[str(k) + ':' + str(v) for k,v in sorted (STATS.items())]}")

    logging.info("Dropping patch collection")
    clean_col.delete_many({})

    logging.info("Start processing")
    num_ids = 0
    num_new = 0
    for place_id in LICS:
        if args.verbose:
            logging.info(f"Processing place_id {place_id} with {len(LICS[place_id]) - 1} updates ")
        list_ids = sorted(LICS[place_id], key=lambda x: x['updated'])
        final_id = list_ids[-1]['_id']
        final_doc = ntp.NtpEntry()
        final_doc.load_from_db(incoming_col, final_id)
        final_doc.commit_to_db(clean_col)
        num_new += 1
        num_ids += 1
    logging.info(f"Processed {num_ids} entries, added {num_new} unique documents")

if __name__ == "__main__":
    main()
