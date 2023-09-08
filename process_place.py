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
    parser.add_argument('--drop',action='store_true', help='Drop patch collection on start')

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
    patch_col = db_lnk.db.get_collection('place_patch')

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
    PLACE_IDS = [
        result['id']
        for result in incoming_col.find(query, projection={'id':1, '_id':0})
    ]
    LICS = {}
    STATS = {}
    for doc in list(incoming_col.aggregate([
        {'$match' : {'id': {'$in': PLACE_IDS}}},
        {
        '$group':
            {
                '_id':'$id',
                'versions': {'$addToSet': {'_id':"$_id", 'updated': "$updated"}}
            }
        }], allowDiskUse=True)):
        if len(doc['versions']) == 1:
            continue
        place_id = os.path.basename(doc['_id'])
        LICS[place_id] = doc['versions']
        if len(doc['versions']) in STATS:
            STATS[len(doc['versions'])] += 1
        else:
            STATS[len(doc['versions'])] = 1
    logging.info(f"Found versioned entries: {[str(k) + ':' + str(v) for k,v in sorted (STATS.items())]}")

    if args.drop:
        logging.info("Dropping patch collection")
        patch_col.delete_many({})

    logging.info("Start processing")
    num_ids = 0
    num_new = 0
    num_mod = 0
    num_del = 0
    for place_id in LICS:
        if args.verbose:
            logging.info(f"Processing place_id {place_id} with {len(LICS[place_id]) - 1} updates ")
        list_ids = sorted(LICS[place_id], key=lambda x: x['updated'])
        base_id = list_ids[0]['_id']
        base_doc = ntp.NtpEntry()
        base_doc.load_from_db(incoming_col, base_id)
        for doc in LICS[place_id]:
            id = doc['_id']
            if id == base_id:
                continue
            new_doc = ntp.NtpEntry()
            new_doc.load_from_db(incoming_col, id)
            new, modif, miss = base_doc.diff_document(new_doc)
            patch = {}
            if new:
                patch['add'] = new
                num_new += 1
            if modif:
                patch['mod'] = modif
                num_mod += 1
            if miss:
                patch['del'] = miss
                num_del += 1
            if not new and not modif and not miss:
                logging.warning(f"Potential duplicate {base_id}, {id}")
            num_ids += 1
            patch_col.update_one(
                {'_id': place_id},
                {
                    '$set': {
                        '_id': place_id,
                        'base_id': base_id
                    },
                    '$addToSet': {
                        'update': {
                            'id': id,
                            'patched_values': patch
                        }
                    }
                },
                upsert=True
            )
    logging.info(f"Processed {num_ids} entries, added {num_new}, modified {num_mod}, deleted {num_del}")

if __name__ == "__main__":
    main()
