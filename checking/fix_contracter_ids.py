#!/usr/bin/env python
# coding: utf-8
''' Script to fix Entidad_Adjudicadora/ID to all string
    usage: fix_contracter_ids.py [-h] [--ini INI] [--fin FIN] [--id ID]
'''
import sys
import argparse
import logging
import os
import time
from yaml import load, CLoader
from mmb_data.mongo_db_connect import Mongo_db

def main():
    ''' Main '''
    parser = argparse.ArgumentParser(description='Fix contracter ids')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
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
        config['MONGODB_DB'],
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )

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

    for col in ['insiders','minors']:
        incoming_col = db_lnk.db.get_collection(config[f"{col}_col_prefix"])
        logging.info(f"Processing {col} collection")
        logging.debug(incoming_col)
        for doc in incoming_col.find({'Entidad_Adjudicadora/ID': {'$exists': 1}}, projection={'Entidad_Adjudicadora/ID':1}):
            logging.debug(doc['Entidad_Adjudicadora/ID'])
            if not doc['Entidad_Adjudicadora/ID']:
                logging.warning(f"Empty Entidad_Adjudicadora/ID in {doc['_id']}")
                continue
            if not isinstance(doc['Entidad_Adjudicadora/ID'], list):
                new_ids = [str(doc['Entidad_Adjudicadora/ID'])]

            elif len(str(doc['Entidad_Adjudicadora/ID'][0])) == 1:
                # Fix from single id
                new_ids = [''.join(doc['Entidad_Adjudicadora/ID'])]
            elif len(doc['Entidad_Adjudicadora/ID']) > 1:
                new_ids = [str(x) for x in doc['Entidad_Adjudicadora/ID']]
            else:
                new_ids = [str(doc['Entidad_Adjudicadora/ID'][0])]
            logging.debug(new_ids)
            incoming_col.update_one(
                {'_id': doc['_id']},
                {'$set': {'Entidad_Adjudicadora/ID': new_ids}}
            )


if __name__ == "__main__":
    main()
