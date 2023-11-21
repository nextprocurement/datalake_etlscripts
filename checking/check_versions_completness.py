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
from nextplib import ntp_entry as ntp
from mmb_data.mongo_db_connect import Mongo_db


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Check versioned documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--group', action='store', help='Group (outsiders|insiders|minors)')
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
    if args.group in ['insiders', 'outsiders']:
        incoming_col = db_lnk.db.get_collection('place')
        place_old_col = db_lnk.db.get_collection('place_old')
        cond = {'_id': {'$regex': 'ntp0'}}
        ini_doc = 1
    elif args.group in ['minors']:
        incoming_col = db_lnk.db.get_collection('place_menores')
        place_old_col = db_lnk.db.get_collection('place_menores_old')
        cond = {'_id': {'$regex': 'ntp1'}}
        ini_doc = 10000000

    max_id_c = incoming_col.aggregate(
        [
            {'$match': cond},
            {'$group':{'_id':'max_id', 'value':{'$max': '$_id'}}}
        ]
    )
    max_id = list(max_id_c)
    if max_id:
        id_num = ntp.parse_ntp_id(max_id[0]['value'])
    else:
        logging.info(f"No records found of the required type on {incoming_col.name}")

    for ntp_order in range(ini_doc, id_num):
        doc = ntp.NtpEntry()
        doc.ntp_order = ntp_order
        doc.set_ntp_id()
        logging.info(f"Processing {doc.ntp_id}")
        if doc.load_from_db(incoming_col, doc.ntp_id):
            if doc.is_obsolete():
                logging.info("Document found marked as obsolete version")
                final_doc = ntp.NtpEntry()
                if not final_doc.load_from_db(incoming_col, doc.data['updated_to']):
                    logging.error(f"Final document for ntp_id {doc.ntp_id} ({doc.data['updated_to']}) not found")
                    while final_doc.is_obsolete():
                        logging.warning(f"updated_to points to another obsolete_version")
                        final_doc.load_from_db(incoming_col, final_doc.data['updated_to'])
                    doc.data['updated_to'] = final_doc.ntp_id
                    logging.info(f"Updating pointer to {final_doc.ntp_id}")
                    doc.commit_to_db(incoming_col)
                else:
                    logging.info(f"Document found updated on {final_doc.ntp_id}")
            else:
                logging.info("Document found as active version")
        else:
            logging.warning(f"Document {doc.ntp_id} not found, recovering from old collection")
            recovered_doc = ntp.NtpEntry()
            if recovered_doc.load_from_db(place_old_col, doc.ntp_id):
                final_doc= incoming_col.find_one({
                    '$and': [
                        {'id': recovered_doc.data['id']},
                        {'obsolete_version': {'$exists':0}}
                    ]
                })
                if final_doc:
                    doc.data['id'] = recovered_doc.data['id']
                    doc.make_obsolete(final_doc['_id'])
                    doc.commit_to_db(incoming_col)
                    logging.info(f"Document found updated on {final_doc['_id']}")
                else:
                    logging.error(f"Place id {recovered_doc.data['id']} not found")
            else:
                logging.warning(f"{doc.ntp_id} not found on place legacy collection, skipping")




if __name__ == "__main__":
    main()
