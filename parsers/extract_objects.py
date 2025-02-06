#!/usr/bin/env python
# coding: utf-8
''' Script to extract contract objects
    usage: extract_objects.py [-h] [--replace] [--ini INI] [--fin FIN] [--id ID]
                            [--config CONFIG] [-v] [--debug]
                            [--group GROUP] [--drop]

Download Entities

options:
  -h, --help       show this help message and exit
  --ini INI        Initial document range
  --fin FIN        Final document range
  --id ID          Selected document id
  --config CONFIG  Configuration file (default: secrets.yml)
  -v, --verbose    Extra progress information
  --debug          Extra debug information
  --group GROUP    ousiders|minors|insiders
'''
import sys
import argparse
import logging
import os
import time
import re
import json
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db

FIELDS = [
    'id',
    'title',
    "Datos_Generales_del_Expediente/Objeto_del_Contrato",
    "Datos_Generales_del_Expediente_del_Lote/Objecto_del_Lote",
    "nextp_enriched/predicted_cpv"
]


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--group', action='store', help='tipo: ousiders|minors|insiders')

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

    logging.info(f"Connecting to MongoDB as {config['MONGODB_HOST']}")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.group in ['insiders', 'outsiders']:
        incoming_col = db_lnk.db.get_collection('place')
    elif args.group == 'minors':
        incoming_col = db_lnk.db.get_collection('place_menores')
    else:
        logging.error("--group missing or not recognized. acceptable minors|insiders|outsiders")
        sys.exit()

    if args.verbose:
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not nu.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{'obsolete_version': {'$exists':False}}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}
    num_ids = 0


    for doc in list(incoming_col.find(query, {'_id' : 1, 'obsolete_version': 1})):
        ntp_id = doc['_id']
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        num_ids += 1
        ntp_doc = ntp.NtpEntry()
        ntp_doc.load_from_db(incoming_col, ntp_id)
        if 'data_model' not in ntp_doc.data:
            logging.warning(f"{ntp_doc.data['_id']} is not in the appropriate data model, skipping")
            continue
        if 'obsolete_version' in ntp_doc.data and ntp_doc.data['obsolete_version']:
            logging.warning(f"{ntp_doc.data['_id']} is marked as obsolete, skipping")
            continue

        object = {}

        for field in FIELDS:
            if field in ntp_doc.data:
                object[field] = ntp_doc.data[field]
            else:
                object[field] = None
        if object['title'] == object['Datos_Generales_del_Expediente/Objeto_del_Contrato']:
                del(object['title'])
        if object['nextp_enriched/predicted_cpv'] and 'objective' in object['nextp_enriched/predicted_cpv']:
                object['nextp_enriched/predicted_cpv'] = object['nextp_enriched/predicted_cpv']['objective']
        print(json.dumps(object, ensure_ascii=False))

    logging.info(f"DONE. Processed {num_ids} entries")



    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
