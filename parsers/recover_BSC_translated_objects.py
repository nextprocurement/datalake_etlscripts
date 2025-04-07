#!/usr/bin/env python
# coding: utf-8
''' Script to add BSC translated contract objects
    usage: extract_objects.py [-h] [--config CONFIG] [-v] [--debug]
                            [--group GROUP]

Download Entities

options:
  -h, --help       show this help message and exit
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

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--group', action='store', help='tipo: ousiders|minors|insiders')
    parser.add_argument('--dry_run', action='store_true', help='')
    parser.add_argument('json_files', help="Input file", nargs='+')
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

    num_ids = 0
    
    for file in args.json_files:
        logging.info(f"Processing {file}")
        with open(file) as json_file:
            for line in json_file:
                doc = json.loads(line)
                if 'id' not in doc:
                    logging.error(f"Missing id in {file}")
                    continue
                if args.verbose:
                    logging.info(f"Processing {doc['id']}")
                ntp_doc = ntp.NtpEntry()
                ntp_id = nu.get_active_version(doc['id'], incoming_col)
                ntp_doc.load_from_db(incoming_col, ntp_id)
                ntp_doc.data['nextp_enriched/translated_object'] = doc['Datos_Generales_del_Expediente/Objeto_del_Contrato']
                if not args.dry_run:
                    ntp_doc.commit_to_db(incoming_col)
                    logging.info(f"Updated {doc['id']}")
                else:
                    logging.info(f"Would update {doc['id']} (dry-run)") 
                num_ids += 1 
                

    
    logging.info(f"DONE. Processed {num_ids} entries")



    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
