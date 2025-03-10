#!/usr/bin/env python
# coding: utf-8
''' Integrates GENCAT OpenData in main Place collection
    usage: parse_gencat_json.py [-h] [--config CONFIG] [--debug] [-v] [--dry_run] json_files

Parse NextProcurement parquets

positional arguments:
  json_files       CPV data

options:
  -h, --help       show this help message and exit
  --config CONFIG  Configuration file
  --debug          Add Debug information
  -v, --verbose    Add Extra information
  --dry_run        Do not alter DB, just list actions
  --collection     Collection to use    

'''

import sys
import argparse
import logging
import re
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db


def main():
    parser = argparse.ArgumentParser(description='Parse GENCAT OpenData')
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")
    parser.add_argument('--dry_run', action='store_true', help="Do not alter DB, just check")
    parser.add_argument('--collection', action='store', help="Collection to fix", default="place")
    parser.add_argument('--prefix', action='store', help="limit to this docs")

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')
    if args.debug:
        logging.getLogger().setLevel(10)
    else:
        logging.getLogger().setLevel(20)

    # Config file
    with open(args.config)  as config_file:
        config = load(config_file, Loader=CLoader)

    logging.info(f"Configuration: {args.config}")

    logging.info(f"Connecting MongoDB at {config['MONGODB_HOST']}")
    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        config['MONGODB_DB'],
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    logging.info(f"Connected to {config['MONGODB_DB']}")

    place_col = db_lnk.db.get_collection(args.collection)

    logging.debug(f"Collection to fix: {place_col}")

    if args.prefix:
        logging.info(f"Limiting to prefix {args.prefix}")
        query = {"_id": re.compile(f"^{args.prefix}"), 'obsolete_version': {'$exists': False}}
    else:
        query = {'obsolete_version': {'$exists': False}}   
    all_ids = list(place_col.find(query, projection={"_id": 1}))
    logging.info(f"Found {len(all_ids)} documents")
    fixed = 0
    for ntp_id in all_ids:
        logging.info(f"Processing {ntp_id['_id']}")
        doc = ntp.NtpEntry()
        doc.load_from_db(place_col, ntp_id['_id'])
        # list
        for k in doc.data.keys():
            if isinstance(doc.data[k], list) and len(doc.data[k]) == 1:
                doc.data[k] = doc.data[k][0]
                logging.info(f"Fixed {k} to {doc.data[k]}")
                fixed = 1
                
        if not doc.data['Datos_Generales_del_Expediente/Objeto_del_Contrato']:
            doc.data['Datos_Generales_del_Expediente/Objeto_del_Contrato'] = doc.data['title']
            logging.info(f"Setting Objeto del Contrato to title")
            fixed = 1

        if re.match(r'contrataciopublica.cat', doc.data['link']) and 'link_gc' in doc.data:
            doc.data['link_old']   = doc.data['link']
            doc.data['link']       = doc.data['link_gc']
            del(doc.data['link_gc'])
            logging.info(f"Setting link to {doc.data['link']}")
            fixed = 1
            
        if not args.dry_run and fixed:
            doc.commit_to_db(place_col)
            logging.info(f"Fixed {ntp_id['_id']}")
        else:
            logging.info(f"Nothing to fix {ntp_id['_id']} or --dry_run")

        #sys.exit()    
if __name__ == "__main__":
    main()
