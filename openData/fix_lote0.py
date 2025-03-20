#!/usr/bin/env python
# coding: utf-8
''' Fix GENCAT OpenData in main Place collection

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
    parser.add_argument('map', help='Columns map Lot to main')

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

    # Map
    columns_map = {}
    with open(args.map, 'r') as map_file:
       for line in map_file:
          lot_col, main_col = line.rstrip().split('\t')
          columns_map[lot_col] = main_col

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
    
    for ntp_id in sorted(all_ids, key=lambda x: x['_id']):
        modified = False
        logging.info(f"Processing {ntp_id['_id']}")
        doc = ntp.NtpEntry()
        doc.load_from_db(place_col, ntp_id['_id'])
        if not 'Datos_Generales_del_Expediente_del_Lote/ID_del_Lote' in doc.data or\
           not doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] or\
           doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] == 'nan' or\
           doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] == ['nan']:
            continue

        
        if isinstance(doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'], str):
            if ',' in doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']:

                if 'nan' in doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']: 
                    doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] =  doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'].replace('nan','0')
                if 'Lot' in doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] or 'LOT' in doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']: 
                    doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] =  doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'].replace('Lot ','').replace('LOT ','')

                doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] = doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'].strip().split(',')
                try:
                    doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] = list(map(int, doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']))
                except:
                    logging.warning(f"Could not convert |{doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']}|")
            else:
                try:
                    doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_lote'] = int(doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_lote'])       
                except:
                    logging.warning(f"Could not convert |{doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote']}|")
                #modified = True
        if doc.data['Datos_Generales_del_Expediente_del_Lote/ID_del_Lote'] in [0, '0']:
            for k in columns_map:
               if k not in doc.data or not doc.data[k]:
                   continue
               if columns_map[k] in doc.data and doc.data[columns_map[k]]:
                   logging.warning(f"{columns_map[k]} contains data already, skipping")
                   continue
               doc.data[columns_map[k]] = doc.data[k]
               logging.info(f"{columns_map[k]} modified from {k}")
               modified = True

        if modified:
            if not args.dry_run:
                doc.commit_to_db(place_col)
                logging.info(f"Fixed {ntp_id['_id']}")
            else:
                logging.info(f"Would fix {ntp_id['_id']} (--dry_run)")
        else:
            logging.info(f"Nothing to fix {ntp_id['_id']})")

if __name__ == "__main__":
    main()
