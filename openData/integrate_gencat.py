#!/usr/bin/env python
# coding: utf-8
''' Integrates GENCAT OpenData in main Place collection
    usage: parse_gencat_json.py [-h] [--config CONFIG] [--debug] [-v] [--dry_run] json_files

Parse NextProcurement parquets

positional arguments:
  json_files       Gencat data

options:
  -h, --help       show this help message and exit
  --config CONFIG  Configuration file
  --debug          Add Debug information
  -v, --verbose    Add Extra information
  --dry_run        Do not alter DB, just list actions

'''

import sys
import argparse
import logging
import json
import re
import pandas as pd
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db
from pymongo.collation import Collation

OPENDATA_COL = "tmp_openData"
PLACE_COL = "place"

def main():
    parser = argparse.ArgumentParser(description='Parse GENCAT OpenData')
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")
    parser.add_argument('--dry_run', action='store_true', help="Do not alter DB, just check")
    parser.add_argument('--op', action='store', help="merge|add|all" )
    parser.add_argument('--processed', action='store', help="Processed ids (optional)" )

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

    gencatDA_col = db_lnk.db.get_collection(OPENDATA_COL)
    place_col = db_lnk.db.get_collection(PLACE_COL)

    logging.debug(f"OpenData collection: {gencatDA_col}")
    logging.debug(f"OpenData collection: {place_col}")

    id_num = nu.get_last_order('DA', place_col)

    logging.info(f"New DA documents starting at {id_num}")

    processed_ids = []
    if args.processed:
        with open(args.processed, 'r') as proc_file:
            for line in proc_file:
                processed_ids.append(line.strip())
            logging.info(f"Found {len(processed_ids)} already processed ids")
    all_ids = list(gencatDA_col.find({}, projection={"_id": 1}))
    logging.info(f"Found {len(all_ids)} documents in OpenData")

    regx = re.compile('^http')
    matched_ids = list(gencatDA_col.find({'id': regx}, projection={"_id": 1, "id": 1}))

    logging.info(f"Found {len(matched_ids)} matched documents in OpenData")

    if args.op == 'merge' or args.op == 'all':
        for doc in matched_ids:
            if doc['_id'] in processed_ids:
                logging.info(f"{doc['_id']} already processed, skipping")
                continue
            da_doc = ntp.NtpEntry()
            da_doc.load_from_db(gencatDA_col, doc['_id'])
            place_doc_id = nu.get_active_version(da_doc.data['id'], place_col)
            if place_doc_id:
                logging.info(f"Merging {doc['_id']} on {place_doc_id}")
                place_doc = ntp.NtpEntry()
                place_doc.load_from_db(place_col, place_doc_id)
                new = []
                mod = []
                for k in da_doc.data.keys():
                    if k in ['indice_unico', '_id']:
                        continue

                    if isinstance(da_doc.data[k], list) and len(da_doc.data[k]) == 1:
                        da_doc.data[k] = da_doc.data[k][0]

                    if not da_doc.data[k] or da_doc.data[k] in ['nan']:
                        continue

                    if isinstance(da_doc.data[k], list) and not any(da_doc.data[k]):
                        continue

                    if k not in place_doc.data or not place_doc.data[k]:
                        new.append(k)
                        logging.info(f"New field {k} {da_doc.data[k]}")
                        place_doc.data[k] = da_doc.data[k]

                    elif da_doc.data[k] != place_doc.data[k]:
                        if f"{k}_gc" in place_doc.data:
                            del(place_doc.data[f"{k}_gc"])
                            
                        #comparing strings with newlines removed and numbers as str
                        if nu.nonewlines(str(da_doc.data[k])) == nu.nonewlines(str(place_doc.data[k])):
                            continue
                        #dates
                        if 'Fecha' in k and isinstance(da_doc.data[k], str):   
                            if da_doc.data[k].startswith(place_doc.data[k]):
                                continue                    
                        
                        mod.append(k)
                        
                        logging.info(f"Changed field contents {k} {da_doc.data[k]} != {place_doc.data[k]}")
                        
                        if k == 'link' and 'contractaciopublica.gencat' in  place_doc.data['link']:
                            place_doc.data['link_old']   = place_doc.data['link']
                            place_doc.data['link']       = da_doc.data['link']
                            logging.info(f"Added field {k}_old {place_doc.data[k]}")
                        else:
                            place_doc.data[f"{k}_gc"] = da_doc.data[k]
                            logging.info(f"Added field {k}_gc {da_doc.data[k]}")
                
                if not args.dry_run:
                    place_doc.commit_to_db(place_col, update=False)
                    logging.info(f"Updated {place_doc.ntp_id} with DA doc {doc['_id']}")
                else:
                    logging.info(f"Would update {place_doc.ntp_id} with DA doc {doc['_id']}")
            else:
                logging.error(f"Missing place document for {da_doc.data['id']}")

    if args.op == 'add' or args.op == 'all':
        for doc in all_ids:
            if doc in matched_ids or doc in processed_ids:
                continue
            da_doc = ntp.NtpEntry()
            da_doc.load_from_db(gencatDA_col, doc['_id'])
            if not da_doc.data['id']:
                id_num += 1
                da_doc.data['id'] = f"/unmatched_gencat/{id_num}"
                da_doc.ntp_order = id_num
                da_doc.set_ntp_id()
            da_doc.data['tmp_DA_id'] = da_doc.ntp_id
            for k in da_doc.data:
                if isinstance(da_doc.data[k], list) and len(da_doc.data[k]) == 1:
                    da_doc.data[k] = da_doc.data[k][0]
                if da_doc.data[k] == 'nan':
                    da_doc.data[k] = None

            if not args.dry_run:
                da_doc.commit_to_db(place_col, update=False)
                logging.info(f"Added/Replaced document as {da_doc.ntp_id}")
            else:
                logging.info(f"Would add/replace document as {da_doc.ntp_id}")



if __name__ == "__main__":
    main()
