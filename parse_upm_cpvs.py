#!/usr/bin/env python
# coding: utf-8
''' Read UPM produced JSON for predicted CPVs
    usage: parse_bsc_companies.py [-h] [--config CONFIG] [--debug] [-v] --dry_run json_files

Parse NextProcurement parquets

positional arguments:
  json_files       CPV data

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
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db

def main():
    parser = argparse.ArgumentParser(description='Parse BSC Companies')
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")
    parser.add_argument('--dry_run', action='store_true', help="Do not alter DB, just check")
    parser.add_argument('--drop', action='store_true', help="Drop existing predicted_cpv values")
    parser.add_argument('json_files', help="Input file", nargs='+')

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
    logging.info(f"CPVs:          {args.json_files}")

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
    place_cols = [db_lnk.db.get_collection(config["outsiders_col_prefix"]), db_lnk.db.get_collection(config["minors_col_prefix"])]
    logging.debug(f"Place collections: {place_cols}")
    
    if args.drop:
        if not args.dry_run:
            for col in place_cols:
                logging.info(f"Dropping predicted_cpv from {col.name}")
                col.update_many({}, {"$unset": {"nextp_enriched/predicted_cpv": ""}})
        else:
            logging.info("Would drop predicted_cpv (--dry_run)")

    for file in args.json_files:
        logging.info(f"Processing {file}")

        processed_docs = {}

        with open(file) as json_file:
            data = json.load(json_file)
            for doc in data:
                logging.debug(f"Processing {doc['procurement_id_x']}")
                if doc['procurement_id_x'] not in processed_docs:
                    col = place_cols[nu.get_group(doc['procurement_id_x'])]
                    ref_doc = ntp.NtpEntry()
                    ref_doc.load_from_db(col, doc['procurement_id_x'])
                    if ref_doc.is_obsolete():
                        active_doc_id = nu.get_active_version(ref_doc.data['id'], col)
                        logging.warning(f"Document {doc['procurement_id_x']} is obsolete, active version is {active_doc_id}")
                        ref_doc.load_from_db(col, active_doc_id)
                        if not ref_doc:
                            logging.error(f"Active version {active_doc_id} not found")
                            continue
                    else:
                        logging.info(f"Document {doc['procurement_id_x']} is active")
                    processed_docs[doc['procurement_id_x']] = ref_doc.ntp_id
                else:
                    logging.debug(f"Document {doc['procurement_id_x']} already found as {processed_docs[doc['procurement_id_x']]}")
                    ref_doc = ntp.NtpEntry()
                    ref_doc.load_from_db(col, processed_docs[doc['procurement_id_x']])
                doc['cpv_code'] = str(doc['cpv_code']).ljust(8, '0')
                ref_doc.data['nextp_enriched/predicted_cpv'] = doc
                logging.debug(f"Document {ref_doc.ntp_id} to update ")
                logging.debug(ref_doc.data['nextp_enriched/predicted_cpv'])
                if not args.dry_run:
                    ref_doc.commit_to_db(col)
                else:
                    logging.info(f"Document {ref_doc.ntp_id} not saved (--dry_run)")
        logging.info(f"Processed {len(processed_docs)} documents")
    logging.info("Done")


if __name__ == "__main__":
    main()
