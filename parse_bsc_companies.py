#!/usr/bin/env python
# coding: utf-8
''' Read BSC producer JSON for extracted companies
    usage: parse_bsc_companies.py [-h] [--config CONFIG] [--debug] [-v] --dry_run json_file

Parse NextProcurement parquets

positional arguments:
  json_file       Column sanitized names

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

    parser.add_argument('json_file', help="INput file")
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
    logging.info(f"Companies:     {args.json_file}")

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

    processed_docs = {}

    with open(args.json_file) as json_file:
        for line in json_file:
            data = json.loads(line)
            if all(x not in data for x in ['SINGLE_COMPANY', 'UTE']):
                logging.warning(f"Document {data['doc_name']} does not have companies")
                continue

            if data['procurement_id'] not in processed_docs:
                logging.debug(f"Processing {data['procurement_id']}")
                col = place_cols[nu.get_group(data['procurement_id'])]
                ref_doc = ntp.NtpEntry()
                ref_doc.load_from_db(col, data['procurement_id'])
                if ref_doc.is_obsolete():
                    active_doc_id = nu.get_active_version(ref_doc.data['id'], col)
                    logging.warning(f"Document {data['procurement_id']} is obsolete, active version is {active_doc_id}")
                    ref_doc.load_from_db(col, active_doc_id)
                    if not ref_doc:
                        logging.error(f"Active version {active_doc_id} not found")
                        continue
                else:
                    logging.info(f"Document {data['procurement_id']} is active")
                processed_docs[data['procurement_id']] = ref_doc.ntp_id
            else:
                logging.debug(f"Document {data['procurement_id']} already found as {processed_docs[data['procurement_id']]}")
                ref_doc = ntp.NtpEntry()
                ref_doc.load_from_db(col, processed_docs[data['procurement_id']])

            if 'empresas_en_docs' not in ref_doc.data:
                ref_doc.data['empresas_en_docs'] = {}

            ref_doc.data['empresas_en_docs'][data['doc_name']] = {}
            for group in ('SINGLE_COMPANY', 'UTE'):
                if group in data and data[group]:
                    ref_doc.data['empresas_en_docs'][data['doc_name']][group] = data[group]

            logging.debug(f"Document {ref_doc.ntp_id} to update ")
            logging.debug(ref_doc.data['empresas_en_docs'])
            if not args.dry_run:
                ref_doc.commit_to_db(col)
            else:
                logging.info(f"Document {ref_doc.ntp_id} not saved (--dry_run)")



if __name__ == "__main__":
    main()
