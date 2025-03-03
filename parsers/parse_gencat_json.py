#!/usr/bin/env python
# coding: utf-8
''' Read GENCAT produced JSON for OpenData
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

'''

import sys
import argparse
import logging
import json
import pandas as pd
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db

def main():
    parser = argparse.ArgumentParser(description='Parse GENCAT OpenData')
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")
    parser.add_argument('--dry_run', action='store_true', help="Do not alter DB, just check")
    parser.add_argument('--drop', action='store_true', help="Drop existing values")
    parser.add_argument('columns_file', help="Input file")
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
    logging.info(f"Columns:       {args.columns_file}")
    logging.info(f"Json Files:    {args.json_files}")

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
    #place_cols = [db_lnk.db.get_collection(config["outsiders_col_prefix"]), db_lnk.db.get_collection(config["minors_col_prefix"])]
    gencatDA_col = db_lnk.db.get_collection("tmp_openData")
    logging.debug(f"Place collection: {gencatDA_col}")
    
    if args.drop:
        if not args.dry_run:
            logging.info(f"Dropping data from {gencatDA_col.name}")
            gencatDA_col.delete_many({})
        else:
            logging.info("Would drop gencat OpenData (--dry_run)")

    new_cols = pd.read_csv(args.columns_file, sep='\t', index_col='ORIGINAL')

    tmp_id = 0
    id_num = 0

    for file in args.json_files:
        logging.info(f"Processing {file}")

        processed_docs = {}

        with open(file) as json_file:
            data = json.load(json_file)
            for doc in data:
                new_data = {}
                for col in doc.keys():
                    try:
                        if new_cols.loc[col]['DBFIELD'] in new_data:
                            if not isinstance(new_data[new_cols.loc[col]['DBFIELD']], list):
                                new_data[new_cols.loc[col]['DBFIELD']] = [new_data[new_cols.loc[col]['DBFIELD']]]
                                logging.debug(f"WARNING: multiple values found for {new_cols.loc[col]['DBFIELD']}, appending")
                            new_data[new_cols.loc[col]['DBFIELD']].append(doc[col])
                        else:
                            new_data[new_cols.loc[col]['DBFIELD']] = doc[col]
                    except KeyError:
                        mod_col = nu.get_new_dbfield(col)
                        logging.error(f'"{col}"\t"{mod_col}"\t"string"\n')
                if 'id' not in new_data or not new_data['id']:
                    logging.warning(f"Missing id field, using ntp9{str(tmp_id).zfill(7)}")
                    new_data['id'] = f"ntp9{str(tmp_id).zfill(7)}"
                    tmp_id += 1
                logging.info(f"Processing {new_data['id']}")
                new_doc = ntp.NtpEntry()
                new_doc.load_data(id_num + 1, new_data)
                if not args.dry_run:
                    tmp_num = new_doc.commit_to_db(gencatDA_col, update=False)
                    id_num = max(tmp_num, id_num)
                else:
                    print(new_doc)
                

                

        logging.info(f"Processed {len(processed_docs)} documents")
    logging.info("Done")


if __name__ == "__main__":
    main()
