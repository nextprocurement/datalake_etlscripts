#!/usr/bin/env python
# coding: utf-8
''' Read parquet files into mongodb DB tracking duplicates
    usage: read_parquet.py [-h] [--drop] [--config CONFIG] [--debug] [-v]
                            --group GROUP [--update]
                            codes_file pkt_file

Parse NextProcurement parquets

positional arguments:
  codes_file       Column sanitized names
  pkt_file         Parquet file

options:
  -h, --help       show this help message and exit
  --drop           Clean MongoDB collection
  --config CONFIG  Configuration file
  --debug          Add Debug information
  -v, --verbose    Add Extra information
  --group GROUP    outsiders|minors|insiders

'''

import sys
import argparse
import logging
import pandas as pd
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_storage as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db

def main():
    parser = argparse.ArgumentParser(description='Parse NextProcurement parquets')
    parser.add_argument('--drop', action='store_true', help="Clean MongoDB collection")
    parser.add_argument('--config', action='store', help="Configuration file", default="secrets.yml")
    parser.add_argument('--debug', action='store_true', help="Add Debug information")
    parser.add_argument('-v','--verbose', action='store_true', help="Add Extra information")
    parser.add_argument('--group', action='store', help="outsiders|minors|insiders", required=True)

    parser.add_argument('codes_file', help="Columns sanitized names")
    parser.add_argument('pkt_file', help="Parquet file")
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
    logging.info(f"Parquet:       {args.pkt_file}")
    logging.info(f"Codes:         {args.codes_file}")
    logging.info(f"Group:         {args.group}")

    logging.info(f"Connecting MongoDB at {config['MONGODB_HOST']}")
    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        config['MONGODB_DB'],
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )

    incoming_col = db_lnk.db.get_collection(config[f'{args.group}_col_prefix'])

    data_table = pd.read_parquet(args.pkt_file, use_nullable_dtypes=True)
    new_cols = pd.read_csv(args.codes_file, sep='\t', index_col='ORIGINAL')


    if args.drop:
        logging.info("Dropping previously stored data")
        incoming_col.drop()
        id_num = cts.MIN_ORDER(args.group)
    else:
        id_num = nu.get_last_order(args.group, incoming_col)

    logging.info(f"Last reference found {id_num}")

    n_procs = 0
    for i in range(len(data_table.index)):
        new_data = nu.parse_parquet(data_table.iloc[i].to_dict().copy(), new_cols)
        logging.info(f"Processing {new_data['id']}")
        versions = nu.get_versions(new_data['id'], incoming_col)
        logging.debug(new_data['updated'])
        logging.debug(versions)
        found_version = nu.get_last_active_version(new_data, versions)
        new_doc = ntp.NtpEntry()
        if found_version:
            logging.info(f"found in {found_version['_id']}, updating")
            new_data['updated'] = nu.merge_updates(
                new_data['updated'],
                [vers['updated'] for vers in versions if vers['status'] == 'active']
            )
            logging.debug(new_data['updated'])
            #new_doc.load_data(nu.parse_ntp_id(found_version['_id']), new_data)
            selected_id = found_version['_id']
            previous_doc = ntp.NtpEntry()
            previous_doc.load_from_db(incoming_col, selected_id)
            previous_doc.merge_data(new_data)
            tmp_num = previous_doc.commit_to_db(incoming_col, update=False)
        else:
            logging.info("No active document found. Adding new document")
            new_doc.load_data(id_num + 1, new_data)
            tmp_num = new_doc.commit_to_db(incoming_col, update=False)
            id_num = max(tmp_num, id_num)
            selected_id = new_doc.ntp_id
        for vers in versions:
            if vers['_id'] == selected_id:
                continue
            vers_obs = ntp.NtpEntry(ntp_id=vers['_id'], place_id=vers['id'])
            vers_obs.make_obsolete(new_doc.ntp_id)
            logging.info(f"Updating obsolete {vers['_id']}")
            vers_obs.commit_to_db(incoming_col, update=False)

        if args.verbose:
            logging.info(f"Processed {selected_id}")
        n_procs += 1
    logging.info(f"Completed {n_procs} documents")

if __name__ == "__main__":
    main()
