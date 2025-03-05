import sys
import argparse
import logging
import os
import time
import re
import json
import random
import uuid
import string
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db


def main():
    parser = argparse.ArgumentParser(description='Download tenders')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--collection', action='store', help='Collection to use: place|place_menores|tmp_OpenData')

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

    incoming_col = db_lnk.db.get_collection(args.collection)

    logging.info(f"Collection: {args.collection}")

    query = {'obsolete_version': {'$exists':False}, 'indice_unico': {'$exists':False}}

    tenders_list = list(incoming_col.find(query, {'_id' : 1}))
    logging.info(f"Found {len(tenders_list)} tenders")
    num_tenders = len(tenders_list)
    cur_tender = 0

    for tender in tenders_list:
        cur_tender += 1
        tender_id = tender['_id']
        doc = ntp.NtpEntry()
        doc.load_from_db(incoming_col, tender_id)
        if 'indice_unico' not in doc.data:
            unique_code = doc.add_unique_code()
            doc.commit_to_db(incoming_col)
            logging.info(f"Added unique code {unique_code} to {tender_id} ({cur_tender}/{num_tenders})")
        else:
            unique_code = doc.data['indice_unico']
            logging.info(f"Unique code {unique_code} already exists for {tender_id} ({cur_tender}/{num_tenders})")


if __name__ == '__main__':
    main()
