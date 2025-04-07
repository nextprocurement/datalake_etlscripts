#!/usr/bin/env python
# coding: utf-8
''' Read UC3M topics (V2 dual model)
    usage: parse_uc3m_topics.py [-h] [--config CONFIG] [--debug] [-v] --dry_run json_files


positional arguments:
  json_files       Metadata data

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
    logging.info(f"Topics:          {args.json_files}")

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
   

    for file in args.json_files:
        logging.info(f"Processing {file}")

        processed_docs = {}

        with open(file) as json_file:
            data = json.load(json_file)
            topics = data['info_topics']
            logging.info(f"Found {len(topics)} topics")
            logging.debug(f"Topics: {topics}")
            for doc_place_id in data['info_docs']:
                logging.debug(f"Processing {doc_place_id}")
                if doc_place_id in processed_docs:
                    logging.warning(f"Document {doc_place_id} already processed")
                    continue
                #col_index = nu.get_group_from_id(config['uri_prefixes'], doc_place_id)
                # if col_index is None:
                #     logging.error(f"Collection not found for {doc_place_id}")
                #     continue
                # # All data at insiders collection                    
                # col = place_cols[col_index]
                col = place_cols[0]
                logging.debug(f"Collection: {col.name}")
                active_doc_id = nu.get_active_version(doc_place_id, col)        
                if not active_doc_id:
                    logging.error(f"Document {doc_place_id} not found")
                    continue
                ref_doc = ntp.NtpEntry()
                ref_doc.load_from_db(col, active_doc_id)
                ref_doc.data['nextp_enriched/topics/augmented_objective'] = data['info_docs'][doc_place_id]['augmented_objective']
                ref_doc.data['nextp_enriched/topics/augmented_cpv'] = data['info_docs'][doc_place_id]['augmented_cpv']
                for analyzed_cpv in data['info_docs'][doc_place_id]['info_topics'].keys():
                    for group in 'small', 'large':
                        label = f"nextp_enriched/topics/info_topics/{group}"
                        ref_doc.data[label] = {}
                        data_probs = eval(data['info_docs'][doc_place_id]['info_topics'][analyzed_cpv][group])
                           
                        for topic_ind, prob in data_probs.items():
                            try:
                                ref_doc.data[label][topics[analyzed_cpv][group][str(topic_ind)]] = float(prob)
                            except KeyError as e:
                                logging.error(f"Missing topic value, CPV {analyzed_cpv} {group} {topic_ind}")
                                continue

                    logging.debug(f"Document {ref_doc.ntp_id} to update ")
                    logging.debug(ref_doc.data[label])
                if not args.dry_run:
                    logging.info(f"Stored Document {ref_doc.ntp_id} ")
                    ref_doc.commit_to_db(col)
                else:
                    logging.info(f"Document {ref_doc.ntp_id} not saved (--dry_run)")
               
        logging.info(f"Processed {len(processed_docs)} documents")
    logging.info("Done")


if __name__ == "__main__":
    main()
