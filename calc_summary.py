#!/usr/bin/env python
# coding: utf-8
''' Script to build historical on place
    usage: process_place.py [-h] [--ini INI] [--fin FIN] [--id ID]
'''
import sys
import argparse
import logging
import os
import time
from yaml import load, CLoader
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Summary')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--type', action='store', default='mayores', help='Mayores (no menores) | menores')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')

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

    logging.info(f"Connecting to MongoDB at {config['MONGODB_HOST']}")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.type == 'menores':
        data_store = 'place_menores'
    else:
        data_store = 'place'

    incoming_col = db_lnk.db.get_collection(data_store)

    data = {}
    data['total_documents'] = incoming_col.estimated_document_count()
    aggregated_counts = [
        'Tipo_Contrato',
        'TenderingProcess_ContractingSystemCode',
        'TenderingTerms_Language_ID',
        'LocatedContractingParty_ContractingPartyTypeCode',
        'LocatedContractingParty_Country_Name',
        'ProcurementProject_RealizedLocation_Country_Name'
    ]

    for field in aggregated_counts:
        data[field] = list(incoming_col.aggregate(
            [
                {
                    '$match':{
                        '_id':{'$ne':'summary_data'}
                    }
                },
                {
                    '$group': {
                        '_id':'$' + field, 'count':{'$sum':1}
                    }
                }
            ]
        ))
    print(data)
    incoming_col.replace_one({'_id': 'summary_data'}, data, upsert=True)
    logging.info(f"Updated {data_store} summary")

if __name__ == "__main__":
    main()