#!/usr/bin/env python
# coding: utf-8
''' Script to extract entities (Licitadores / Adjudicatarios)
    usage: get_entities.py [-h] [--update] [--ini INI] [--fin FIN] [--id ID]

    optional arguments:
       -h, --help            show this help message and exit
       -v, --verbose         Add extra progress information
       --ini INI             Initial document range
       --fin FIN             Final document range
       --id ID               Selected document id
       --config              Configuration file. Default:secrets.yml
       --debug
       -v --verbose
'''
import sys
import argparse
import logging
import os
import time
import re
from yaml import load, CLoader
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db


def valid_nif(a):
    return len(a) == 9


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--replace', action='store_true', help='Replace existing files')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--type', action='store', help='tipo: mayores|menores', default='mayores')

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

    logging.info("Connecting to MongoDB")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.type == 'mayores':
        incoming_col = db_lnk.db.get_collection('place')
    else:
        incoming_col = db_lnk.db.get_collection('place_menores')

    contract_col = db_lnk.db.get_collection('contractingParties')
    adjud_col = db_lnk.db.get_collection('adjudicatarios')

    if args.verbose:
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not ntp.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}
    num_ids = 0

    for doc in list(incoming_col.find(query, {'_id' : 1})):
        ntp_id = doc['_id']
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        num_ids += 1
        ntp_doc = ntp.NtpEntry()
        ntp_doc.load_from_db(incoming_col, ntp_id)

        contracting_party = {}
        if 'ID' in ntp_doc.data :
            if isinstance(ntp_doc.data['ID'], list):
                contracting_party['rec'], contracting_party['nif'] = ntp_doc.data['ID']
            else:
                contracting_party['nif'] = ntp_doc.data['ID']
        if 'Nombre' in ntp_doc.data:
            contracting_party['Nombre'] = ntp_doc.data['Nombre']
        for k in ntp_doc.data:
            if 'LocatedContractingParty' in k:
                contracting_party[k] = ntp_doc.data[k]


        if valid_nif(contracting_party['nif']):
            contracting_party['_id'] = contracting_party['nif']
            contract_col.update_one(
                {'_id':contracting_party['_id']},
                {'$set': contracting_party},
                upsert=True
            )
        else:
            logging.error(f"No valid nif {contracting_party['nif']}")

        print(contracting_party)


        adjudicatario = {}

        if 'Identificador' in ntp_doc.data:
            for ind, nif in enumerate(ntp_doc.data['Identificador']):
                if valid_nif(nif):
                    adjudicatario['_id'] = nif
                    adjudicatario['nif'] = nif
                    adjudicatario['nombre'] = ntp_doc.data['Nombre_Adjudicatario'][ind]
                    adjud_col.update_one(
                        {'_id': id},
                        {'$set': adjudicatario},
                        upsert=True
                    )
                else:
                    logging.error(f"No valid nif {contracting_party['nif']}")
                print(adjudicatario)




    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
