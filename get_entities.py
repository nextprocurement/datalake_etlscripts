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

DNI_REGEX = r'^(\d{8})([A-Z])$'
CIF_REGEX = r'^([ABCDEFGHJKLMNPQRSUVW])(\d{7})([0-9A-J])$'
NIE_REGEX = r'^[XYZ]\d{7,8}[A-Z]$'
FIELDS = ['Nombre', 'Ubicacion_organica', '']

def valid_nif(a):
    a = a.upper().replace('-','').replace(' ','').replace('.', '')
    if re.match(CIF_REGEX, a) or re.match(DNI_REGEX, a) or re.match(NIE_REGEX, a):
        return a
    return False


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

    logging.info(f"Connecting to MongoDB as {config['MONGODB_HOST']}")

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
            if not isinstance(ntp_doc.data['ID'], list):
                contracting_party['nif'] = ntp_doc.data['ID']
            else:
                contracting_party['other_ids'] = []
                for item in ntp_doc.data['ID']:
                    nif_ok = valid_nif(item)
                    if nif_ok:
                        contracting_party['nif'] = nif_ok
                    else:
                        contracting_party['other_ids'].append(item)

        for k in ntp_doc.data:
            if k in FIELDS or 'LocatedContractingParty' in k:
                contracting_party[k] = ntp_doc.data[k]

        if 'nif' in contracting_party:
            contracting_party['_id'] = contracting_party['nif'].replace('-', '')
            try:
                contract_col.update_one(
                    {'_id': contracting_party['_id']},
                    {'$set': contracting_party},
                    upsert=True
                )
            except Exception as e:
                print(e)
                print(contracting_party)
        else:
            if 'nif' in contracting_party:
                logging.error(f"nif contr. incorrecto {contracting_party['nif']}")
            else:
                logging.error(f"nif no encontrado")
            print(contracting_party)

        adjudicatario = {}

        if 'Identificador' in ntp_doc.data:
            for i, nif in enumerate(ntp_doc.data['Identificador']):
                nif_ok = valid_nif(nif)
                if nif_ok:
                    nif = nif.replace('-', '')
                    adjudicatario['_id'] = nif_ok
                    adjudicatario['nif'] = nif_ok
                    adjudicatario['Nombre'] = ntp_doc.data['Nombre_Adjudicatario'][i]
                    try:
                        adjud_col.update_one(
                            {'_id': nif_ok},
                            {'$set': adjudicatario},
                            upsert=True
                        )
                    except Exception as e:
                        print(e)
                        print(adjudicatario)
                else:
                    logging.error(f"nif adj. incorrecto {nif}")
                    print(ntp_doc.data['Nombre_Adjudicatario'][i])




    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
