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
from nextplib import ntp_entry as ntp
from mmb_data.mongo_db_connect import Mongo_db

DNI_REGEX = r'^(\d{8})([A-Z])$'
CIF_REGEX = r'^([ABCDEFGHJKLMNPQRSUVW])(\d{7})([0-9A-J])$'
NIE_REGEX = r'^[XYZ]\d{7,8}[A-Z]$'
FIELDS = ['Nombre', 'Ubicacion_organica', '']


def valid_nif(a):
    a = str(a).upper().replace('-','').replace(' ','').replace('.', '')
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
    parser.add_argument('--group', action='store', help='tipo: ousiders|minors|insiders')
    parser.add_argument('--drop', action='store_true', help='Delete previous data')

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
    if args.group in ['insiders', 'outsiders']:
        incoming_col = db_lnk.db.get_collection('place')
    elif args.group == 'minors':
        incoming_col = db_lnk.db.get_collection('place_menores')
    else:
        logging.error("--group missing or not recognized. acceptable minors|insiders|outsiders")
        sys.exit()

    contract_col = db_lnk.db.get_collection('contractingParties')
    adjud_col = db_lnk.db.get_collection('adjudicatarios')

    if args.drop:
        contract_col.delete_many({})
        adjud_col.delete_many({})

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
        if 'data_model' not in ntp_doc.data:
            logging.warning(f"{ntp_doc.data['_id']} is not in v2023 data model, skipping")
            continue
        contracting_party = {}
        contracting_party['other_ids'] = []
        if 'Entidad_Adjudicadora/ID' in ntp_doc.data and ntp_doc.data['Entidad_Adjudicadora/ID']:
            if not isinstance(ntp_doc.data['Entidad_Adjudicadora/ID'], list):
                ntp_doc.data['Entidad_Adjudicadora/ID'] = [ntp_doc.data['Entidad_Adjudicadora/ID']]
                ntp_doc.data['Entidad_Adjudicadora/IDschemeName'] = [ntp_doc.data['Entidad_Adjudicadora/IDschemeName']]
            for ind, value in enumerate(ntp_doc.data['Entidad_Adjudicadora/ID']):
                logging.debug(f"{ind}, {value}")
                if ntp_doc.data['Entidad_Adjudicadora/IDschemeName'][ind] == 'NIF':
                    contracting_party['nif'] = value
                else:
                    contracting_party['other_ids'].append({
                            ntp_doc.data['Entidad_Adjudicadora/IDschemeName'][ind]: value
                    })
            for k in ntp_doc.data:
                if not 'Entidad_Adjudicadora' in k:
                    continue
                lb = k.replace('Entidad_Adjudicadora/', '')
                contracting_party[lb] = ntp_doc.data[k]

            if 'nif' in contracting_party:
                contracting_party['_id'] = contracting_party['nif'].replace('-', '')
                try:
                    contract_col.update_one(
                        {'_id': contracting_party['_id']},
                        {'$set': contracting_party},
                        upsert=True
                    )
                except Exception as e:
                    logging.error(e)
                    logging.error(contracting_party)
        else:
            if 'nif' in contracting_party:
                logging.error(f"nif contr. incorrecto {contracting_party['nif']}")
            else:
                logging.error(f"Nif no encontrado")
                logging.debug(ntp_doc.data)
            logging.debug(contracting_party)

        adjudicatario = {}
        #logging.debug(ntp_doc.data)
        if 'Adjudicatario/Identificador' in ntp_doc.data and ntp_doc.data['Adjudicatario/Identificador']:
            if not isinstance(ntp_doc.data['Adjudicatario/Identificador'], list):
                ntp_doc.data['Adjudicatario/Identificador'] = [ntp_doc.data['Adjudicatario/Identificador']]
            for ind, nif in enumerate(ntp_doc.data['Adjudicatario/Identificador']):
                logging.debug(nif)
                nif_ok = valid_nif(nif)
                if nif_ok:
                    nif = nif.replace('-', '')
                    adjudicatario['_id'] = nif_ok
                    adjudicatario['nif'] = nif_ok
                    for k in ntp_doc.data:
                        if not 'Adjudicatario' in k:
                            continue
                        lb = k.replace('Adjudicatario/', '')
                        adjudicatario[lb] = ntp_doc.data[k][ind]
                    try:
                        adjud_col.update_one(
                            {'_id': nif_ok},
                            {'$set': adjudicatario},
                            upsert=True
                        )
                    except Exception as e:
                        logging.error(e)
                        logging.error(adjudicatario)
                else:
                    logging.error(f"nif adj. incorrecto {nif}")





    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
