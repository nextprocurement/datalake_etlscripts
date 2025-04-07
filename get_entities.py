#!/usr/bin/env python
# coding: utf-8
''' Script to extract entities (Licitadores / Adjudicatarios)
    usage: get_entities.py [-h] [--replace] [--ini INI] [--fin FIN] [--id ID]
                            [--config CONFIG] [-v] [--debug]
                            [--group GROUP] [--drop]

Download Entities

options:
  -h, --help       show this help message and exit
  --replace        Replace existing files
  --ini INI        Initial document range
  --fin FIN        Final document range
  --id ID          Selected document id
  --config CONFIG  Configuration file (default: secrets.yml)
  -v, --verbose    Extra progress information
  --debug          Extra debug information
  --group GROUP    ousiders|minors|insiders
  --drop           Delete previous data
'''
import sys
import argparse
import logging
import os
import time
import re
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db
from spanish_dni.dni import DNI
from spanish_dni.validator.exceptions import NotValidDNIException
from spanish_dni.validator import validate_dni

DNI_REGEX = r'^(\d{8})([A-Z])$'
CIF_REGEX = r'^([ABCDEFGHJKLMNPQRSUVW])(\d{7})([0-9A-J])$'
NIE_REGEX = r'^[XYZ]\d{7,8}[A-Z]$'
FIELDS = ['Nombre', 'Ubicacion_organica', '']


def process_nif(nif):
    nif = str(nif).upper().replace('-','').replace(' ','').replace('.', '')
    logging.info(f"Checking NIF {nif}")
    if re.match(CIF_REGEX, nif):
        valid = True
        logging.debug(f"CIF {nif} is valid")
    else:
        valid = True
        try:
            dni_parsed: DNI = validate_dni(nif)
            logging.debug(f"DNI {nif} is type {dni_parsed.dni_type}")
        except (NotValidDNIException, ValueError) as error:
            valid = False
            logging.error(f"DNI/NIE {nif} is not valid")
    if valid:
        return nif
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

    entities_col = db_lnk.db.get_collection('entities')

    #if args.drop:
    #    entities_col.delete_many({})

    if args.verbose:
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not nu.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{'obsolete_version': {'$exists':False}}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}
    num_ids = 0

    # Fetch all ids to avoid cursor timeouts
    list_ids = list(incoming_col.find(query, {'_id' : 1, 'obsolete_version': 1}))
    logging.info(f"Found {len(list_ids)} to process")

    processed_nifs = set()
    if not args.replace:
        for nif in entities_col.find({}, projection={'_id':1}):
            processed_nifs.add(nif['_id'])

        logging.info(f"Found {len(processed_nifs)} entities")

    for doc in sorted(list_ids, key=lambda x: x['_id']):
        ntp_id = doc['_id']
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        num_ids += 1
        ntp_doc = ntp.NtpEntry()
        ntp_doc.load_from_db(incoming_col, ntp_id)
        #if 'data_model' not in ntp_doc.data:
        #    logging.warning(f"{ntp_doc.data['_id']} is not in the appropriate data model, skipping")
        #    continue
        if 'obsolete_version' in ntp_doc.data and ntp_doc.data['obsolete_version']:
            logging.warning(f"{ntp_doc.data['_id']} is marked as obsolete, skipping")
            continue

        contracting_party = {}
        contracting_party['other_ids'] = []
        if 'Entidad_Adjudicadora/ID' in ntp_doc.data and ntp_doc.data['Entidad_Adjudicadora/ID']:
            if 'Entidad_Adjudicadora/IDschemeName' not in ntp_doc.data:
                ntp_doc.data['Entidad_Adjudicadora/IDschemeName'] = 'NIF'
            if not isinstance(ntp_doc.data['Entidad_Adjudicadora/ID'], list):
                ntp_doc.data['Entidad_Adjudicadora/ID'] = [ntp_doc.data['Entidad_Adjudicadora/ID']]
                ntp_doc.data['Entidad_Adjudicadora/IDschemeName'] = [ntp_doc.data['Entidad_Adjudicadora/IDschemeName']]
            for ind, value in enumerate(ntp_doc.data['Entidad_Adjudicadora/ID']):
                logging.debug(f"{ind}, {value}")
                if len(ntp_doc.data['Entidad_Adjudicadora/IDschemeName']) > ind:
                    logging.debug(ntp_doc.data['Entidad_Adjudicadora/IDschemeName'][ind])
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
                lb = k.replace('Entidad_Adjudicadora_Jerarquia/', 'Jerarquia/')
                contracting_party[lb] = ntp_doc.data[k]


            if 'nif' in contracting_party:
                contracting_party['_id'] = contracting_party['nif'].replace('-', '')
                contracting_party['nif_valid'] = process_nif(contracting_party['nif'])
                contracting_party['type'] = 'Entidad_Adjudicadora'
                logging.debug(contracting_party)
                if contracting_party['_id'] in processed_nifs:
                    logging.info(f"Already processed, skipping")
                    continue
                if args.replace:
                    previous_entity = entities_col.find_one({'_id':contracting_party['_id']})

                    for id in previous_entity['Entidad_Adjudicadora/ID']:
                        if not id in contracting_party['Entidad_Adjudicadora/ID']:
                            contracting_party['Entidad_Adjudicadora/ID'].append(id)


                processed_nifs.add(contracting_party['_id'])

                try:
                    entities_col.update_one(
                        {'_id': contracting_party['_id']},
                        {'$set': contracting_party,'$addToSet': {'contratos': ntp_id}},
                        upsert=True
                    )
                    entities_col.update_one(
                        {'_id': contracting_party['_id']},
                        {'$addToSet': {'contratos': ntp_doc.data['id']}}
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
                logging.debug(f"{ind}, {nif}")
                nif_ok = process_nif(nif)
                if nif_ok:
                    nif = nif.replace('-', '')
                    adjudicatario['_id'] = nif_ok
                    adjudicatario['nif'] = nif_ok
                    for k in ntp_doc.data:
                        if not 'Adjudicatario' in k:
                            continue
                        lb = k.replace('Adjudicatario/', '')
                        if k == 'Adjudicatario/Nombre_del_Adjudicatario':
                            lb = 'Nombre'
                        if not isinstance(ntp_doc.data[k], list):
                            adjudicatario[lb] = ntp_doc.data[k]
                        elif len(ntp_doc.data[k]) > ind:
                            adjudicatario[lb] = ntp_doc.data[k][ind]
                        adjudicatario['type'] = 'Adjudicatario'
                    logging.debug(adjudicatario)
                    if nif_ok in processed_nifs:
                        logging.info(f"Already processed, skipping")
                        continue
                    processed_nifs.add(nif_ok)
                    try:
                        entities_col.update_one(
                            {'_id': nif_ok},
                            {'$set': adjudicatario},
                            upsert=True
                        )
                        entities_col.update_one(
                            {'_id': nif_ok},
                            {'$addToSet': {'contratos': ntp_doc.data['id']}}
                        )
                    except Exception as e:
                        logging.error(e)
                        logging.error(adjudicatario)
                else:
                    logging.error(f"nif adj. incorrecto {nif}")
    logging.info(f"DONE. Processed {num_ids} entries")




    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
