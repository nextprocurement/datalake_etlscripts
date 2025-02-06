#!/usr/bin/env python
# coding: utf-8
''' Read BSC producer JSON for extracted companies
    usage: parse_bsc_companies.py [-h] [--config CONFIG] [--debug] [-v] --dry_run json_files

Parse NextProcurement parquets

positional arguments:
  json_files       Column sanitized names

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
import re
import requests
from yaml import load, CLoader
from nextplib import ntp_entry as ntp, ntp_constants as cts, ntp_utils as nu
from mmb_data.mongo_db_connect import Mongo_db
from spanish_dni.dni import DNI
from spanish_dni.validator.exceptions import NotValidDNIException
from spanish_dni.validator import validate_dni

API_PREFIX = "https://nextprocurement.bsc.es/api"

DNI_REGEX = r'^(\d{8})([A-Z])$'
CIF_REGEX = r'^([ABCDEFGHJKLMNPQRSUVW])(\d{7})([0-9A-J])$'
NIE_REGEX = r'^[XYZ]\d{7,8}[A-Z]$'

FIELDS_LIST = [
    'FullName',
    'Name',
    'Province', 
    'CompanyType', 
    'CompanyDescription',
    'Ciudad',
    'Codigo_Postal',
    'Identificador_de_Pais',
    'Pais',
    'es_PYME',
    'es_UTE'                            
]


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
        except NotValidDNIException:
            valid = False
            logging.error(f"DNI/NIE {nif} is not valid")
    if valid:
        req = requests.get(f"{API_PREFIX}/companies/{nif}")
        if req.status_code == 200:
            company_data = req.json()
            logging.info(f"NIF/CIF {nif} found as {company_data['Name']}")
            return company_data
        req = requests.get(f"{API_PREFIX}/entities/{nif.upper()}")
        if req.status_code == 200:
            try:
                company_data = req.json()
            except json.decoder.JSONDecodeError:            
                logging.error(f"Error decoding JSON for NIF {nif}")
                return False 
            print(company_data)
            if 'Nombre_del_Adjudicatario' in company_data:
                company_data['Name'] = company_data['Nombre_del_Adjudicatario']
                del(company_data['Nombre_del_Adjudicatario'])
            if 'Nombre' in company_data:
                company_data['Name'] = company_data['Nombre']
                del(company_data['Nombre'])
            company_data['FullName'] = company_data['Name']
            logging.info(f"NIF/CIF {nif} found as {company_data['Name']}")
            return company_data
        logging.error(f"NIF/CIF {nif} not found as company")
    return False
            
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
    logging.info(f"Files:     {args.json_files}")

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
            for line in json_file:
                data = json.loads(line)
                print(data)

                 
                if all(x not in data for x in ['SINGLE_COMPANY', 'UTE', 'NIFs']):
                    logging.warning(f"Document {data['doc_name']} does not have companies")
                    continue
                if all(not data[x] for x in ['SINGLE_COMPANY', 'UTE', 'NIFs']):
                    logging.warning(f"Document {data['doc_name']} has empty companies data")
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

                if 'nextp_enriched/companies' not in ref_doc.data:
                    ref_doc.data['nextp_enriched/companies'] = {}

                ref_doc.data['nextp_enriched/companies'][data['doc_name']] = {}

                for group in ('SINGLE_COMPANY', 'UTE', 'NIFs'):
                    if group in data and data[group]:
                        ref_doc.data['nextp_enriched/companies'][data['doc_name']][group] = data[group]

                for nif in data['NIFs']:
                    company_data = process_nif(nif)
                    if company_data:                        
                        for key in FIELDS_LIST:
                            if key in company_data:
                                ref_doc.data['nextp_enriched/companies'][data['doc_name']]['NIFs'][nif][key] = company_data[key]

                for company in data['SINGLE_COMPANY']:
                    if 'NIF' in company:
                        nif = company['NIF']
                        company_data = process_nif(nif)
                        if company_data:
                            for key in FIELDS_LIST:
                                if key in company_data:
                                    ref_doc.data['nextp_enriched/companies'][data['doc_name']]['SINGLE_COMPANY'][key] = company_data[key]

                logging.debug(f"Document {ref_doc.ntp_id} to update ")
                logging.debug(ref_doc.data['nextp_enriched/companies'])
                if not args.dry_run:
                    ref_doc.commit_to_db(col)
                else:
                    logging.warning(f"Document {ref_doc.ntp_id} not saved (--dry_run)")
        logging.info(f"Processed {len(processed_docs)} documents")
    logging.info("Done")


if __name__ == "__main__":
    main()
