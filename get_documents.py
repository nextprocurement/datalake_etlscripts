#!/usr/bin/env python
# coding: utf-8
''' Script to load PDF documents onto data_lake, takes input data from MongoDB
    usage: get_documents.py [-h] [--update] [--ini INI] [--fin FIN] [--id ID]
                             [--where {disc,gridfs,swift}]
    Download documents

    optional arguments:
       -h, --help            show this help message and exit
       -v, --verbose         Add extra progress information
       --replace             Replace existing files
       --ini INI             Initial document range
       --fin FIN             Final document range
       --id ID               Selected document id
       --where {disk,gridfs,swift}
                             Selected storage (disk|gridfs|swift). Default:disk
       --folder              Disk folder
       --config              Configuration file. Default:secrets.yml
       --debug
       -v --verbose
       --scan_only           Scan for presence and detect file type, do not download
       --delay               Add a time delay between calls to the same server
'''
import sys
import argparse
import logging
import os
import time
from yaml import load, CLoader
import swiftclient as sw
from nextplib import ntp_entry as ntp
from nextplib import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db

FIELDS_TO_SKIP = [
        'id',
        'LocatedContractingParty_WebsiteURI',
        'Entidad_Adjudicadora/URL_perfil_de_contratante',
        'Entidad_Adjudicadora/Sitio_Web',
        'Proceso_de_licitacion/Medio_de_Presentacion_de_Ofertas_Electronicas'
]

STORE_DOC_NAMES = {
        'Datos_Generales_del_Expediente/Pliego_de_Clausulas_Administrativas/URI': 'Pliego_clausulas_administrativas_URI',
        'Datos_Generales_del_Expediente/Pliego_de_Prescripciones_Tecnicas/URI': 'Pliego_Prescripciones_tecnicas_URI',
        'Datos_Generales_del_Expediente/Anexos_a_los_Pliegos/URI': 'Anexos_pliegos_URI',
        'Otros_documentos_publicados/Documento_Publicado/URI': 'Documento_Publicado_URI',
        'Datos_Generales_del_Expediente/Pliego_de_Prescripciones_Tecnicas/Archivo': 'Pliego_Prescripciones_tecnicas_Archivo'
        }

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--replace', action='store_true', help='Replace existing files')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--where', action='store', default='disk', choices=['disk', 'gridfs', 'swiºft'], help='Selected storage (disk|gridfs|swift)')
    parser.add_argument('--folder', action='store', help='Selected Disk/Swift folder')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--scan_only', action='store_true', help='Scan URL for doc type, do not download (implies --debug)')
    parser.add_argument('--delay', action='store', default=0, type=int, help="Time delay between requests to same server")
    parser.add_argument('--container', action='store_true', help="Swift container to use", default='PLACE')
    parser.add_argument('--allow_redirects', action='store_true', help='Allow for automatic redirects on HTTP 301 302')
    parser.add_argument('--skip_early', action='store_true', help='Skip immediately if any file for the corresponding field is already stored')
    # parser.add_argument('--no_verify', action='store_true', help='Do not verify certificates')
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

    logging.info(f"Connecting to MongoDB at {config['MONGODB_HOST']}")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.type=='mayores':
        incoming_col = db_lnk.db.get_collection('place')
    else:
        incoming_col = db_lnk.db.get_collection('place_menores')


    if not args.scan_only:
        if args.verbose:
            logging.info("Connecting to storage...")

        if args.where == 'disk':
            if args.folder is None:
                data_folder = config['TMPDIR']
            else:
                data_folder = args.folder

            if not os.path.isdir(data_folder):
                try:
                    os.mkdir((args.folder))
                except:
                    sys.exit("Error creating {data_folder}")
                logging.info(f"{data_folder} non existent, created")

            storage = ntpst.NtpStorageDisk(data_dir=data_folder)
            logging.info(f"Using disk storage at {data_folder}")

        elif args.where == 'gridfs':
            logging.info(f"Using GridFS storage at {config['MONGODB_HOST']}")
            storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs('downloadedDocuments'))

        elif args.where == 'swift':
            logging.info("Using Swift storage")
            swift_conn = sw.Connection(
                authurl=config['OS_AUTH_URL'],
                auth_version=3,
                os_options={
                    'auth_type': config['OS_AUTH_TYPE'],
                    'region_name': config['OS_REGION_NAME'],
                    'application_credential_id': config['OS_APPLICATION_CREDENTIAL_ID'],
                    'application_credential_secret': config['OS_APPLICATION_CREDENTIAL_SECRET'],
                    'service_project_name': 'bsc22NextProcurement'
                }
            )
            storage = ntpst.NtpStorageSwift(
                swift_connection=swift_conn,
                swift_container='PLACE',
                swift_prefix='documentos'
            )
    else:
        args.debug = True
        storage = None

    if args.verbose:
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not ntp.check_ntp_id(ntp_id):
            logging.error(f'{ntp_id} is not a valid ntp id')
            sys.exit()

    if args.id is not None:
        query = {'_id': args.id}
    else:
        query = [{'obsolete_version': {'$exists':0}}]
        if args.ini is not None:
            query.append({'_id':{'$gte': args.ini}})
        if args.fin is not None:
            query.append({'_id':{'$lte': args.fin}})
        query = {'$and': query}
    num_ids = 0
    last_server = ''
    for doc in list(incoming_col.find(query, {'_id':1})):
        ntp_id = doc['_id']
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        num_ids += 1
        ntp_doc = ntp.NtpEntry()
        ntp_doc.load_from_db(incoming_col, ntp_id)
        for url_field in ntp_doc.extract_urls():
            if ':' in url_field:
                url_base, url_index = url_field.split(':')
            else:
                url_base = url_field
                url_index = -1
            if url_base in FIELDS_TO_SKIP:
                logging.debug(f"Skipping {url_base}")
                continue
            if args.debug:
                logging.debug(f"{url_base}: {ntp_doc.data[url_base]}")
            if args.delay and ntp_doc.get_server(url_field) == last_server:
                time.sleep(args.delay)
            else:
                last_server = ntp_doc.get_server(url_field)
            # print(f"{last_server}")
            try:
                file_name = STORE_DOC_NAMES[url_field]
            except Exception as e:
                logging.error(e)
                continue
            results = ntp_doc.store_document(
                url_field,
                file_name,
                replace=args.replace,
                storage=storage,
                scan_only=args.scan_only,
                allow_redirects=args.allow_redirects,
                skip_early=args.skip_early
            )
            if args.verbose:
                if results[0] == ntp.SKIPPED:
                    logging.info(f"{file_name} skipped, File already exists and --replace not set or --scan_only")
                elif results[0] == ntp.UNWANTED_TYPE:
                    logging.info(f"{file_name} skipped, unwanted file type {results[1]}")
                elif results[0] == ntp.STORE_OK:
                    logging.info(f"File Stored as {ntp_doc.get_file_name(file_name, results[1])}")
                elif results[0] == ntp.SSL_ERROR:
                    logging.info(f"{url_field} unavailable, Reason: certificate error")
                else:
                    logging.warning(f"{url_field} unavailable. Reason: {results[1]}")
    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
