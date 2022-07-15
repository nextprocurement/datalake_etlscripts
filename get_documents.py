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
       --config              Configuration file. Default:secrets.yml
'''
import sys
import argparse
import logging
from yaml import load, CLoader
import swiftclient as sw
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db

def main():
    ''' Main '''


    
    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--replace', action='store_true', help='Replace existing files')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--where', action='store', default='disk', choices=['disk', 'gridfs', 'swift'], help='Selected storage (disk|gridfs|swift)')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
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
    
    logging.info("Connecting to MongoDB")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS']
    )

    incoming_col = db_lnk.get_collections(['incoming'])['incoming']

    if args.verbose:
        logging.info("Connecting to storage...")

    if args.where == 'disk':
        storage = ntpst.NtpStorageDisk(data_dir=config['TMPDIR'])
        logging.info(f"Using disk storage at {config['TMPDIR']}")
    elif args.where == 'gridfs':
        logging.info(f"Using GridFS storage")
        storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs('downloadedDocuments'))
    elif args.where == 'swift':
        logging.info(f"Using Swift storage")
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
            swift_container='nextp-data',
            swift_prefix='/data'
        )

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
    for doc in list(incoming_col.find(query, {'_id':1})):
        ntp_id = doc['_id']
        if args.verbose:
            logging.info(f'Processing {ntp_id}')
        num_ids += 1
        ntp_doc = ntp.NtpEntry()
        ntp_doc.load_from_db(incoming_col, ntp_id)
        for url_field in ntp_doc.extract_urls():
            if args.debug:
                logging.debug(f"{url_field}: {ntp_doc.data[url_field]}")

            results = ntp_doc.store_document(url_field, replace=args.replace, storage=storage)
            if args.verbose:
                if results == 0:
                    logging.info(f"File Stored as {ntp_doc.get_file_name(url_field)}")
                elif results == 1:
                    logging.info(f"{url_field} skipped, File already exists and --replace not set")
                elif results == 2:
                    logging.info(f"{url_field} skipped, html or empty file")
                else:
                    logging.warning("Unknown store condition")
    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
