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
       --from {disk,gridfs,swift}
                             Selected storage (disk|gridfs|swift). Default:disk
       --to {disk,gridfs,swift}
                             Selected storage (disk|gridfs|swift). Default:disk
       --from_folder              Disk folder
       --to_folder              Disk folder
       --config              Configuration file. Default:secrets.yml
       --debug
       -v --verbose
'''
import sys
import argparse
import logging
import os
import time
from types import NoneType
from yaml import load, CLoader
import swiftclient as sw
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db

def get_id_range(args):
    if args.id is not None:
        id_range = args.id
    elif args.ini is not None or args.fin is not None:
        id_range = args.ini, args.fin
    else:
        id_range = None
    return id_range

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--replace', action='store_true', help='Replace existing files')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('--where_from', action='store', default='disk', choices=['disk', 'gridfs', 'swift'], help='Selected Origin storage (disk|gridfs|swift)')
    parser.add_argument('--where_to', action='store', default='disk', choices=['disk', 'gridfs', 'swift'], help='Selected Destination storage (disk|gridfs|swift)')
    parser.add_argument('--from_folder', action='store', help='Selected Disk/Swift folder')
    parser.add_argument('--to_folder', action='store', help='Selected Disk/Swift folder')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--delete',action='store_true', help='Delete files at destination that are not present at Origin')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--container', action='store_true', help="Swift container to use", default='ESPROC')

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
    incoming_col = db_lnk.db.get_collection('incoming')

    if args.verbose:
        logging.info("Connecting to storage...")

    if args.where_from == 'disk' or args.where_to == 'disk':
        to_folder = from_folder = ''
        if args.where_from == 'disk':
            if args.from_folder is None:
                from_folder = config['TMPDIR']
            else:
                from_folder = args.from_folder
            if not os.path.isdir(from_folder):
                logging.error(f"{from_folder} does not exist, exiting")
                sys.exit(1)
            from_storage = ntpst.NtpStorageDisk(data_dir=from_folder)
            logging.info(f"Using Origin disk storage at {from_folder}")

        if args.where_to == 'disk':
            if args.to_folder is None:
                to_folder = config['TMPDIR']
            else:
                to_folder = args.to_folder

            if to_folder == from_folder:
                logging.error("Origin and Destination folders are the same, exiting")
                sys.exit()

            logging.info(f"Using Destination disk storage at {to_folder}")
            if not os.path.isdir(to_folder):
                try:
                    os.mkdir(to_folder)
                    logging.info(f"{to_folder} non existent, created")
                except:
                    sys.exit("Error creating {to_folder}")
            to_storage = ntpst.NtpStorageDisk(data_dir=to_folder)

    if args.where_from == 'gridfs' or args.where_to == 'gridfs':
        if args.where_from == args.where_to:
            logging.erro("Origin and destination points to gridFS, exiting")
            sys.error(1)
        if args.where_from == 'gridfs':
            logging.info("Using Origin GridFS storage")
            from_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs('downloadedDocuments'))
            from_folder = 'downloadedDocuments'
        if args.where_to == 'gridfs':
            logging.info("Using Destination GridFS storage")
            to_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs('downloadedDocuments'))
            to_folder = 'downloadedDocuments'

    if args.where_from == 'swift' or args.where_to == 'swift':
        if args.where_from == args.where_to and args.from_folder == args.to_folder:
            logging.error("Origin and destination to Swift coincident, exiting")
            sys.error(1)

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
        if args.where_from == 'swift':
            if args.from_folder is None:
                args.from_folder = 'documentos'
            from_storage = ntpst.NtpStorageSwift(
                swift_connection=swift_conn,
                swift_container=args.container,
                swift_prefix=args.from_folder
            )
            logging.info(f"Using Origin Swift storage at {args.container}:{args.from_folder}")

        if args.where_to == 'swift':
            if args.to_folder is None:
                args.to_folder = 'documentos'
            to_storage = ntpst.NtpStorageSwift(
                swift_connection=swift_conn,
                swift_container=args.container,
                swift_prefix=args.args.to_folder
            )
            logging.info(f"Using Destination Swift storage at {args.container}:{args.to_folder}")

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

    print(get_id_range(args))
    from_files = from_storage.file_list(id_range=get_id_range(args))
    print(from_files[0:10])

    to_files = to_storage.file_list(id_range=get_id_range(args))
    print(to_files[0:10])

    print(f"Origin: Files available at {args.from_where}:{args.from_folder} {len(from_files)}")
    print(f"Destination: Files available at {args.to_where}:{args.to_folder} {len(to_files)}")

    to_transfer = []
    for file in from_files:
        if file not in to_files:
            to_transfer.append(file)
    print(f"{len(to_transfer)} files to transfer")
    if args.delete:
        to_delete = []
        for file in to_files:
            if file not in from_files:
                to_delete.append(file)
        print(f"{len(to_delete)} files to delete")




    if args.verbose:
        logging.info(f"Processed {num_ids} entries")
if __name__ == "__main__":
    main()
