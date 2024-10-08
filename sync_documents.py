#!/usr/bin/env python
# coding: utf-8
''' Script to load PDF documents onto data_lake, takes input data from MongoDB
    usage: sync_documents.py [-h] [--ini INI] [--fin FIN] [--id ID]
                        [-i FOLDER_IN] [-o FOLDER_OUT] [--config CONFIG]
                         [--delete] [--replace] [-v] [--debug] [--check_only]

Sync documents between storages

options:
  -h, --help            show this help message and exit
  --ini INI             Initial document range
  --fin FIN             Final document range
  --id ID               Selected document id
  -i FOLDER_IN, --folder_in FOLDER_IN
                        Selected Origin (local folder|gridfs:|container@swift:folder)
  -o FOLDER_OUT, --folder_out FOLDER_OUT
                        Selected Destination (local folder|gridfs:|container@swift:folder)
  --config CONFIG       Configuration file (default;secrets.yml)
  --delete              Delete files at destination that are not present at Origin
  --replace             Replace existing files
  -v, --verbose         Extra progress information
  --debug               Extra debug information
  --check_only          Check only, no transfer
'''

import sys
import argparse
import logging
import os
from yaml import load, CLoader
import swiftclient as sw
from nextplib import ntp_entry as ntp, ntp_storage as ntpst, ntp_utils as nu, ntp_constants as cts
from mmb_data.mongo_db_connect import Mongo_db

def parse_folder_str(folder):
    container = None
    where_folder = None
    if ':' not in folder:
        where_from = 'disk'
        where_folder = folder
    elif folder == 'gridfs:':
        where_from = 'gridfs'
    elif 'swift:' in folder:
        container, path = folder.split('@', 1)
        where_from, where_folder = path.split(':', 1)
    else:
        logging.error(f"Not recognized folder {folder}")
        sys.exit(1)
    return where_from, where_folder, container

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Sync documents between storage')
    parser.add_argument('--ini', action='store', help='Initial document range')
    parser.add_argument('--fin', action='store', help='Final document range')
    parser.add_argument('--id', action='store', help='Selected document id')
    parser.add_argument('-i', '--folder_in', action='store', help='Selected Origin (local folder|gridfs:|container@swift:folder)')
    parser.add_argument('-o', '--folder_out', action='store', help='Selected Destination (local folder|gridfs:|container@swift:folder)')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--delete',action='store_true', help='Delete files at destination that are not present at Origin')
    parser.add_argument('--replace', action='store_true', help='Replace existing files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')
    parser.add_argument('--check_only',action='store_true', help='Check only, no transfer')
    parser.add_argument('--patch_list', action='store', help='Prepare a listing of modifications')

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

    if args.verbose:
        logging.info("Connecting to MongoDB")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        config['MONGODB_DB'],
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )

    if args.verbose:
        logging.info("Connecting to storage...")

    where_from, from_folder, container_from = parse_folder_str(args.folder_in)
    where_to, to_folder, container_to = parse_folder_str(args.folder_out)

    log_message_i = log_message_o = ''

    if where_from == 'disk' or where_to == 'disk':
        if where_from == 'disk':
            if from_folder is None:
                from_folder = config['TMPDIR']
            else:
                from_folder = args.from_folder
            if not os.path.isdir(from_folder):
                logging.error(f"{from_folder} does not exist, exiting")
                sys.exit(1)
            from_storage = ntpst.NtpStorageDisk(data_dir=from_folder)
            log_message_i = f"Using Origin disk storage at {from_folder}"

        if where_to == 'disk':
            if to_folder is None:
                to_folder = config['TMPDIR']

            if to_folder == from_folder:
                logging.error("Origin and Destination folders are the same, exiting")
                sys.exit()

            log_message_o = f"Using Destination disk storage at {to_folder}"
            if not os.path.isdir(to_folder):
                try:
                    os.mkdir(to_folder)
                    logging.info(f"{to_folder} non existent, created")
                except Exception as err:
                    sys.exit(f"Error creating {to_folder} {err}")
            to_storage = ntpst.NtpStorageDisk(data_dir=to_folder)

    if where_from == 'gridfs' or where_to == 'gridfs':
        if where_from == where_to:
            logging.error("Origin and destination points to gridFS, exiting")
            sys.error(1)
        if where_from == 'gridfs':
            log_message_i = f"Using Origin GridFS storage at {config['MONGODB_HOST']}"
            from_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_col']))
            from_folder = config['documents_col']
        if where_to == 'gridfs':
            log_message_o = f"Using Destination GridFS storage at {config['MONGODB_HOST']}"
            to_storage = ntpst.NtpStorageGridFs(gridfs_obj=db_lnk.get_gfs(config['documents_col']))
            to_folder = config['documents_col']

    if where_from == 'swift' or where_to == 'swift':
        if where_from == where_to and from_folder == to_folder:
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
                'service_project_name': config['OS_PROJECT_NAME']
            }
        )
        if where_from == 'swift':
            if from_folder is None:
                from_folder = config['OS_SWIFT_DOCUMENTS_FOLDER']
            from_storage = ntpst.NtpStorageSwift(
                swift_connection=swift_conn,
                swift_container=container_from,
                swift_prefix=from_folder
            )
            log_message_i = f"Using Origin Swift storage at {container_from}:{from_folder}"

        if where_to == 'swift':
            if to_folder is None:
                to_folder = config['OS_SWIFT_DOCUMENTS_FOLDER']
            to_storage = ntpst.NtpStorageSwift(
                swift_connection=swift_conn,
                swift_container=container_to,
                swift_prefix=to_folder
            )
            log_message_o = f"Using Destination Swift storage at {container_to}:{to_folder}"

    if args.verbose:
        logging.info(log_message_i)
        logging.info(log_message_o)
        logging.info("Getting ids...")

    for ntp_id in (args.id, args.ini, args.fin):
        if ntp_id is not None and not nu.check_ntp_id(ntp_id):
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

    if args.verbose:
        logging.info(f"id_range: {nu.get_id_range(args)}")

    from_files = set(from_storage.file_list(
        id_range=nu.get_id_range(args),
        set_debug=args.debug
    ))
    logging.info(f"Origin: {len(from_files)} Files available at {args.folder_in} ")

    to_files = set(to_storage.file_list(
        id_range=nu.get_id_range(args),
        set_debug=args.debug
    ))
    logging.info(f"Destination: {len(to_files)} Files available at {args.folder_out} ")

    new_files = []
    exist_files = []
    for file in from_files:
        if file not in to_files:
            new_files.append(file)
        else:
            exist_files.append(file)

    logging.info(f"{len(new_files)} new files at Origin")
    if args.replace:
        logging.info(f"{len(exist_files)} existing files at Destination")

    if args.delete:
        to_delete = []
        for file in to_files:
            if file not in from_files:
                to_delete.append(file)
        logging.info(f"{len(to_delete)} files to delete at Destination")

    if args.patch_list:
        with open(args.patch_list, "w") as patch_file:
            if args.delete:
                for file in to_delete:
                    print(f"DEL {file}", file=patch_file)
            if args.replace:
                for file in exist_files:
                    print(f"UPD {file}", file=patch_file)
            for file in new_files:
               print(f"ADD {file}", file=patch_file)


    if not args.check_only:
        if args.verbose:
            logging.info(f"Starting transfer")

        n_delete = 0
        n_transfer = 0
        n_error = 0

        if args.delete:
            for file in to_delete:
                try:
                    if args.verbose:
                        logging.info(f"Deleting {file}")
                    to_storage.delete_file(file)
                    n_delete += 1
                except Exception as e:
                    logging.debug(e)
                    logging.error(f"Error deleting {file}")

        if args.replace:
            to_transfer = from_files
        else:
            to_transfer = new_files
        if not args.check_only:
            for file in to_transfer:
                try:
                    if args.verbose:
                        logging.info(f"Transferring {file} ({n_transfer}/{len(to_transfer)})")
                    to_storage.file_store(file, from_storage.file_read(file))
                    n_transfer += 1
                except Exception as e:
                    logging.debug(e)
                    logging.error(f"Error storing {file}")
                    n_error += 1
            logging.info(f"Transfer completed. {n_transfer} files transferred, {n_delete} files deleted, {n_error} errors found")
    else:
        logging.info(f"no action done (--check_only)")

if __name__ == "__main__":
    main()
