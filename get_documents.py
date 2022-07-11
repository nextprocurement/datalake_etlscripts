#!/usr/bin/env python
# coding: utf-8
from mmb_data.mongo_db_connect import Mongo_db
import sys
from swift_secrets import MONGODB_HOST
import swiftclient as sw
import argparse
from yaml import load, CLoader
import requests
import ntp_entry as ntp

tmpdir = "/tmp"

def main():
    # usage: get_documents.py [-h] [--update] [--ini INI] [--fin FIN] [--id ID]
    #                         [--where {disc,gridfs,swift}]

    # Download documents

    # optional arguments:
    #   -h, --help            show this help message and exit
    #   --update              Add new files only
    #   --ini INI             Initial document range
    #   --fin FIN             Final document range
    #   --id ID               Selected document id
    #   --where {disc,gridfs,swift}
    #                         Selected storage (disk|gridfs|swift)    
    #   --config              Configuration file

    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--update', action='store_true', help="Add new files only")
    parser.add_argument('--ini', action='store', help="Initial document range")
    parser.add_argument('--fin', action='store', help="Final document range")
    parser.add_argument('--id', action='store', help="Selected document id")
    parser.add_argument('--where', action='store', default='disc', choices=['disc', 'gridfs', 'swift'], help="Selected storage (disk|gridfs|swift)")
    parser.add_argument('--config'  action='store' default='secrets.yml', help="Configuration file (default;secrets.yml)")
    args = parser.parse_args()

    # Config file
    with open(args.config)  as config_file:
        config = load(config_file, Loader=CLoader)
    
    print("Connecting to MongoDB")
    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement', 
        False, 
        config['MONGODB_AUTH']
        credentials=config['MONGODB_CREDENTIALS']
    )
    incoming_col = db_lnk.get_collections(['incoming','contratos'])['incoming']

    print("Connecting to storage...")
    if args.where == 'disc':
        storage = ntp.NtpStorage(type='disc', data_dir='/tmp')
    elif args.where == 'gridfs':
        storage = ntp.NtpStorage(type='gridfs', gridfs_obj=db_lnk.get_gfs('downloadedDocuments'))
    elif args.where == 'swift':
        swift_conn = sw.Connection(
            authurl=config['OS_AUTH_URL'], 
            auth_version=3,
            os_options = {
                'auth_type': config['OS_AUTH_TYPE'],
                'region_name': config['OS_REGION_NAME'],
                'application_credential_id': config['OS_APPLICATION_CREDENTIAL_ID'],
                'application_credential_secret': config['OS_APPLICATION_CREDENTIAL_SECRET'],
                'service_project_name': 'bsc22NextProcurement'
            }
        )
        storage = ntp.NtpStorage(
            type='swift', 
            swift_connection=swift_conn,
            swift_container='nextp-data',
            swift_prefix='data'
        )

    print("Getting ids...")
    if args.id:
        if ntp.check_ntp_id(args.id):
            query = {'_id': args.id}
        else:
            print(f'--fin {args.fin} argument is not a valid ntp id')
            sys.exit()
    else:
        query = [{}]
        if args.ini is not None:
            if ntp.check_ntp_id(args.ini):
                query.append({'_id':{'$gte': args.ini}})
            else:
                print(f'--ini {args.ini} argument is not a valid ntp id')
                sys.exit()
        if args.fin is not None:
            if ntp.check_ntp_id(args.fin):
                query.append({'_id':{'$lte': args.fin}})
            else:
                print(f'--fin {args.fin} argument is not a valid ntp id')
                sys.exit()

    for id in list(incoming_col.find({'$and':query}, {'_id':1})):
        ntp_id = id['_id']
        doc = ntp.NtpEntry()
        doc.load_from_db(incoming_col, ntp_id)
        #print(vars(doc))

        for url_field in doc.extract_urls():
            print(url_field, doc.data[url_field])
            if doc.download_document(url_field, storage=storage):
                print("Downloaded")
            else:
                print("Skipped, not a pdf")

if __name__ == "__main__":
    main()    
