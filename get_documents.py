#!/usr/bin/env python
# coding: utf-8
from mmb_data.mongo_db_connect import Mongo_db
import sys
# import re
import argparse
# import json
import requests
import ntp_entry as ntp

tmpdir = "/tmp"
# def parse_ntp_id(ntp_id):
#     return int(ntp_id.replace('ntp',''))
# def check_ntp_id(ntp_id):
#     return re.match(r'^ntp[0-9]{8}', ntp_id)

# class NtpEntry:
#     def __init__(self):
#         self.ntp_order = 0
#         self.ntp_id = ''
#         self.data = {}

#     def load_data(self, ntp_order, data):
#         self.ntp_order = ntp_order
#         self.set_ntp_id()
#         self.data = data

#         self.data['_id'] = self.ntp_id
#     def set_ntp_id(self):
#         self.ntp_id = 'ntp{:s}'.format(str(self.ntp_order).zfill(8))

#     def order_from_id(self, id):
#         self.ntp_order = parse_ntp_id(self.ntp_id)

#     def commit_to_db(self, col):
#         try:
#             new_id = col.insert_one(self.data)
#         except Exception as e:
#             print(self.data)
#             for k in self.data:
#                 print(k, self.data[k], type(self.data[k]))
#             print("ERROR",e)
#             sys.exit()
#         return new_id

#     def load_from_db(self, col_id,  ntp_id):
#         try:
#             self.data = col_id.find_one({'_id': ntp_id})
#             self.ntp_id = ntp_id
#             self.ntp_order = parse_ntp_id(ntp_id)
#         except Exception as e:
#             print(e)
#             sys.exit()

#     def extract_url(self):
#         urls = {}
#         for k in self.data:
#             if isinstance(self.data[k], str) and self.data[k].startswith('http'):
#                 urls[k] = self.data[k]
#         return urls

#     def download_document(
#             self, 
#             field,
#             towhere='disk', 
#             tmpdir='/tmp', 
#             grid_fs_col='',
#             update=False
#             ):
#         file_name = f'{self.ntp_id}_{field}.pdf'
#         if update or not self.check_file_exists(
#                 file_name, 
#                 towhere=towhere
#                 tmpdir=tmpdir, 
#                 grid_fs_col=grid_fs_col''
#             )

#             url = self.data[field]
#             r = requests.get(url, stream=True)
#             if r.status_code == 200:
#                 if 'Content-type' not in r.headers or r.headers['Content-type'].startswith('text/html') :
#                     return False
#                 if towhere == 'disc':
#                     with open(f"{tmpdir}/{file_name}", 'bw') as output_file:
#                         output_file.write(r.content)
#                 elif towhere == 'gridfs':
#                     grid_fs_col.put(r.content, filename=file_name)
#                 elif towhere == 'swift':
#                     pass
#             return True
#         return False
        
#         def check_file_exists(self, file_name, towhere, tmpdir, grid_fs_col):
#             return False

def main():
    parser = argparse.ArgumentParser(description='Download documents')
    parser.add_argument('--update', action='store_true')
    parser.add_argument('--ini', action='store')
    parser.add_argument('--fin', action='store')
    parser.add_argument('--where', action='store', default='disc', choices=['disc', 'gridfs', 'swift'])
    args = parser.parse_args()

    db_lnk = Mongo_db('mdb-login.bsc.es', 'nextprocurement', False, True)
    incoming_col = db_lnk.get_collections(['incoming','contratos'])['incoming']
    documents_gfs = db_lnk.get_gfs('downloadedDocuments')

    # Getting ids
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

    id_list = list(incoming_col.find({'$and':query}, {'_id':1}))
    for id in id_list:
        ntp_id = id['_id']
        doc = ntp.NtpEntry()
        doc.load_from_db(incoming_col, ntp_id)
        print(vars(doc))
        for url_field in doc.extract_url():
            print(url_field, doc.data[url_field])
            if doc.download_document(
                    url_field, 
                    towhere=args.where,
                    grid_fs_col=documents_gfs
                ):
                print("Downloaded")
            else:
                print("Skipped, not a pdf")

if __name__ == "__main__":
    main()    
