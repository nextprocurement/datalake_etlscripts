
import pandas as pd
import requests
import numpy as np
import sys
import re
import argparse
from mmb_data.mongo_db_connect import Mongo_db

def parse_ntp_id(ntp_id):
    return int(ntp_id.replace('ntp',''))

def check_ntp_id(ntp_id):
    return re.match(r'^ntp[0-9]{8}', ntp_id)

def parse_parquet(pd_data_row, new_cols):
    new_data = {}
#    print(pd_data_row)
    for k in pd_data_row:
#        print(k)
        if isinstance(pd_data_row[k], np.ndarray):
            pd_data_row[k] = list(pd_data_row[k])
        elif pd.isna(pd_data_row[k]):
            pd_data_row[k] = ''
        new_data[new_cols.loc[k]['DBFIELD']] = pd_data_row[k]
    return new_data

class NtpStorage:
    def __init__(self, type='disc', **kwargs):
        self.type = type
        if type == 'disc':
            self.data_dir = kwargs['data_dir']
        elif type == 'gridfs':
            self.gridfs = kwargs['gridfs_obj']
        elif type == 'swift':
            self.swift_connection = kwargs['swift_connection']
            self.swift_container = kwargs['swift_container']
            self.swift_prefix = kwargs['swift_prefix']
    
    def file_store(self, file_name, contents):   
        if self.type == 'disc':
            with open(f"{self.data_dir}/{file_name}", 'bw') as output_file:
                    output_file.write(contents)
        elif self.type == 'gridfs':
            self.gridfs.put(contents, filename=file_name)
        elif self.type == 'swift':
            self.swift_connection.put_object(
                self.swift_container,
                f"{self.swift_prefix}/{file_name}",
                contents=contents
            )
    
    def file_exists(self, file_name):
        if self.type == 'disc':
            return False
        elif self.type == 'gridfs':
            return False
        elif self.type == 'swift':
            return False
    
class NtpEntry:
    def __init__(self):
        self.ntp_order = 0
        self.ntp_id = ''
        self.data = {}

    def load_data(self, ntp_order, data):
        self.ntp_order = ntp_order
        self.set_ntp_id()
        self.data = data
        self.data['_id'] = self.ntp_id

    def set_ntp_id(self):
        self.ntp_id = 'ntp{:s}'.format(str(self.ntp_order).zfill(8))

    def order_from_id(self, id):
        self.ntp_order = parse_ntp_id(self.ntp_id)

    def commit_to_db(self, col):
        try:
            new_id = col.insert_one(self.data)
        except Exception as e:
            print(self.data)
            for k in self.data:
                print(k, self.data[k], type(self.data[k]))
            print("ERROR",e)
            sys.exit()
        return new_id

    def load_from_db(self, col_id,  ntp_id):
        try:
            self.data = col_id.find_one({'_id': ntp_id})
            self.ntp_id = ntp_id
            self.ntp_order = parse_ntp_id(ntp_id)
        except Exception as e:
            print(e)
            sys.exit()

    def extract_urls(self):
        urls = {}
        for k in self.data:
            if isinstance(self.data[k], str) and self.data[k].startswith('http'):
                urls[k] = self.data[k]
        return urls

    def download_document(
            self, 
            field,
            storage=None,
            update=False
            ):
        file_name = f'{self.ntp_id}_{field}.pdf'
        if update or not storage.file_exists(file_name):  
            url = self.data[field]
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                if 'Content-type' not in r.headers or r.headers['Content-type'].startswith('text/html') :
                    return False
                storage.file_store(file_name, r.content)
            return True
        return False
    