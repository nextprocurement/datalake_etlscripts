''' Classes NtoStorage and NtpEntry '''
import pandas as pd
import requests
import numpy as np
import sys
import re
import os.path
from os.path import join as opj
import logging
import argparse
from mmb_data.mongo_db_connect import Mongo_db

def parse_ntp_id(ntp_id):
    ''' Get document order from ntp_id
        Parameters:
            ntp_id (str): ntp id as ntp[0-9]{8}
    '''
    return int(ntp_id.replace('ntp',''))

def check_ntp_id(ntp_id):
    ''' Check ntp id syntax
        Parameters:
            ntp_if (str)
    '''
    return re.match(r'^ntp[0-9]{8}', ntp_id)

def parse_parquet(pd_data_row, new_cols):
    ''' Parse data Pandas' data row read from a parquet file
        Parameters:
            pd_data_row (Pandas' data row): Single entry from pandas dataframe
            new_cols (dict): Dictionary with translated columns names
    '''
    new_data = {}
    for col in pd_data_row:
        if isinstance(pd_data_row[col], np.ndarray):
            pd_data_row[col] = list(pd_data_row[col])
        elif pd.isna(pd_data_row[col]):
            pd_data_row[col] = ''
        new_data[new_cols.loc[col]['DBFIELD']] = pd_data_row[col]
    return new_data

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
            logging.debug(self.data)
            for k in self.data:
                logging.debug(f"{k} {self.data[k]} {type(self.data[k])}")
            logging.error(e)
            sys.exit(1)
        return new_id

    def load_from_db(self, col_id,  ntp_id):
        try:
            self.data = col_id.find_one({'_id': ntp_id})
            self.ntp_id = ntp_id
            self.ntp_order = parse_ntp_id(ntp_id)
        except Exception as e:
            logging.error(e)
            sys.exit(1)

    def extract_urls(self):
        urls = {}
        for k in self.data:
            if isinstance(self.data[k], str) and self.data[k].startswith('http'):
                urls[k] = self.data[k]
        return urls

    def get_file_name(self, field):
        return f'{self.ntp_id}_{field}.pdf'

    def store_document(
            self,
            field,
            storage=None,
            replace=False
            ):
        file_name = self.get_file_name(field)
        if replace or not storage.file_exists(file_name):
            url = self.data[field]
            r = requests.get(url, stream=True)
            logging.debug(f"Headers: {r.headers}")
            if r.status_code == 200:
                #TODO check headers for more options
                not_valid_document = check_document(r.headers) 
                if not_valid_document:
                    return not_valid_document
                else:
                    storage.file_store(file_name, r.content)
                return 0
        return 1

def check_document(headers):
    #TODO check other options
    if 'Content-type' not in headers or headers['Content-type'].startswith('text/html'):
        return 2
    else:
        return 0
              
    