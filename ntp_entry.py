''' Classes NtpEntry '''
import sys
import re
import copy
import os.path
import logging
import requests
import numpy as np
from urllib.parse import urlparse
import pandas as pd

ACCEPTED_DOC_TYPES = (
    '7z', 'doc', 'docx', 'pdf',
    'tcq', 'dwg', 'odg', 'odt',
    'rar', 'rtf', 'tcq', 'txt',
    'xls', 'xlsm', 'xlsx', 'zip'
)

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
        self.data = copy.deepcopy(data)
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

    def get_file_name(self, field, ext):
        return f'{self.ntp_id}_{field}.{ext}'

    def get_server(self, field):
        return urlparse(self.data[field]).netloc

    def store_document(
            self,
            field,
            storage=None,
            replace=False,
            scan_only=False
            ):
        url = self.data[field]
        try:
            r = requests.head(url, timeout=5)
            logging.debug(r.headers)
            if r.status_code == 200:
                doc_type = get_file_type(r.headers)
                if doc_type:
                    logging.debug(f"DOC_TYPE {doc_type}")
                else:
                    logging.debug(f"EMPTY DOC TYPE at {self.ntp_id}")
                if doc_type in ACCEPTED_DOC_TYPES:
                    file_name = self.get_file_name(field, doc_type)
                    if not scan_only and (replace or not storage.file_exists(file_name)):
                        res = requests.get(url, stream=True)
                        storage.file_store(file_name, res.content)
                        return doc_type
                    return 1
                return 2
            logging.error(f"Not Found: {url}")
            return r.status_code
        except requests.exceptions.ReadTimeout:
            logging.warning(f"TimeOut: {url}")
        except Exception as e:
            logging.warning(e)
        return -1

def get_file_type(headers):
    doc_type = ''
    debug = []
    if 'Content-type' in headers:
        debug.append(f"Content-type: {headers['Content-type']}")
        if headers['Content-type'] == 'application/pdf':
            doc_type='pdf'
        elif headers['Content-type'].startswith('text/html'):
            doc_type='html'
    if 'Content-disposition' in headers:
        debug.append(f"Content-Disposition: {headers['Content-Disposition']}")
        headers['Content-disposition'] = headers['Content-disposition'].replace('769;','_').replace('8230;','_')
        for item in headers['Content-disposition'].split(';'):
            if 'filename' in item:
                lb, file_name = item.split('=', maxsplit=1)
                file_name = file_name.replace(' .', '.').lower()
                logging.debug(file_name)
                doc_type = os.path.splitext(file_name)[1].replace('.', '').replace('?=', '').replace('"', '')
    logging.debug(f"HEADS {debug} {doc_type}")
    return doc_type
