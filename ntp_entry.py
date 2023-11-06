''' Classes NtpEntry '''
import sys
import re
import copy
import os.path
import logging
import requests
import numpy as np
from unidecode import unidecode
from urllib.parse import urlparse, unquote
import pandas as pd
from bs4 import BeautifulSoup
from http import HTTPStatus
import dns.resolver

ACCEPTED_DOC_TYPES = (
    '7z', 'doc', 'docx', 'pdf',
    'tcq', 'dwg', 'odg', 'odt',
    'rar', 'rtf', 'tcq', 'txt',
    'xls', 'xlsm', 'xlsx', 'zip'
)

TIMEOUT = 10

REDIRECT_CODES = (301, 302, 303, 307, 308)
MAX_REDIRECTS = 30

# EXIT_CODES
SKIPPED = 1
UNWANTED_TYPE = 2
STORE_OK = 200
SSL_ERROR = 3
ERROR = -1

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

def get_new_dbfield(col):
    mod_col = col.replace('ContractFolderStatus - ', '').replace(' - ', '_').replace(' ', '_')
    if '_(' in mod_col:
        m = re.search(r'(.*)_\((.*)\)', mod_col)
        mod_col = f"{m[2]}/{m[1]}"
    return unidecode(mod_col)

def parse_parquet(pd_data_row, new_cols):
    ''' Parse data Pandas' data row read from a parquet file
        Parameters:
            pd_data_row (Pandas' data row): Single entry from pandas dataframe
            new_cols (dict): Dictionary with translated columns names
    '''
    new_data = {}
    r = False
    for col in pd_data_row:
        if isinstance(pd_data_row[col], np.ndarray):
            tmp_list = []
            for item in pd_data_row[col].tolist():
                if item.startswith('['):
                    new_list = eval(item) # Transform string list into actual list
                    tmp_list.append(new_list)
                else:
                    tmp_list.append(item)
            if len(tmp_list) == 1:
                tmp_list = tmp_list[0] # Remove useless list level for single item list.
            pd_data_row[col] = tmp_list
            if pd_data_row[col] == 'nan':
                pd_data_row[col] = ''

        elif pd.isna(pd_data_row[col]):
            pd_data_row[col] = ''

        try:
            if new_cols.loc[col]['DBFIELD'] in new_data:
                r = True
                if not isinstance(new_data[new_cols.loc[col]['DBFIELD']], list):
                    new_data[new_cols.loc[col]['DBFIELD']] = [new_data[new_cols.loc[col]['DBFIELD']]]
                    logging.debug(f"WARNING: multiple values found for {new_cols.loc[col]['DBFIELD']}, appending")
                new_data[new_cols.loc[col]['DBFIELD']].append(pd_data_row[col])
            else:
                new_data[new_cols.loc[col]['DBFIELD']] = pd_data_row[col]
        except KeyError:
            mod_col = get_new_dbfield(col)
            logging.error(f'"{col}"\t"{mod_col}"\t"string"\n')
    # if r:
    #    print(new_data,"\n")
    return new_data


def _check_meta_refresh(url, contents):
    """Check for redirection as http-equiv:refresh tags"""
    soup = BeautifulSoup(contents, features='lxml')
    result = soup.find("meta", attrs={"http-equiv": "refresh"})
    if result:
        wait, text = result["content"].split(";")
        if text.strip().lower().startswith("url="):
            redir_url = text.strip()[4:].replace("'", "")
            logging.debug(f"Found meta refresh {redir_url}")
            if redir_url.startswith('/'):
                parsed_url = urlparse(url)
                redir_url = f"{parsed_url.scheme}://{parsed_url.hostname}{redir_url}"
            logging.debug(f"New URL found {redir_url}")
            return redir_url
    return ''

def _get_ips(url):
    ip_solver = dns.resolver.query(urlparse(url).netloc)
    ips = []
    for ipval in ip_solver:
        ips.append(ipval.to_text())
    return ips

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

    def order_from_id(self):
        self.ntp_order = parse_ntp_id(self.ntp_id)

    def _find_previous_doc(self, col):
            # old_doc = col.find_one(
            #     {'id': self.data['id'], 'updated': self.data['updated']}
            # )
            found = False
            old_doc = {}
            for vers in col.find({'id': self.data['id']}):
                found = vers['updated'] == self.data['updated']
                print(vers['updated'], self.data['updated'], found)
                if found:
                    old_doc = vers
                    break
            sys.exit()
            return old_doc

    def commit_to_db(self, col, upsert=False):
        if upsert:
            # old_doc = col.find_one(
            #     {'id': self.data['id'], 'updated': self.data['updated']}
            # )
            old_doc = self._find_previous_doc(col)
            if old_doc and old_doc['_id']:
                logging.info(f"Updating previous version {old_doc['_id']}")
                self.data['_id'] = old_doc['_id']
                self.ntp_id = old_doc['_id']
                self.order_from_id()
        try:
            col.replace_one(
                {'_id': self.data['_id']},
                 self.data,
                 upsert=True
            )
        except Exception as e:
            logging.debug(self.data)
            for k in self.data:
                logging.debug(f"{k} {self.data[k]} {type(self.data[k])}")
            logging.error(e)
            sys.exit(1)

        return self.ntp_order

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
            if isinstance(self.data[k], list):
                for index, url in enumerate(self.data[k]):
                    if isinstance(url, str) and url.startswith('http'):
                        urls[f"{k}:{index}"] = url
        return urls

    def get_file_name(self, field, ext):
        return f'{self.ntp_id}_{field}.{ext}'

    def get_server(self, field):
        if ':' in field:
            base, index = field.split(':')
            return urlparse(self.data[base][int(index)]).netloc
        else:
            base = field
            return urlparse(self.data[field]).netloc

    def store_document(
            self,
            field,
            storage=None,
            replace=False,
            scan_only=False,
            allow_redirects=False,
            verify_ca=True,
            skip_early=False
    ):
        ''' Retrieves and stores document accounting for possible redirections'''
        if ':' in field:
            base, index = field.split(':')
            url = unquote(self.data[base][int(index)]).replace(' ', '%20').replace('+', '')
        else:
            base = field
            url = unquote(self.data[field]).replace(' ', '%20').replace('+', '')
        if skip_early:
            if storage.type != 'gridfs':
                logging.error(f"--skip_early only available for GridFS storage  (yet)")
                sys.exit(1)
            file_name_root = self.get_file_name(field, '')
            if storage.file_exists(file_name_root, no_ext=True):
                return SKIPPED, field
        try:
            logging.debug(f"IP: {','.join(_get_ips(url))}")
            response = requests.get(
                url,
                timeout=TIMEOUT,
                allow_redirects=allow_redirects,
                verify=verify_ca
            )
            logging.debug(response.headers)
            num_redirects = 0
            while response.status_code in REDIRECT_CODES and num_redirects <= MAX_REDIRECTS:
                num_redirects +=1
                url = response.headers['Location']
                logging.warning(f"Found {response.status_code}: Redirecting to {url}")
                logging.debug(f"IP: {','.join(_get_ips(url))}")
                response = requests.get(
                    url, timeout=TIMEOUT,
                    verify=verify_ca
                )
            if num_redirects > MAX_REDIRECTS:
                logging.warning(f"Max. Redirects {MAX_REDIRECTS} achieved, skipping")

            if response.status_code == 200:
                doc_type = get_file_type(response.headers)

                if doc_type:
                    logging.debug(f"DOC_TYPE {doc_type}")
                else:
                    logging.debug(f"EMPTY DOC TYPE at {self.ntp_id}")

                if doc_type == 'html':
                    redir_url = _check_meta_refresh(url, response.content)
                    if redir_url:
                        logging.debug(f"IP: {','.join(_get_ips(url))}")
                        response = requests.get(
                            redir_url,
                            timeout=TIMEOUT,
                            allow_redirects=allow_redirects,
                            verify=verify_ca
                        )
                        logging.debug(response.headers)
                        if response.status_code == 200:
                            doc_type = get_file_type(response.headers)
                            logging.debug(f"New doc type {doc_type}")
                            url = redir_url
                        else:
                            return response.status_code, 'Error on redirect'

                if doc_type in ACCEPTED_DOC_TYPES:
                    file_name = self.get_file_name(field, doc_type)
                    if not scan_only and (replace or not storage.file_exists(file_name)):
                        storage.file_store(file_name, response.content)
                        return STORE_OK, doc_type
                    return SKIPPED, doc_type
                return UNWANTED_TYPE, doc_type
            logging.error(f"{HTTPStatus(response.status_code).phrase}: {url}")
            return response.status_code, HTTPStatus(response.status_code).phrase
        except requests.exceptions.SSLError as err:
            logging.error(err)
            return SSL_ERROR, err
        except requests.exceptions.ReadTimeout:
            logging.error(f"TimeOut: {url}")
            return ERROR, 'Timeout'
        except Exception as err:
            logging.error(err)
        return ERROR, 'unknown'

    def diff_document(self, other):
        ''' Find patch from two versions of atom'''
        new = {}
        modif = {}
        miss ={}
        for k in self.data:
            if k == '_id':
                continue
            if k in other.data:
                if self.data[k] != other.data[k]:
                    modif[k] = (self.data[k], other.data[k])
            else:
                miss[k] = self.data[k]

        for k in other.data:
            if k not in self.data:
                new[k] = other.data[k]
        return (new, modif, miss)

def get_file_type(headers):
    doc_type = ''
    debug = []
    if 'Content-type' in headers:
        debug.append(f"Content-type: {headers['Content-type']}")
        if headers['Content-type'] == 'application/pdf':
            doc_type = 'pdf'
        elif headers['Content-type'].startswith('text/html'):
            doc_type = 'html'
        elif headers['Content-type'] == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
            doc_type = 'docx'
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
