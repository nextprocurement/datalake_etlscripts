''' Classes NtpEntry '''
import sys
import re
import os.path
import logging
from urllib.parse import urlparse
from datetime import datetime
from unidecode import unidecode
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import dns.resolver

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
    ''' Suggest a new label for missing fields'''
    mod_col = col.replace('ContractFolderStatus - ', '').replace(' - ', '_').replace(' ', '_')
    if '_(' in mod_col:
        m = re.search(r'(.*)_\((.*)\)', mod_col)
        mod_col = f"{m[2]}/{m[1]}"
    return unidecode(mod_col)

def get_last_order(group, col):
    ''' Get order of last document in collection'''
    if group in ['outsiders', 'insiders']:
        cond = {'_id': {'$regex': 'ntp0'}}
    else:
        cond = {'_id': {'$regex': 'ntp1'}}

    max_id = list(col.aggregate(
        [
            {'$match': cond},
            {'$group':{'_id':'max_id', 'value':{'$max': '$_id'}}}
        ]
    ))
    if max_id:
        id_num = parse_ntp_id(max_id[0]['value'])
    else:
        logging.info("No records found")
        id_num = cts.MIN_ORDER(group)
    return id_num

def parse_parquet(pd_data_row, new_cols):
    ''' Parse data Pandas' data row read from a parquet file
        Parameters:
            pd_data_row (Pandas' data row): Single entry from pandas dataframe
            new_cols (dict): Dictionary with translated columns names
    '''
    new_data = {}
    for col in pd_data_row:
        if isinstance(pd_data_row[col], np.ndarray):
            tmp_list = []
            for item in pd_data_row[col].tolist():
                if item.startswith('['):
                    try:
                        new_list = eval(item) # Transform string list into actual list
                    except Exception as e:
                        logging.error(e)
                        logging.error(item)
                        new_list = item
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
                if not isinstance(new_data[new_cols.loc[col]['DBFIELD']], list):
                    new_data[new_cols.loc[col]['DBFIELD']] = [new_data[new_cols.loc[col]['DBFIELD']]]
                    logging.debug(f"WARNING: multiple values found for {new_cols.loc[col]['DBFIELD']}, appending")
                new_data[new_cols.loc[col]['DBFIELD']].append(pd_data_row[col])
            else:
                new_data[new_cols.loc[col]['DBFIELD']] = pd_data_row[col]
        except KeyError:
            mod_col = get_new_dbfield(col)
            logging.error(f'"{col}"\t"{mod_col}"\t"string"\n')
        new_data['data_model'] = 'v2023'
    return new_data

def get_versions(new_id, col):
    ''' get list versions of the incoming document'''
    versions = []
    for vers in col.find(
        {'id': new_id},
        projection={
            '_id': 1,
            'id': 1,
            'obsolete_version':1,
            'updated':1
        }):
        if 'obsolete_version' in vers and vers['obsolete_version']:
            versions.append({
                '_id': vers['_id'],
                'id': vers['id'],
                'status': 'obsolete'
            })
        else:
            if not isinstance(vers['updated'], list):
                vers['updated'] = [vers['updated']]
            for update in vers['updated']:
                versions.append({
                    '_id': vers['_id'],
                    'id': vers['id'],
                    'status': 'active',
                    'updated': update
                })

    return versions

def get_last_active_version(new_data, versions):
    ''' Get last active document for tender'''
    last_vers = {'_id': 'ntp00000000'}
    for vers in versions:
        if vers['status'] == 'obsolete':
            continue
        found = exists_update(new_data['updated'], vers['updated'])
        if found:
            last_vers = vers
            break
        elif vers['_id'] > last_vers['_id']:
            last_vers = vers
    if last_vers['_id'] == 'ntp00000000':
        return False
    return last_vers

# Crawling utils

def check_meta_refresh(url, contents):
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

def get_ips(url):
    ''' get server ip'''
    ip_solver = dns.resolver.query(urlparse(url).netloc)
    ips = []
    for ipval in ip_solver:
        ips.append(ipval.to_text())
    return ips

def get_file_type(headers):
    '''Obtain file type form HTTP headers'''
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

def get_server(data, field):
    '''Get server from url a field'''
    if ':' in field:
        base, index = field.split(':')
        return urlparse(data[base][int(index)]).netloc
    base = field
    return urlparse(data[field]).netloc

def get_file_name(ntp_id, field, ext):
    ''' Composes file name for stored documents'''
    return f"{ntp_id}_{field}.{ext}"

def exists_update(new_update, existing_update):
    '''Check if new_update exists in previous'''
    if isinstance(existing_update, datetime):
        existing_update = existing_update.strftime('%Y-%m-%d %H:%M:%S')
    vl = isinstance(existing_update, list)
    if isinstance(new_update, datetime):
        new_update = new_update.strftime('%Y-%m-%d %H:%M:%S')
    nl = isinstance(new_update, list)
    # Comparison limited to YYYY-MM-DD HH:MM:SS to avoid format issues
    if vl:
        vers_tmp = [item[0:19] for item in existing_update]
    else:
        vers_tmp = existing_update[0:19]

    if nl:
        new_tmp = [item[0:19] for item in new_update]
    else:
        new_tmp = new_update[0:19]

    if vl and nl or not vl and not nl:
        found = vers_tmp == new_tmp
    elif nl:
        found = vers_tmp in new_tmp
    else:
        found = new_tmp in vers_tmp

    return found

def merge_updates(new_update, old_updates):
    updates = set()
    if not isinstance(new_update, list):
        new_update = [new_update]
    for update in old_updates + new_update:
        if isinstance(update, datetime):
            update = update.strftime('%Y-%m-%d %H:%M:%S')
        updates.add(update[0:19])
    return sorted(list(updates))

def find_previous_doc(data, col):
    '''Finds previous doc if exists that matched new document'''
    found = False
    old_doc = {}
    for vers in col.find({'id': data['id']}):
        found = exists_update(data['updated'], vers['updated'])
        logging.debug(f"{vers['updated']} {data['updated']} {found}")
        logging.debug(f"{vers_tmp} {new_tmp} {found}")

        if found:
            old_doc = vers
            break
    return old_doc
