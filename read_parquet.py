#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from mmb_data.mongo_db_connect import Mongo_db
import numpy as np
import sys
import argparse

parser = argparse.ArgumentParser(description='Parse NextProcurement parquets')
parser.add_argument('--drop', action='store_true')
parser.add_argument('codes_file')
parser.add_argument('pkt_file')
args = parser.parse_args()

db_lnk = Mongo_db('mdb-login.bsc.es', 'nextprocurement', False, True)

incoming_col = db_lnk.get_collections(['incoming','contratos'])['incoming']

data_table = pd.read_parquet(args.pkt_file, use_nullable_dtypes=True)

new_cols = pd.read_csv(args.codes_file, sep='\t', index_col='ORIGINAL')
if args.drop:
    incoming_col.drop()
    
id_num = 1
for i in data_table.index:
    data = data_table.iloc[i].to_dict().copy()
    new_data = {'_id' : 'ntp{:s}'.format(str(id_num).zfill(8))}
    id_num += 1
    for k in data:
        print(type(data[k]))
        if isinstance(data[k], np.ndarray):
            data[k] = list(data[k])
        elif pd.isna(data[k]):
            data[k] = ''
        new_data[new_cols.loc[k]['DBFIELD']] = data[k]
    try:
        new_id = incoming_col.insert_one(new_data)
    except Exception as e:
        print(new_data)
        for k in new_data:
            print(k, new_data[k], type(new_data[k]))
        print("ERROR",e)
        sys.exit()



