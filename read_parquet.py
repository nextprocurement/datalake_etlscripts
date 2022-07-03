#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from mmb_data.mongo_db_connect import Mongo_db
import numpy as np
import sys
import argparse




db_lnk = Mongo_db('localhost', 'nextprocurement', False, False)

incoming_col, done_col = db_lnk.get_collections(['incoming']['done'])

pkt_file = '/home/gelpi/DEVEL/NEXTP/outsiders_2021.parquet'

data_table = pd.read_parquet(pkt_file, use_nullable_dtypes=True)

new_cols = pd.read_csv('columns.csv', sep='\t', index_col='ORIGINAL')


id_num = 1
for i in data_table.index:
    data = data_table.iloc[i].to_dict().copy()
    new_data = {'_id' : 'ntp{:s}'.format(str(id_num).zfill(8))}
    id_num += 1
    for k in data:
        if isinstance(data[k], pd._libs.missing.NAType) or\
            isinstance(data[k], pd._libs.tslibs.nattype.NaTType):
            data[k] = ''
        if isinstance(data[k], np.ndarray):
            data[k] = list(data[k])
        new_data[new_cols.loc[k]['DBFIELD']] = data[k]
    try:
        new_id = incoming_col.insert_one(new_data)
    except Exception as e:
        print(new_data)
        for k in new_data:
            print(k, new_data[k], type(new_data[k]))
        print("ERROR",e)
        sys.exit()



