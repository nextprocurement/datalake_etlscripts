#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from mmb_data.mongo_db_connect import Mongo_db
import numpy as np
import sys
import argparse

def parse_parquet(pd_data_row, new_cols):
    new_data = {}
    print(pd_data_row)
    for k in pd_data_row:
        print(k)
        if isinstance(pd_data_row[k], np.ndarray):
            pd_data_row[k] = list(pd_data_row[k])
        elif pd.isna(pd_data_row[k]):
            pd_data_row[k] = ''
        new_data[new_cols.loc[k]['DBFIELD']] = pd_data_row[k]
    return new_data

def parse_ntp_id(ntp_id):
    return int(ntp_id.replace('ntp',''))
class NtpEntry:
    def __init__(self, ntp_order, data):
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

def main():
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
        id_num = 0
    else:
        max_id = incoming_col.aggregate(
                    [{'$group':{'_id':'max_id', 'value':{'$max': '$_id'}}}]
                )
        id_num = parse_ntp_id(list(max_id)[0]['value'])
    print(f"Last reference found {id_num}")

    for i in data_table.index:
        data_row = data_table.iloc[i].to_dict().copy()
        id_num += 1
        new_data = NtpEntry(id_num, parse_parquet(data_row, new_cols))
        new_data.commit_to_db(incoming_col)

if __name__ == "__main__":
    main()    
