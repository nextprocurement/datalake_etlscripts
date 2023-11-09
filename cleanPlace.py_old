import sys
import argparse
import logging
import os
import time
from types import NoneType
from yaml import load, CLoader
import swiftclient as sw
import ntp_entry as ntp
import ntp_storage as ntpst
from mmb_data.mongo_db_connect import Mongo_db

def diff (a, b):
    missing = []
    new = []
    diff = []
    for k,v in a.items():
        if k == '_id':
            continue
        if k in b and b[k] == a[k]:
            continue
        if not k in b:
            missing.append({k:a[k]})
        elif b[k] != a[k]:
            diff.append({k +'_0':a[k], k +'_1':b[k]})
    for k,v in b.items():
        if not k in a:
            new.append({k:b[k]})
    return missing, new, diff

def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Clean duplicates PLACE')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')

    args = parser.parse_args()
    # Setup logging
    logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')
    if args.debug:
        logging.getLogger().setLevel(10)
    else:
        logging.getLogger().setLevel(20)

 # Config file
    with open(args.config, 'r')  as config_file:
        config = load(config_file, Loader=CLoader)

    if args.verbose:
        logging.info("Connecting to MongoDB")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )

    incoming_col = db_lnk.db.get_collection('place')
    # Check multiple docs
    IDS = [doc['_id'] for doc in incoming_col.aggregate([{"$group":{"_id":"$id","cnt":{"$sum":1}}},{"$match":{"cnt":{"$gt":1}}}])]
    for id in IDS:
        data = list(incoming_col.find({'id': id}))
        for idx, doc in enumerate(data):
            if idx == 0:
                doc_0 = doc
                continue
            print(diff (doc_0, doc))
            sys.exit()



if __name__ == "__main__":
    main()
