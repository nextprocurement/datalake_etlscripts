
import logging
import sys

from pymongo import DeleteOne, UpdateOne, UpdateMany
from pymongo.errors import BulkWriteError

CTS = {
    'UPDATE': 0,
    'UPSERT': 1,
    'DELETE': 2,
    'INSERT': 3
}

OP_LABELS = ['update', 'upsert', 'delete', 'insert']

class MongoDBBulkWrite():

    def __init__(self, collection, mode, size):
        self.collection = collection
        self.desc = OP_LABELS[mode]
        self.mode = mode
        self.length = size
        self.data = []
        self.ibuff = 0
        self.total = 0
        self.removed = 0
        self.upserted = 0
        self.modified = 0
        self.inserted = 0

    def clean(self):
        self.data = []
        self.ibuff = 0

    def append(self, id, val, ser_id=None):
        self.data.append({
            'id': id,
            'val': val,
            'ser_id': ser_id
        })
        self.ibuff += 1

    def full(self):
        return self.ibuff >= self.length

    def reset(self):
        self.clean()
        self.total = 0

    def commit_data(self, if_full=True, many=False):
        if not if_full or self.full():
            if self.ibuff:
                if self.mode == CTS['INSERT']:
                    buffer = []
                    for item in self.data:
                        buffer.append(item['val'])
                        self.collection.insert_many(buffer)
                    log = 'Committing {:7.0f} ops. ({:8.0f}) to {:15s}:'.format(
                        self.ibuff, self.total, self.collection.name
                    )
                    log += '{:7.0f} inserted'.format(len(buffer))
                    logging.info(log)
                    self.inserted += len(buffer)
                else:
                    bulk = []
                    last_id = ''
                    for item in self.data:
                        if self.mode == CTS['UPSERT']:
                            bulk.append(UpdateOne(item['id'], item['val'], upsert=True))
                        elif self.mode == CTS['DELETE']:
                            bulk.append(DeleteOne(item['id']))
                        else:
                            if many:
                                bulk.append(UpdateMany(item['id'], item['val']))
                            else:
                                bulk.append(UpdateOne(item['id'], item['val']))
                        if item['ser_id']:
                            last_id = item['ser_id']
                        elif '_id' in item['id']:
                            last_id = item['id']['_id']
                    try:
                        hres = self.collection.bulk_write(bulk, ordered=False)
                    except BulkWriteError as bwe:
                        logging.error(bwe.details)
                        sys.exit()

                    self.total += self.ibuff
                    log = 'Committing {:7} ops. ({:8}) to {:15}:'.format(
                        self.ibuff, self.total, self.collection.name
                    )
                    log += '{:7} matched, {:7} removed, {:7} upserted, {:7} modified'.format(
                        hres.matched_count, hres.deleted_count, hres.upserted_count, hres.modified_count
                    )
                    if last_id:
                        log += " (Last processed Id: {})".format(last_id)
                    logging.info(log)
                    self.removed += hres.deleted_count
                    self.upserted += hres.upserted_count
                    self.modified += hres.modified_count
                    self.clean()

    def commit_data_if_full(self, many=False):
        self.commit_data(True, many)

    def commit_any_data(self, many=False):
        self.commit_data(False, many)


    def global_stats(self):
        log = "{:10} ({:6}) {:8} ops, ". format(
            self.collection.name,
            OP_LABELS[self.mode],
            self.total,
        )
        log += "{:7} removed, {:7} upserted, {:7} modified".format(
            self.removed, self.upserted, self.modified
        )
        return log
