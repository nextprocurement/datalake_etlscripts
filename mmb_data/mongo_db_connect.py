# package mongo_db_connect;
#
## Based on generic access auth, TODO adapt to per database auth
## Defaults to mmb

import sys
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from gridfs import GridFS

class Mongo_db():
    def __init__(self, host, db, read_only, auth=True, wconcern=1, credentials = None):
        self.credentials = credentials
        self.authDB = 'admin'
        self.host = host
        self.db = db
        self.read_only = read_only
        self.auth = auth
        self.wconcern = wconcern
        self.uri = self._set_uri()

        self.connected = False

    def set_auth(self, user, passw, auth_db):
        if self.read_only:
            self.credentials['ROUser'] = user
            self.credentials['ROPwd'] = passw
        else:
            self.credentials['RWUser'] = user
            self.credentials['RWPwd'] = passw
        self.authDB = auth_db

    def _set_uri(self):
        self.uri = 'mongodb://'
        if self.auth:
            if self.read_only:
                self.uri += '{}:{}@{}/{}'.format(
                    self.credentials['ROUser'],
                    self.credentials['ROPwd'],
                    self.host,
                    self.authDB
                )
            else:
                self.uri += '{}:{}@{}/{}'.format(
                    self.credentials['RWUser'],
                    self.credentials['RWPwd'],
                    self.host,
                    self.authDB
                )
        else:
            self.uri += self.host
#        print(self.uri)

    def connect_db(self):
        self._db_connect()

    def get_collections(self, cols):
        if not self.connected:
            self._db_connect()
        dbs = {}
        for c in cols:
            dbs[c] = self.db.get_collection(c)
        return dbs

    def get_gfs(self, col_name='fs'):
        return GridFS(self.db, col_name, disable_md5=True)

    def _db_connect(self):
        if not self.uri:
            self._set_uri()
        try:
            self.client = MongoClient(
                self.uri,
                connectTimeoutMS=500000
            )
            self.db = self.client.get_database(self.db)
            self.connected = True
        except ConnectionFailure:
            sys.exit("Error connecting DB")
        # test connection
        try:
            self.client.server_info()
        except Exception as e:
            logging.error("No response from MongoDB server")
            sys.exit()
        

    def close(self):
        if self.connected:
            self.client.close()
            self.connected = False
