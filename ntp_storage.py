''' Classes NtoStorage '''
import sys
import os.path
import logging
from os.path import join as opj
import swiftclient as sw

class NtpStorage:
    ''' Abstract class to manage alternative storages'''
    def __init__(self, type_store):
        self.type = type_store

class NtpStorageDisk (NtpStorage):
    ''' Class to manage disk storage'''
    def __init__(self, type_store='disk', data_dir=''):
        super().__init__(type_store=type_store)
        self.data_dir = data_dir

    def file_store(self, file_name, contents):

        with open(opj(self.data_dir, file_name), 'bw') as output_file:
            output_file.write(contents)

    def file_exists(self, file_name):
        return os.path.exists(opj(self.data_dir, file_name))

class NtpStorageGridFs (NtpStorage):
    '''Class to manage GridFS storage'''
    def __init__(self, type_store='gridfs', gridfs_obj=None):
        super().__init__(type_store=type_store)
        self.gridfs = gridfs_obj

    def file_store(self, file_name, contents):
        #removing previous version if exists
        if self.file_exists(file_name):
            file_id = self.gridfs.find_one({'filename':file_name})._id
            self.gridfs.delete(file_id)
        self.gridfs.put(contents, filename=file_name)

    def file_exists(self, file_name):
        return self.gridfs.exists(filename=file_name)

class NtpStorageSwift (NtpStorage):
    '''Class to manage Swift storage'''
    def __init__(self, type_store='swift', **kwargs):
        super().__init__(type_store=type_store)
        self.connection = kwargs['swift_connection']
        self.container = kwargs['swift_container']
        self.data_prefix = kwargs['swift_prefix']

    def file_store(self, file_name, contents):
        self.connection.put_object(
            self.container,
            opj(self.data_prefix, file_name),
            contents=contents
        )

    def file_exists(self, file_name):
        try:
            resp_headers = self.connection.head_object(
                self.container,
                opj(self.data_prefix, file_name)
            )
            return True
        except sw.ClientException as e:
            logging.debug(e.http_status)
            if e.http_status == 404:
                return False
            logging.error("Error connecting swift storage")
            sys.exit()

    def get_folder(self, tmp_dir='/tmp/spark_data', remote_prefix=None):
        head, files = self.connection.get_container(
            self.container,
            prefix=remote_prefix,
            full_listing=True
            )
        ok = 0
        ko = 0
        logging.debug(f"{len(files)} found from {remote_prefix}")
        if not os.path.isdir(tmp_dir):
            logging.debug(f"Creating {tmp_dir}")
            os.mkdir(tmp_dir)
        for file in files:
            try:
                headers, data = self.connection.get_object(
                    self.container,
                    file['name']
                )
                with open(opj(tmp_dir, os.path.basename(file['name'])), "bw") as output_file:
                    output_file.write(data)
                ok += 1
            except Exception as e:
                logging.debug(e)
                logging.error(f"download of {file} failed")
                ko += 1
        logging.debug(f"{ok} files downloaded and {ko} failed, using {tmp_dir}")
        return tmp_dir
