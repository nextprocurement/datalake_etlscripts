''' Classes NtoStorage '''
import sys
import os.path
import logging
import re

from os.path import join as opj
from bson.regex import Regex
from gridfs.errors import CorruptGridFile
import swiftclient as sw

def is_in_range(ntp_id, id_range):
    ''' Check whether ntp_id is in id_range'''
    if id_range is None:
        return True
    if isinstance(id_range, str):
        return id_range == ntp_id
    id_min, id_max = id_range
    if id_min is None:
        return ntp_id <= id_max
    if id_max is None:
        return ntp_id >= id_min
    return id_min <= ntp_id <= id_max

def get_ntpid(file):
    ''' get ntpid from document file name '''
    if '_' not in file:
        return ''
    ntp_id, field = file.split('_', 1)
    return ntp_id

class NtpStorage:
    ''' Abstract class to manage alternative storages'''
    def __init__(self, type_store):
        self.type = type_store

    def get_ntpid(self, file):
        ntp_id, field = file.split('_', 1)
        return ntp_id

class NtpStorageDisk (NtpStorage):
    ''' Class to manage disk storage'''
    def __init__(self, type_store='disk', data_dir=''):
        super().__init__(type_store=type_store)
        self.data_dir = data_dir

    def file_store(self, file_name, contents):
        ''' Store contents as file_name '''
        with open(opj(self.data_dir, file_name), 'bw') as output_file:
            output_file.write(contents)

    def file_read(self, file_name):
        ''' Read file_name'''
        try:
            with open(opj(self.data_dir, file_name), 'br') as input_file:
                return input_file.read()
        except Exception as err:
            logging.debug(err)
            logging.error(f"Reading file {opj(self.data_dir, file_name)} failed")
        return ''

    def delete_file(self, file_name):
        ''' Delete file_name from storage '''
        try:
            os.remove({opj(self.data_dir, file_name)})
        except Exception as err:
            logging.debug(err)
            logging.error(f"Error deleting {opj(self.data_dir, file_name)}")

    def file_exists(self, file_name):
        ''' Check whether file_name exists'''
        return os.path.exists(opj(self.data_dir, file_name))

    def file_list(self, id_range=None, set_Debug=False):
        ''' Obtains list of file within id_range'''
        file_list = []
        for file in os.listdir(self.data_dir):
            if id_range is None or is_in_range(get_ntpid(file), id_range):
                file_list.append(file)
        return file_list

class NtpStorageGridFs (NtpStorage):
    '''Class to manage GridFS storage'''
    def __init__(self, type_store='gridfs', gridfs_obj=None):
        super().__init__(type_store=type_store)
        self.gridfs = gridfs_obj

    def file_store(self, file_name, contents):
        ''' Stores file_name on gridfs'''
        #removing previous version if exists
        if contents is not None:
            self.delete_file(file_name)
            self.gridfs.put(contents, filename=file_name)

    def file_read(self, file_name):
        ''' Retreives file_name from gridFS'''
        if self.file_exists(file_name):
            file_id = self.gridfs.find_one({'filename':file_name})._id
            try:
                return self.gridfs.get(file_id).read()
            except CorruptGridFile as err:
                logging.error(f"Error reading {file_name} {err}")
                return ''
        else:
            logging.error(f"File {file_name} not found")
        return ''

    def delete_file(self, file_name):
        ''' Delete file_name from gridFS'''
        if self.file_exists(file_name):
            file_id = self.gridfs.find_one({'filename':file_name})._id
            self.gridfs.delete(file_id)

    def file_exists(self, file_name, no_ext=False):
        ''' Check whether file_name exists on gridFS'''
        if no_ext:
            rgx = re.compile("^" + file_name)
            return self.gridfs.exists({'filename':rgx})
        else:
            return self.gridfs.exists(filename=file_name)

    def file_list(self, id_range=None, set_debug=False):
        ''' Obtains list of files in id_range'''
        list = []
        for file in self.gridfs.find():
            if id_range is None or is_in_range(get_ntpid(file.name), id_range):
                list.append(file.name)
        return list

    def file_list_per_doc(self, files_col, ntp_id):
        filename_pattern = re.compile (f"^{ntp_id}")
        filename_rex = Regex.from_native(filename_pattern)
        filename_rex.flags ^= re.UNICODE
        file_list = []
        for file in files_col.find({"filename": filename_rex}, projection=['_id', 'filename']):
            file_list.append(file)
        return file_list

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
            logging.debug(e)
            if e.http_status == 404:
                return False
            logging.error("Error connecting swift storage")
            sys.exit()

    def get_folder(self, tmp_dir='/tmp', remote_prefix=None):
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
            status = self.get_file(file['name'], tmp_dir)
            if status:
                ok += 1
            else:
                ko += 1
        logging.debug(f"{ok} files downloaded and {ko} failed, using {tmp_dir}")
        return tmp_dir

    def file_read(self, file_name):
        try:
            headers, data = self.connection.get_object(
                self.container,
                opj(self.data_prefix, file_name)
            )
            return data
        except Exception as e:
            logging.debug(e)
            logging.error(f"download of {file_name} failed")
        return 0

    def download_file(self, file_name, tmp_dir='/tmp'):
        ok = False
        try:
            data = self.file_read(self, file_name)
            with open(opj(tmp_dir, os.path.basename(file_name)), "bw") as output_file:
                output_file.write(data)
            ok = True
        except Exception as e:
            logging.debug(e)
            logging.error(f"download of {file_name} failed")
        return ok

    def delete_file(self, file_name):
        try:
            headers, data = self.connection.delete_object(
                self.container,
                opj(self.data_prefix, file_name)
            )
        except Exception as e:
            logging.debug(e)
            logging.error(f"deletion of {file_name} failed")
        return 0

    def file_list(self, id_range=None, set_debug=False):
        logging.getLogger().setLevel(20)
        head, files = self.connection.get_container(
            self.container,
            full_listing=True
        )
        if set_debug:
            logging.getLogger().setLevel(10)
        list = []
        for file in files:
            if not file['name'].startswith(self.data_prefix):
                continue
            if '_' in file['name'] and (id_range is None or is_in_range(get_ntpid(os.path.basename(file['name'])), id_range)):
                list.append(os.path.basename(file['name']))
        return list
