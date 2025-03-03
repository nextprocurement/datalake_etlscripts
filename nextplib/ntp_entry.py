''' Classes NtpEntry '''
import sys
import copy
import logging
from urllib.parse import unquote
from http import HTTPStatus
import requests
from nextplib import ntp_constants as cts, ntp_utils as nu

class NtpEntry:
    '''Class to manage ntp documents'''
    def __init__(self, ntp_id=None, place_id=None):
        self.ntp_order = 0
        self.ntp_id = ntp_id
        self.patch_data = {}
        self.data = {
            '_id': ntp_id,
            'id': place_id
        }
        if ntp_id is not None:
            self.order_from_id()

    def load_data(self, ntp_order, data):
        ''' Load data dictionary into instance'''
        self.ntp_order = ntp_order
        self.set_ntp_id()
        self.data = copy.deepcopy(data)
        self.data['_id'] = self.ntp_id

    def merge_data(self, new_data):
        '''Merge new data into instance'''
        patch_data={'add': {}, 'mod': {}}
        if 'patch_data' not in self.data:
            self.data['patch_data'] = {}
        for k in new_data:
            if k in self.data:
                if new_data[k] and self.data[k] != new_data[k]:
                    logging.debug(f"Modifying {k} {self.data[k]} -> {new_data[k]}")
                    self.data[k] = new_data[k]
                    patch_data['mod'][k] = new_data[k]

            else:
                self.data[k] = new_data[k]
                patch_data['add'][k] = new_data[k]
                logging.debug(f"Adding {k} {new_data[k]}")
        self.patch_data[self.ntp_id] = patch_data

    def set_ntp_id(self):
        '''Set ntp_id from ntp_order'''
        self.ntp_id = 'ntp{:s}'.format(str(self.ntp_order).zfill(8))

    def order_from_id(self):
        '''Set ntp_order from ntp_id'''
        self.ntp_order = nu.parse_ntp_id(self.ntp_id)

    def is_obsolete(self):
        '''Check whether is a obsolete document'''
        return 'obsolete_version' in self.data and self.data['obsolete_version']

    def make_obsolete(self, update_id):
        '''Mark adocument as obsolete'''
        new_data = {
            '_id': self.ntp_id,
            'id': self.data['id'],
            'obsolete_version': True,
            'updated_to': update_id
        }
        self.data = new_data
    
    def add_unique_code(self):
        # Use the first 50 characters of 'title' if it exists
        if 'title' in self.data and self.data['title']:
            inicio_title = self.data['title'][:50]
        else:
            # Generate a unique random string combined with a UUID
            random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=30))
            unique_id = str(uuid.uuid4())[:20]  # First 20 chars of UUID
            inicio_title = f"{random_part}_{unique_id}"  # Ensures uniqueness

        contract_folder_id = self.data.get('Datos_Generales_del_Expediente/Numero_de_Expediente', 'UNKNOWN')
        country_subentity_code = self.data.get('Lugar_de_ejecucion/Codigo_de_Subentidad_Territorial', 'UNKNOWN')

        nom = self.data.get('Entidad_Adjudicadora/Nombre', '')
        nom = nom[:50] if nom else ""
        self.data['codigo_unico'] = f"{inicio_title}&{contract_folder_id}&{country_subentity_code}&{nom}"
        
        return self.data['codigo_unico']

    def commit_to_db(self, col, update=False):
        '''Commit document to db'''
        if update:
            old_doc = nu.find_previous_doc(self.data, col)
            if old_doc and old_doc['_id']:
                logging.info(f"Updating previous version {old_doc['_id']}")
                self.data['_id'] = old_doc['_id']
                self.ntp_id = old_doc['_id']
                self.order_from_id()
        try:
            col.replace_one(
                {'_id': self.data['_id'], 'id': self.data['id']},
                 self.data,
                 upsert=True
            )
        except Exception as e:
            logging.debug(self.data)
            for k in self.data:
                logging.debug(f"{k} {self.data[k]} {type(self.data[k])}")
            logging.error(e)
            #sys.exit(1)

        return self.ntp_order

    def load_from_db(self, col_id,  ntp_id, follow_version=False):
        ''' Load data from db'''
        try:
            self.data = col_id.find_one({'_id': ntp_id})
            if not self.data:
                self.data = {}
                return False
            self.ntp_id = ntp_id
            self.ntp_order = nu.parse_ntp_id(ntp_id)
            if follow_version and self.is_obsolete():
                self.load_from_db(col_id, self.data['updated_to'], follow_version=follow_version)
        except Exception as e:
            logging.error(e)
            return False
        return True

    def extract_urls(self):
        '''Extract existing URLs from document'''
        urls = {}
        for k in self.data:
            if isinstance(self.data[k], str) and self.data[k].startswith('http'):
                urls[k] = self.data[k]
            if isinstance(self.data[k], list):
                for index, url in enumerate(self.data[k]):
                    if isinstance(url, str) and url.startswith('http'):
                        urls[f"{k}:{index}"] = url
        return urls


    def store_document(
            self,
            field,
            filename,
            storage=None,
            replace=False,
            scan_only=False,
            allow_redirects=False,
            verify_ca=True,
            skip_early=False,
            check_md5=True,
            dry_run=False
    ):
        ''' Retrieves and stores document accounting for possible redirections'''
        if ':' in field:
            base, index = field.split(':')
            url = unquote(self.data[base][int(index)]).replace(' ', '%20').replace('+', '')
        else:
            base = field
            url = unquote(self.data[field]).replace(' ', '%20').replace('+', '')

        file_id = f"PL{format(int(self.data['id'].split('/')[-1]), '08d')}"

        logging.debug(f"Assigned File ID {file_id}")

        if skip_early:
            if storage.type != 'gridfs':
                logging.error(f"--skip_early only available for GridFS storage  (yet)")
                sys.exit(1)
            #file_name_root = nu.get_file_name(self.ntp_id, filename, '')
            file_name_root = nu.get_file_name(file_id, filename,'')
            if storage.file_exists(file_name_root, no_ext=True):
                return cts.SKIPPED, field
        try:
            logging.debug(f"IP: {','.join(nu.get_ips(url))}")
            response = requests.get(
                url,
                timeout=cts.TIMEOUT,
                allow_redirects=allow_redirects,
                verify=verify_ca
            )
            logging.debug(response.headers)
            num_redirects = 0
            while response.status_code in cts.REDIRECT_CODES and num_redirects <= cts.MAX_REDIRECTS:
                num_redirects +=1
                url = response.headers['Location']
                logging.warning(f"Found {response.status_code}: Redirecting to {url}")
                logging.debug(f"IP: {','.join(nu.get_ips(url))}")
                response = requests.get(
                    url, timeout=cts.TIMEOUT,
                    verify=verify_ca
                )
            if num_redirects > cts.MAX_REDIRECTS:
                logging.warning(f"Max. Redirects {cts.MAX_REDIRECTS} achieved, skipping")

            if response.status_code == 200:
                doc_type = nu.get_file_type(response.headers)

                if doc_type:
                    logging.debug(f"DOC_TYPE {doc_type}")
                else:
                    logging.debug(f"EMPTY DOC TYPE at {self.ntp_id}")

                if doc_type == 'html':
                    redir_url = nu.check_meta_refresh(url, response.content)
                    if redir_url:
                        logging.debug(f"IP: {','.join(nu.get_ips(url))}")
                        response = requests.get(
                            redir_url,
                            timeout=cts.TIMEOUT,
                            allow_redirects=allow_redirects,
                            verify=verify_ca
                        )
                        logging.debug(response.headers)
                        if response.status_code == 200:
                            doc_type = nu.get_file_type(response.headers)
                            logging.debug(f"New doc type {doc_type}")
                            url = redir_url
                        else:
                            return response.status_code, 'Error on redirect'

                file_name = nu.get_file_name(file_id, filename, doc_type)
                logging.info(f"About to process {file_name}")
                if doc_type in cts.ACCEPTED_DOC_TYPES:
                    if storage.file_exists(file_name) and not replace:
                        return cts.SKIPPED_FILE, doc_type
                    if check_md5:
                        md5 = nu.get_md5(response.content)
                        if storage.file_md5_exists(md5) and not storage.file_exists(file_name):
                            return cts.SKIPPED_MD5, doc_type
                    if not dry_run and not scan_only:
                        storage.file_store(file_name, response.content)
                        return cts.STORE_OK, file_name
                    if scan_only:
                        return cts.SKIPPED_SCAN, doc_type
                    if dry_run:
                        return cts.SKIPPED_DRY, doc_type
                return cts.UNWANTED_TYPE, doc_type

            logging.error(f"{HTTPStatus(response.status_code).phrase}: {url}")
            return response.status_code, HTTPStatus(response.status_code).phrase
        except requests.exceptions.SSLError as err:
            logging.error(err)
            return cts.SSL_ERROR, err
        except requests.exceptions.ReadTimeout:
            logging.error(f"TimeOut: {url}")
            return cts.ERROR, 'Timeout'
        except Exception as err:
            logging.error(err)
        return cts.ERROR, 'unknown'


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
