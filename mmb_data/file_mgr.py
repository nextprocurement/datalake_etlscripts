""" Manager for sequential text files """
import logging
import gzip
import os
import re


class FileMgr():
    """ Utility class to manage text files to be read sequentially """
    def __init__(self, file, ini_line=0, fin_line=0):
        self.fn = file
        file_stat = os.stat(self.fn)
        self.tstamp = int(file_stat.st_ctime)
        self.ini = ini_line
        self.fin = fin_line
        self.current_line = 0
        
    def check_new_stamp(self, tstamp_col):
        stored_tstamp = tstamp_col.find_one({'_id':self.fn})
        logging.info('File time stamp:   {:11.0f}'.format(self.tstamp))
        if stored_tstamp:
            logging.info('Stored time stamp: {:11.0f}'.format(stored_tstamp['ts']))
            if self.tstamp <= stored_tstamp['ts']:
                return False
        if not stored_tstamp:
            logging.info('Stored time stamp: None')
        return True
    
    def skip_lines_to(self, txt, match=False):
        header_lines = True
        for line in self:
        #    print(line)
            if match:
                header_lines = header_lines and line != txt
            else:
                header_lines = header_lines and not re.search(txt, line)
            if not header_lines:
                break

    def skip_lines_to_ini(self):
        if self.ini:
            for line in self:
                if self.current_line >= self.ini:
                    break
                    
    def skip_n_lines(self,n):
        nlin=0
        for line in self:
            if nlin == n:
                break
            nlin += 1
    
    def open_file(self):
        try:
            if self.fn.find('.gz') != -1:
                self.fh_in = gzip.open(self.fn, 'rt')
            else:
                self.fh_in = open(self.fn, 'r') 
        except IOError as e:
            sys.exit(e.message)
        

    def close_file(self):
        self.fh_in.close()

        
    def __next__(self):
        self.current_line += 1
        if self.fin and self.current_line > self.fin:
            raise StopIteration

        line = self.fh_in.__next__()
        if not isinstance(line, str):
            line = line.decode('ascii')
        return line.rstrip()

    def __iter__(self):
        return self
    
