import logging

def print_progress(prefix, nids, ntot, inc):
    ntot = max(1, ntot)
    if nids%inc == 0:
        logging.info("{} {:8}/{:8} {:5.1f}%".format(prefix, nids, ntot, (nids*100./ntot)))


def get_id(fasta_header):
    ids, desc = fasta_header.split(' ', 1)
    if ids.find('|'):
        db,uniq_id,entry_name = ids.split('|', 2)
        return uniq_id
    return ''