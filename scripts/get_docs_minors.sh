#!/bin/bash
#SBATCH -o logs/get_docs_minors.log
python get_documents.py -v --where gridfs --allow_redirects --skip_bad_servers --group minors 
