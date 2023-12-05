#!/bin/bash
#SBATCH -o logs/get_docs_insiders_%A.log
python get_documents.py -v --where gridfs --allow_redirects --skip_bad_servers --group insiders 
