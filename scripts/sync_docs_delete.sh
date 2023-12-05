#!/bin/bash
#SBATCH -o logs/sync_docs_delete_%A.log
python sync_documents.py -v -i gridfs: -o PLACE@swift:documentos --delete
