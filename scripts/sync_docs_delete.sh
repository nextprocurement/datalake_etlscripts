#!/bin/bash
#SBATCH -o logs/sync_docs_delete.log
python sync_documents.py -v --debug -i gridfs: -o PLACE@swift:documentos --delete
