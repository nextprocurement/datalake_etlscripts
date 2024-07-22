#!/bin/bash
#SBATCH -o logs/sync_docs.log
python sync_documents.py -v -i gridfs: -o PLACE@swift:documentos
