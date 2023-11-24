#!/bin/bash
. ~/miniconda3/bin/activate nextp
python sync_documents.py -v -i gridfs: -o PLACE@swift:documentos
