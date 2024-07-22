#!/bin/bash
#SBATCH -o logs/purge_documents_minors.log
python purge_documents.py -v --group minors
