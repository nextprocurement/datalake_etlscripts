#!/bin/bash
#SBATCH -o logs/purge_documents.log
python purge_documents.py -v --group insiders
