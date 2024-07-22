#!/bin/bash
#SBATCH -o logs/check_version_complet.log
python checking/check_versions_completness.py -v --group insiders
