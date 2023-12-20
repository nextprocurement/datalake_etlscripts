#!/bin/bash -x
#SBATCH -o logs/process_parquets_minors.log
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE_NEW
export EXEDIR=$BASEDIR/etlscripts
#Menores
for d in $DATADIR/minors/*renamed.parquet
do
	python $EXEDIR/read_parquet_2023.py -v --debug --group minors --config $EXEDIR/secrets_mdb.yml   $EXEDIR/data/columns_consolidated.tsv $d >> logs/update_dec23_minors.log
done
