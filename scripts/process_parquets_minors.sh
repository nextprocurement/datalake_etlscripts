#!/bin/bash -x
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE_NEW
export EXEDIR=$BASEDIR/etlscripts
#Menores
for d in $DATADIR/minors/*renamed.parquet
do 
	python $EXEDIR/read_parquet.py -v --debug --group minors --config $EXEDIR/secrets_mdb.yml --update $EXEDIR/columns_consolidated.tsv $d >> logs/update_dec23_minors.log
done
