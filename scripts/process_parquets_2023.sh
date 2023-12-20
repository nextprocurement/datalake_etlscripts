#!/bin/bash -x
#SBATCH -o logs/process_parquet_2023.log
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE_NEW
export EXEDIR=$BASEDIR/etlscripts
#Agregados
for d in $DATADIR/outsiders/*renamed.parquet
do
	python $EXEDIR/read_parquet_2023.py -v --debug --group outsiders --config $EXEDIR/secrets_mdb.yml $EXEDIR/data/columns_consolidated.tsv $d >> logs/update_dec23_outsiders.log
done
#insiders
for d in $DATADIR/insiders/*renamed.parquet
do
	python $EXEDIR/read_parquet_2023.py -v --debug --group insiders --config $EXEDIR/secrets_mdb.yml $EXEDIR/data/columns_consolidated.tsv $d >> logs/update_dec23_insiders.log
done
