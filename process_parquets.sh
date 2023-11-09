#!/bin/bash -x
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE_NEW
export EXEDIR=$BASEDIR/etlscripts
#Agregados
#for d in $DATADIR/outsiders/*renamed.parquet
#do 
#	python $EXEDIR/read_parquet.py -v --debug --group outsiders --config $EXEDIR/secrets_mdb.yml --upsert $EXEDIR/columns_consolidated.tsv $d > logs/update_nov23_outsiders.log
#done
#Menores
for d in $DATADIR/minors/*renamed.parquet
do 
	python $EXEDIR/read_parquet.py -v --debug --group minors --config $EXEDIR/secrets_mdb.yml --upsert $EXEDIR/columns_consolidated.tsv $d > logs/update_nov23_minors.log
done
#insiders
#for d in $DATADIR/insiders/*renamed.parquet
#do 
#	python $EXEDIR/read_parquet.py -v  --debug --group insiders --config $EXEDIR/secrets_mdb.yml --upsert $EXEDIR/columns_consolidated.tsv $d > logs/update_nov23_insiders.log
#done
