#!/bin/bash -x
export BASEDIR=/home/ubuntu/ETL
export DATADIR=$BASEDIR/incoming/PLACE
export DLSPROC=$BASEDIR/dlsproc
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_
source $BASEDIR/app-cred-External-openrc.sh
cd $DATADIR/agregados
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
rclone -v copy $DATADIR/agregados/PlataformasAgregadasSinMenores_$1.zip nextp:PLACE/agregados
dlsproc_process_zip.py $DATADIR/agregados/PlataformasAgregadasSinMenores_$1.zip $DATADIR/data/$1.parquet
rclone -v copy $DATADIR/data/${1}.parquet nextp:PLACE/data
dlsproc_rename_cols.py $DATADIR/data/$1.parquet $DLSPROC/samples/PLACE.yaml $DATADIR/data/${1}_flat.parquet
rclone -v copy $DATADIR/data/${1}_flat.parquet nextp:PLACE/data

