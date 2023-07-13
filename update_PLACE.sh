#!/bin/bash -x
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE
export DLSPROC=$BASEDIR/dlsproc
source $BASEDIR/app-cred-External-openrc.sh
#Agregados
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_
cd $DATADIR/agregados
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
rclone -v copy $DATADIR/agregados/PlataformasAgregadasSinMenores_$1.zip nextp:PLACE/agregados
dlsproc_process_zip.py $DATADIR/agregados/PlataformasAgregadasSinMenores_$1.zip $DATADIR/agregados_data/$1.parquet
rclone -v copy $DATADIR/agregados_data/${1}.parquet nextp:PLACE/agregados_data
dlsproc_rename_cols.py $DATADIR/agregados_data/$1.parquet $DLSPROC/samples/PLACE.yaml $DATADIR/agregados_data/${1}_flat.parquet
rclone -v copy $DATADIR/agregados_data/${1}_flat.parquet nextp:PLACE/agregados_data
#Menores
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/contratosMenoresPerfilesContratantes_
cd $DATADIR/menores
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
rclone -v copy $DATADIR/menores/contratosMenoresPerfilesContratantes_$1.zip nextp:PLACE/menores
dlsproc_process_zip.py $DATADIR/menores/contratosMenoresPerfilesContratantes_$1.zip $DATADIR/menores_data/$1.parquet
rclone -v copy $DATADIR/menores_data/${1}.parquet nextp:PLACE/menores_data
dlsproc_rename_cols.py $DATADIR/menores_data/$1.parquet $DLSPROC/samples/PLACE.yaml $DATADIR/menores_data/${1}_flat.parquet
rclone -v copy $DATADIR/menores_data/${1}_flat.parquet nextp:PLACE/menores_data
#perfiles
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_
cd $DATADIR/perfiles
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
rclone -v copy $DATADIR/perfiles/licitacionesPerfilesContratanteCompleto3_$1.zip nextp:PLACE/perfiles
dlsproc_process_zip.py $DATADIR/perfiles/licitacionesPerfilesContratanteCompleto3_$1.zip $DATADIR/perfiles_data/$1.parquet
rclone -v copy $DATADIR/perfiles_data/${1}.parquet nextp:PLACE/perfiles_data
dlsproc_rename_cols.py $DATADIR/perfiles_data/$1.parquet $DLSPROC/samples/PLACE.yaml $DATADIR/perfiles_data/${1}_flat.parquet
rclone -v copy $DATADIR/perfiles_data/${1}_flat.parquet nextp:PLACE/perfiles_data

