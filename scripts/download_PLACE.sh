#!/bin/bash -x
#SBATCH -o logs/download_PLACE.log
export BASEDIR=/home/ubuntu/ETL
export DATADIR=/data/incoming/PLACE_NEW
export EXEDIR=$BASEDIR/etlscripts
#Agregados
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_
cd $DATADIR/outsiders
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
sproc_read_single_zip.py PlataformasAgregadasSinMenores_$1.zip PlaformasAgregadasSinMenores_$1.zip.parquet
#Menores
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/contratosMenoresPerfilesContratantes_
cd $DATADIR/minors
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
#perfiles
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_
cd $DATADIR/insiders
wget -nv -N --no-check-certificate ${BASEURL}$1.zip
