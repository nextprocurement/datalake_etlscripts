#!/bin/bash -x
#SBATCH -o logs/download_PLACE.log
export DATADIR=/data/incoming/PLACE
export EXEDIR=$BASEDIR/etlscripts
export BASEURL=https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion
export DICC=insiders_minors.yaml

#Agregados
export FILE_PREFIX=PlataformasAgregadasSinMenores
export URL=${BASEURL}_1044/${FILE_PREFIX}

cd $DATADIR/outsiders
wget -nv -N --no-check-certificate ${URL}_$1.zip
sproc_read_single_zip.py ${FILE_PREFIX}_$1.zip ${FILE_PREFIX}_$1.zip.parquet
sproc_rename_cols.py -l $DICC ${FILE_PREFIX}_$1.zip.parquet

#Menores
export FILE_PREFIX=contratosMenoresPerfilesContratantes
export URL=${BASEURL}_1143/${FILE_PREFIX}

cd $DATADIR/minors
wget -nv -N --no-check-certificate ${URL}_$1.zip
sproc_read_single_zip.py ${FILE_PREFIX}_$1.zip ${FILE_PREFIX}_$1.zip.parquet
sproc_rename_cols.py -l $DICC ${FILE_PREFIX}_$1.zip.parquet

#perfiles
export FILE_PREFIX=licitacionesPerfilesContratanteCompleto3
export URL=${BASEURL}_643/${FILE_PREFIX}

cd $DATADIR/insiders
wget -nv -N --no-check-certificate ${URL}_$1.zip
sproc_read_single_zip.py ${FILE_PREFIX}_$1.zip ${FILE_PREFIX}_$1.zip.parquet
sproc_rename_cols.py -l $DICC ${FILE_PREFIX}_$1.zip.parquet
