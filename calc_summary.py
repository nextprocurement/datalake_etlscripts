#!/usr/bin/env python
# coding: utf-8
''' Script to build summary data for API
'''
import sys
import argparse
import logging
import os
import time
from yaml import load, CLoader
from nextplib import ntp_entry as ntp
from mmb_data.mongo_db_connect import Mongo_db


def main():
    ''' Main '''

    parser = argparse.ArgumentParser(description='Summary')
    parser.add_argument('--config', action='store', default='secrets.yml', help='Configuration file (default;secrets.yml)')
    parser.add_argument('--group', action='store', help="outsiders|minors|insiders", required=True)
    parser.add_argument('-v', '--verbose', action='store_true', help='Extra progress information')
    parser.add_argument('--debug',action='store_true', help='Extra debug information')

    args = parser.parse_args()
    # Setup logging
    logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d|%H:%M:%S')
    if args.debug:
        logging.getLogger().setLevel(10)
    else:
        logging.getLogger().setLevel(20)

    # Config file
    with open(args.config, 'r')  as config_file:
        config = load(config_file, Loader=CLoader)

    logging.info(f"Connecting to MongoDB at {config['MONGODB_HOST']}")

    db_lnk = Mongo_db(
        config['MONGODB_HOST'],
        'nextprocurement',
        False,
        config['MONGODB_AUTH'],
        credentials=config['MONGODB_CREDENTIALS'],
        connect_db=True
    )
    if args.group in ['outsiders', 'insiders']:
        incoming_col = db_lnk.db.get_collection('place')
        data_store = 'place'
    else:
        incoming_col = db_lnk.db.get_collection('place_menores')
        data_store = 'place_menores'

    data = {}
    data['total_documents'] = incoming_col.estimated_document_count()
    aggregated_counts = [
        'Datos_Generales_del_Expediente/Tipo_Contrato',
        'Proceso_de_licitacion/Sistema_de_contratacion',
        'Proceso_de_licitacion/Idioma_de_Presentacion_de_Oferta',
        'Entidad_Adjudicadora/Tipo_de_Administracion',
        'Entidad_Adjudicadora/Pais',
        'Lugar_de_ejecucion/Pais'
    ]

    for field in aggregated_counts:
        data[field] = list(incoming_col.aggregate(
            [
                {
                    '$match':{
                        '_id':{'$ne':'summary_data'}
                    }
                },
                {
                    '$group': {
                        '_id':'$' + field, 'count':{'$sum':1}
                    }
                }
            ]
        ))
    print(data)
    incoming_col.replace_one({'_id': 'summary_data'}, data, upsert=True)
    logging.info(f"Updated {data_store} summary")

if __name__ == "__main__":
    main()