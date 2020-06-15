# Create Demo Entity to demonstrate anomaly detection with dimensional filters
# See https://github.com/ibm-watson-iot/functions/blob/development/iotfunctions/entity.py

from iotfunctions import metadata
from iotfunctions.metadata import EntityType
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.db import Database

class Equipment (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring a manufacturing line. Monitor comfort levels, energy
    consumption and occupany.
    '''


    def __init__(self,
                 name,
                 db,
                 db_schema=None,
                 description=None,
                 generate_days=0,
                 drop_existing=True,
                 ):

        # constants
        constants = []

        physical_name = name.lower()

        # granularities
        granularities = []

        # columns
        columns = []

        columns.append(Column('asset_id',String(50) ))
        columns.append(Column('drvr_rpm', Float() ))
        columns.append(Column('drvn_flow', Float() ))
        columns.append(Column('drvn_t1', Float() ))
        columns.append(Column('drvn_p1', Float() ))
        columns.append(Column('predict_drvn_t1', Float() ))
        columns.append(Column('predict_drvn_p1', Float() ))
        columns.append(Column('drvn_t2', Float() ))
        columns.append(Column('drvn_p2', Float() ))
        columns.append(Column('predict_drvn_t2', Float() ))
        columns.append(Column('predict_drvn_p2', Float() ))
        columns.append(Column('run_status', Integer() ))
        columns.append(Column('scheduled_maintenance', Integer() ))
        columns.append(Column('unscheduled_maintenance', Integer() ))
        columns.append(Column('compressor_in_x', Float() ))
        columns.append(Column('compressor_in_y', Float() ))
        columns.append(Column('compressor_out_x', Float() ))
        columns.append(Column('compressor_out_y', Float() ))
        columns.append(Column('run_status_x', Integer() ))
        columns.append(Column('run_status_y', Integer() ))
        columns.append(Column('maintenance_status_x', Integer() ))
        columns.append(Column('mainteancne_status_y', Integer() ))


        # dimension columns
        dimension_columns = []
        dimension_columns.append(Column('business', String(50)))
        dimension_columns.append(Column('site', String(50)))
        dimension_columns.append(Column('equipment_type', String(50)))
        dimension_columns.append(Column('train', String(50)))
        dimension_columns.append(Column('service', String(50)))
        dimension_columns.append(Column('asset_id', String(50)))

        # functions
        functions = []
        # simulation settings
        # uncomment this if you want to create entities automatically
        # then comment it out
        # then delete any unwanted dimensions using SQL
        #   DELETE FROM BLUADMIN.EQUIPMENT WHERE DEVICEID=73000;

        loop import csv

        sim = {
            'freq': '5min',
            'auto_entity_count' : 1,
            'data_item_mean': {'drvn_t1': 22,
                               'STEP': 1,
                               'drvn_p1': 50,
                               'asset_id': 1
                               },
            'data_item_domain': {
                #'dim_business' : ['Australia','Netherlands','USA' ],
                'dim_business' : ['Netherlands' ],
                #'dim_site' : ['FLNG Prelude','Pernis Refinery','Convent Refinery', 'FCCU', 'HTU3', 'HTU2','H-Oil','HCU' ],
                'dim_site' : ['HCU'],
                'dim_equipment_type': ['Train'],
                'dim_train_type': ['FGC-B','FGC-A','FGC-C ','P-45001A'],
                #'dim_service': ['Charge Pump','H2 Compressor','Hydrogen Makeup Compressor','Wet Gas Compressor', 'Fresh Feed Pump'],
                'dim_service': ['H2 Compressor'],
                #'dim_asset_id': ['2K-330','2K-331','2K-332','2K-333'],
                'dim_asset_id': ['016-IV-1011','016-IV-3011','016-IV-4011','016-IV-5011','016-IV-6011']
            },
            'drop_existing': False
        }

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        # data type for operator cannot be inferred automatically
        # state it explicitly

        output_items_extended_metadata = {}

        super().__init__(name=name,
                         db = db,
                         constants = constants,
                         granularities = granularities,
                         columns=columns,
                         functions = functions,
                         dimension_columns = dimension_columns,
                         output_items_extended_metadata = output_items_extended_metadata,
                         generate_days = generate_days,
                         drop_existing = drop_existing,
                         description = description,
db_schema = db_schema)
