# Create Demo Entity to demonstrate anomaly detection with dimensional filters
# See https://github.com/ibm-watson-iot/functions/blob/development/iotfunctions/entity.py

from iotfunctions import metadata
from iotfunctions.metadata import EntityType
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions import bif
from iotfunctions.db import Database

class Equipment (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring a equipment.
    '''


    def __init__(self,
                 name,
                 db,
                 db_schema=None,
                 description=None,
                 generate_days=10,
                 drop_existing=False):

        # constants
        constants = []

        physical_name = name.lower()

        # granularities
        granularities = []

        # columns
        columns = []

        columns.append(Column('TURBINE_ID',String(50) ))
        columns.append(Column('drvn_t1', Float() ))
        columns.append(Column('drvn_p1', Float() ))
        columns.append(Column('STEP', Float() ))

        # dimension columns
        dimension_columns = []
        dimension_columns.append(Column('dim_business', String(50)))
        dimension_columns.append(Column('dim_site', String(50)))
        dimension_columns.append(Column('dim_equipment_type', String(50)))

        # functions
        functions = []
        # simulation settings
        sim = {
            'freq': '5min',
            'auto_entity_count' : 10,
            'data_item_mean': {'drvn_t1': 22,
                               'STEP': 1,
                               'drvn_p1': 50,
                               'TURBINE_ID': 1
                               },
            'data_item_domain': {
                'SITE' : ['Riverside MFG','Collonade MFG','Mariners Way MFG' ],
                'dim_site': ['Engineering','Supply Chain', 'Production', 'Quality', 'Other'],
                'dim_equipment_type': ['New Products','Packaging','Planning','Warehouse', 'Logistics', 'Customer Service','Line 1', 'Line 2', 'Quality Control', 'Calibration', 'Reliability']
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
