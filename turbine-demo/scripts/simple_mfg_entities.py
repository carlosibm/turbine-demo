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

        columns.append(Column('TURBINE_ID',String(50) ))
        columns.append(Column('TEMPERATURE', Float() ))
        columns.append(Column('PRESSURE', Float() ))
        columns.append(Column('PREDICT_TEMPERATURE', Float() ))
        columns.append(Column('PREDICT_PRESSURE', Float() ))
        columns.append(Column('STEP', Float() ))
        columns.append(Column('A_TEMP_X', Float() ))
        columns.append(Column('A_TEMP_Y', Float() ))
        columns.append(Column('A_PRESS_X', Float() ))
        columns.append(Column('A_PRESS_Y', Float() ))
        columns.append(Column('B_TEMP_X', Float() ))
        columns.append(Column('B_TEMP_Y', Float() ))
        columns.append(Column('B_PRESS_X', Float() ))
        columns.append(Column('B_PRESS_Y', Float() ))
        columns.append(Column('TEMP_X', Float() ))
        columns.append(Column('TEMP_Y', Float() ))
        columns.append(Column('PRESS_X', Float() ))
        columns.append(Column('PREDICT_PRESS_X', Float() ))
        columns.append(Column('PRESS_Y', Float() ))
        # dimension columns
        dimension_columns = []
        dimension_columns.append(Column('CLIENT', String(50)))
        dimension_columns.append(Column('ORG', String(50)))
        dimension_columns.append(Column('FUNCTION', String(50)))

        # functions
        functions = []
        # simulation settings
        # uncomment this if you want to create entities automatically
        '''
        sim = {
            'freq': '5min',
            'auto_entity_count' : 1,
            'data_item_mean': {'TEMPERATURE': 22,
                               'STEP': 1,
                               'PRESSURE': 50,
                               'TURBINE_ID': 1
                               },
            'data_item_domain': {
                'CLIENT' : ['Riverside MFG','Collonade MFG','Mariners Way MFG' ],
                'ORG': ['Engineering','Supply Chain', 'Production', 'Quality', 'Other'],
                'FUNCTION': ['New Products','Packaging','Planning','Warehouse', 'Logistics', 'Customer Service','Line 1', 'Line 2', 'Quality Control', 'Calibration', 'Reliability']
            },
            'drop_existing': False
        }

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)
        '''
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
