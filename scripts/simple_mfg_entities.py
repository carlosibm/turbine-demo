# Create Demo Entity to demonstrate anomaly detection with dimensional filters
# See https://github.com/ibm-watson-iot/functions/blob/development/iotfunctions/entity.py

from iotfunctions import metadata
from iotfunctions.metadata import EntityType
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
import sys
import csv
import pandas as pd
import numpy as np
from iotfunctions import bif
from iotfunctions.db import Database

class Turbines (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring Equipment.
    '''


    def __init__(self,
                 name,
                 db,
                 db_schema=None,
                 description=None,
                 generate_days=0,
                 drop_existing=True,
                 ):
        if (len(sys.argv) > 0):
            entity_type_name = sys.argv[1]
            input_file = sys.argv[2]
            print("entity_type_name %s" % entity_type_name)
            print("input_file %s" % input_file)
        else:
            print("Please provide path to csv file as script argument")
            exit()

        # Initialize Entity Type class variables
        self.db_schema = db_schema
        self.db = db

        rows = []
        print("Open File")
        with open(input_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            point_dimension_values = {
                "label": "",
                "units": "",
                "metric_name": ""
            }
            metrics = []

            for row in csv_reader:
                if line_count == 0:
                    print("Column names are %s" % {", ".join(row)})
                    line_count += 1
                else:
                    try:
                        metric_name = row["Point"].replace(' ', '_')
                        # name = row["Point"].replace(' ', '_')
                        print("Name %s" % metric_name)
                        type = row["DataType"]
                        print("Type %s" % type)
                        if metric_name == "":
                            break # No more rows

                        # Pull Name and Type from headers
                        # print("row Point_name %s " %row["Point"] )

                        # Create metric
                        if row["Point_Data_Type"] == "S":
                            print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                            #metric_name = row["Point"].split('|| ')[1].replace(' ', '_')
                            print("________________________ Point metric name  %s " %metric_name)
                            print("________________________ Point metric type  %s " %type)
                            metric_to_add = {'metric_name': metric_name, 'type': type}
                            metrics.append(metric_to_add)

                        # Create Constant
                        if row["Point_Data_Type"] == "C":
                            print("________________________ Point value  %s " % row["Value"])
                            print(
                                "________________________ Point point_data_type  %s " % row["Point_Data_Type"].replace(' ',
                                                                                                                       '_'))
                            print("________________________ Point db function name  %s " % row["Function"])

                        # Create Function
                        if row["Point_Data_Type"] == "F":
                            print("________________________ Point point_data_type  %s " % row["Point_Data_Type"])
                            print("________________________ Point db data_type  %s " % row["DataType"])
                            print("________________________ Point db function name  %s " % row["Function"])
                    except:
                        print(sys.exc_info()[0])  # the exception instance
                        break

        # Load Data via csv into rows
        # DF insert using pandas


        # constants
        constants = []

        physical_name = entity_type_name.lower()

        # granularities
        granularities = []

        # columns
        columns = []

        '''
        columns.append(Column('asset_id',String(50) ))
        columns.append(Column('drvr_rpm', Float() ))
        '''
        for metric in metrics:
            print("Adding metric name to entity type %s" %metric['metric_name'] )
            print("Adding metric type to entity type %s" %metric['type'] )
            unallowed_chars = "!@#$()"
            for char in unallowed_chars:
                metric['metric_name'] = metric['metric_name'].replace(char, "")
            print("Adding cleansed metric name to entity type %s" % metric['metric_name'])
            columns.append(Column(metric['metric_name'], Float()))
        #columns.append(Column('asset_id', String(50)))


        # dimension columns
        dimension_columns = []
        #for dim in dims_found:
        #    dimension_columns.append(Column('business', String(50)))

        # functions
        functions = []
        #for fun in functions_found:
        #    functions.append(bif.PythonExpression(expression='df["input_flow_rate"] * df["discharge_flow_rate"]',
        #                                          output_name='output_flow_rate'))

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
            'drop_existing': True
        }

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        # data type for operator cannot be inferred automatically
        # state it explicitly

        output_items_extended_metadata = {}

        super().__init__(name=entity_type_name,
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

    def read_meter_data(self, input_file=None):
        # Check to make sure table was created
        entity_type_name = "Equipment"
        print("DB Name %s " % entity_type_name)
        print("DB Schema %s " % self.db_schema)
        df = self.db.read_table(table_name=entity_type_name, schema=self.db_schema)
        print(df.head())
        df.to_csv('/Users/carlos.ferreira1ibm.com/ws/shell/data/Equipment.csv')
        # df_to_import = pd.read_csv('/Users/carlos.ferreira1ibm.com/ws/shell/data/Equipment.csv')


class Equipment (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring Equipment.
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
            'drop_existing': True
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
