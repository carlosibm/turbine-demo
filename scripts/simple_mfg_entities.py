# Create Demo Entity to demonstrate anomaly detection with dimensional filters
# See https://github.com/ibm-watson-iot/functions/blob/development/iotfunctions/entity.py

from iotfunctions import metadata
from iotfunctions.metadata import EntityType
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func, SmallInteger
import sys
import csv
import pandas as pd
import numpy as np
from iotfunctions import bif
from iotfunctions.base import BaseDataSource
from iotfunctions.db import Database
import logging
from iotfunctions.enginelog import EngineLogging
EngineLogging.configure_console_logging(logging.DEBUG)
logger = logging.getLogger(__name__)
import datetime as dt
import re
from datetime import datetime


# BaseCustomEntityType
class MergeSampleTimeSeries(BaseDataSource):
    """
    Merge the contents of a table containing time series data with entity source data
    """
    merge_method = 'outer'  # or outer, concat, nearest
    # use concat when the source time series contains the same metrics as the entity type source data
    # use nearest to align the source time series to the entity source data
    # use outer to add new timestamps and metrics from the source
    merge_nearest_tolerance = pd.Timedelta('1D')
    merge_nearest_direction = 'nearest'
    source_table_name = 'sample_time_series'
    source_entity_id = 'deviceid'
    # metadata for generating sample
    sample_metrics = ['temp', 'pressure', 'velocity']
    sample_entities = ['entity1', 'entity2', 'entity3']
    sample_initial_days = 3
    sample_freq = '1min'
    sample_incremental_min = 5

    def __init__(self, input_items, output_items=None):
        super().__init__(input_items=input_items, output_items=output_items)

    def get_data(self, start_ts=None, end_ts=None, entities=None):

        self.load_sample_data()
        (query, table) = self._entity_type.db.query(self.source_table_name, schema=self._entity_type._db_schema)
        if not start_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] < end_ts)
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))

        parse_dates = [self._entity_type._timestamp]
        df = pd.read_sql_query(query.statement, con=self._entity_type.db.connection, parse_dates=parse_dates)
        df = df.astype(dtype={col: 'datetime64[ms]' for col in parse_dates}, errors='ignore')

        return df

    @classmethod
    def get_item_values(cls, arg, db=None):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':
            if db is None:
                db = cls._entity_type.db
            return db.get_column_names(cls.source_table_name)
        else:
            msg = 'No code implemented to gather available values for argument %s' % arg
            raise NotImplementedError(msg)

    def load_sample_data(self):

        if not self._entity_type.db.if_exists(self.source_table_name):
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, days=self.sample_initial_days,
                                            timestamp=self._entity_type._timestamp)
        else:
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, seconds=self.sample_incremental_min * 60,
                                            timestamp=self._entity_type._timestamp)

        df = generator.execute()
        self._entity_type.db.write_frame(df=df, table_name=self.source_table_name, version_db_writes=False,
                                         if_exists='append', schema=self._entity_type._db_schema,
                                         timestamp_col=self._entity_type._timestamp_col)

class Turbines(metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring Equipment.
    https://github.com/ibm-watson-iot/functions/blob/60002500117c4559ed256cb68204c71d2e62893d/iotfunctions/metadata.py#L2237
    '''


    def __init__(self,
                 name,
                 db,
                 columns=[],
                 functions=[],
                 db_schema=None,
                 description=None,
                 generate_days=0,
                 drop_existing=True,
                 generate_entities=None,
                 column_map = None,
                 table_name = None
                 ):

        # Initialize Entity Type class variables
        self.db_schema = db_schema
        logging.debug("db_schema %s" %db_schema)
        self.db = db
        logging.debug("db %s" %table_name)
        self.table_name = table_name.upper()
        logging.debug("table_name %s" %table_name)

        rows = []

        # Read CSV File with Entity Type Configuration


        # constants
        constants = []

        physical_name = name.lower()

        # granularities
        granularities = []

        # columns
        # columns = []

        #for fun in functions_found:
        #    functions.append(bif.PythonExpression(expression='df["input_flow_rate"] * df["discharge_flow_rate"]',
        #                                          output_name='output_flow_rate'))

        '''
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
                         # dimension_columns = dimension_columns,
                         output_items_extended_metadata = output_items_extended_metadata,
                         generate_days = generate_days,
                         drop_existing = drop_existing,
                         description = description,
                        db_schema = db_schema)

    def read_meter_data(self, input_file=None):
        # Check to make sure table was created

        source_table_name = self.name # "Equipment"
        logging.debug("DB Name %s " % source_table_name)
        logging.debug("DB Schema %s " % self.db_schema)


        # logging.debug(df.head())
        # df.to_csv(f'../{self.name}.csv')

        if input_file:
            df_to_import = pd.read_csv(input_file) #'/Users/carlos.ferreira1ibm.com/ws/shell/data/COMPRESSORS_D.csv')
            logging.debug(df_to_import.head())
            df = df_to_import
            updated_names = {}
            clean = lambda x: {x: ''.join(re.findall(r'\w+', x)).lower()}
            updated_names_list = list(map(clean, df.columns))
            for k in updated_names_list:
                updated_names.update(k)
            df.rename(updated_names, axis=1, inplace=True)

        else:
            # nothing to import
            return
            df = self.db.read_table(table_name=source_table_name.upper(), schema=self.db_schema)

        '''
        # write the dataframe to the database table
        #mybase = MergeSampleTimeSeries(object, input_items, output_items=None, dummy_items=None))
        #mybase.write_frame(df=df_to_import, table_name=entity_type_name)
        #kwargs = {'table_name': entity_type_name, 'schema': self.db_schema, 'row_count': len(df.index)}
        mybase = MergeSampleTimeSeries(self, input_items=None, output_items=None, dummy_items=None)
        mybase.write_frame(df=df_to_import, table_name=entity_type_name)
        #self.db.write_frame(df=df, table_name=self.source_table_name, version_db_writes=False,
        #                                 if_exists='append', schema=self._entity_type._db_schema,
        #                                 timestamp_col=self._entity_type._timestamp_col)
        entity_type = mybase.get_entity_type()
        entity_type.trace_append(created_by=self, msg='Wrote data to table', log_method=logging.debug, **kwargs)
        #entity.publish_kpis()


        response_back = {"evt_timestamp" : ["2020-06-22T10:21:14.582", "2020-06-22T09:21:14.582", "2020-06-22T08:21:14.582", "2020-06-22T07:21:14.582", "2020-06-22T06:21:14.582"],
                        "deviceid": ["73000", "B", "C", "D", "E"],
                         "asset_id": ["73000", "B", "C", "D", "E"],
                         "entity_id": ["A", "B", "C", "D", "E"],
                         "drvn_t1": [20, 15, 10, 5, 2.5],
                         "drvn_p1": [20, 15, 10, 5, 2.5],
                         "predict_drvn_t1":[20, 15, 10, 5, 2.5],
                         "predict_drvn_p1": [20, 15, 10, 5, 2.5],
                         "drvn_t2": [20, 15, 10, 5, 2.5],
                         "drvn_p2": [20, 15, 10, 5, 2.5],
                         "predict_drvn_t2": [20, 15, 10, 5, 2.5],
                         "predict_drvn_p2": [20, 15, 10, 5, 2.5],
                         "drvn_flow": [20, 15, 10, 5, 2.5],
                         "compressor_in_y": [20, 15, 10, 5, 2.5],
                         "compressor_in_x": [40, 30, 20, 10, 5],
                         "compressor_out_y": [50, 50, 50, 50, 50],
                         "compressor_out_x": [50, 50, 50, 50, 50],
                         "run_status": [5, 5, 5, 5, 4],
                         "run_status_x": [35, 45, 55, 65, 75],
                         "run_status_y": [150, 160, 170, 180, 190],
                         "scheduled_maintenance": [0, 0, 0, 0, 1],
                         "unscheduled_maintenance": [1, 1, 0, 0, 0],
                         "maintenance_status_x": [250, 260, 270, 280, 290],
                         "maintenance_status_y": [35, 45, 55, 65, 75],
                         "drvr_rpm": [10, 20, 30, 40, 50]
                         }

        df = pd.DataFrame(data=response_back)
        '''

        # use supplied column map to rename columns
        #df = df.rename(self.column_map, axis='columns')
        # fill in missing columns with nulls
        required_cols = self.db.get_column_names(table=self.table_name, schema = self.db_schema)
        logging.debug("required_cols ")
        logging.debug(required_cols)
        missing_cols = list(set(required_cols) - set(df.columns))
        logging.debug("missing_cols ")
        logging.debug(missing_cols)

        if len(missing_cols) > 0:
            kwargs = {'missing_cols': missing_cols}
            self.trace_append(created_by=self, msg='http data was missing columns. Adding values.',
                                     log_method=logger.debug, **kwargs)
            for m in missing_cols:
                if m == self._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m == 'devicetype':
                    df[m] = self.logical_name
                else:
                    df[m] = None

        # remove columns that are not required
        print("converting timestamp")
        df['evt_timestamp'] = pd.to_datetime(df['datemmmddyyyy'].apply( lambda x: pd.Timestamp(datetime.strptime(x, '%m/%d/%y %H:%M'))  ))
        df = df[required_cols]
        # df.to_csv("/tmp/output.csv")
        # write the dataframe to the database table
        self.db.write_frame(df=df, table_name=self.table_name.upper(), if_exists='append')
        kwargs = {'table_name': self.table_name.upper(), 'schema': self.db_schema, 'row_count': len(df.index)}
        self.trace_append(created_by=self, msg=f'Wrote input file {input_file} to table', log_method=logger.debug, **kwargs)
        return

    def make_sample_entity(self, db, schema=None, name='as_sample_entity', register=False, data_days=1, freq='1min',
                           entity_count=5, float_cols=5, string_cols=2, bool_cols=2, date_cols=2, drop_existing=False,
                           include_generator=True):
        """
        Build a sample entity to use for testing.
        Parameters
        ----------
        db : Database object
            database where entity resides.
        schema: str (optional)
            name of database schema. Will be placed in the default schema if none specified.
        name: str (optional)
            by default the entity type will be called as_sample_entity
        register: bool
            register so that it is available in the UI
        data_days : number
            Number of days of sample data to generate
        float_cols: list
            Name of float columns to add
        string_cols : list
            Name of string columns to add
        """

        if entity_count is None:
            entities = None
        else:
            entities = ['E%s' % x for x in list(range(entity_count))]

        if isinstance(float_cols, int):
            float_cols = ['float_%s' % x for x in list(range(float_cols))]
        if isinstance(string_cols, int):
            string_cols = ['string_%s' % x for x in list(range(string_cols))]
        if isinstance(date_cols, int):
            date_cols = ['date_%s' % x for x in list(range(date_cols))]
        if isinstance(bool_cols, int):
            bool_cols = ['bool_%s' % x for x in list(range(bool_cols))]

        if drop_existing:
            db.drop_table(table_name=name, schema=schema)

        float_cols = [Column(x.lower(), Float()) for x in float_cols]
        string_cols = [Column(x.lower(), String(255)) for x in string_cols]
        bool_cols = [Column(x.lower(), SmallInteger) for x in bool_cols]
        date_cols = [Column(x.lower(), DateTime) for x in date_cols]

        functions = []
        if include_generator:
            sim = {'freq': freq}
            generator = bif.EntityDataGenerator(ids=entities, parameters=sim)
            functions.append(generator)

        cols = []
        cols.extend(float_cols)
        cols.extend(string_cols)
        cols.extend(bool_cols)
        cols.extend(date_cols)

        entity = metadata.BaseCustomEntityType(name=name, db=db, columns=cols, functions=functions, generate_days=data_days,
                                               drop_existing=drop_existing, db_schema=schema)

        if register:
            entity.register(publish_kpis=True, raise_error=True)
        return entity

class Equipment (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring Equipment.
    '''


    def __init__(self,
                 name,
                 db,
                 db_schema=None,
                 # functions=functions,
                 # columns=columns,
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
