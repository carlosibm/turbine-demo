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

class Turbines (metadata.BaseCustomEntityType):

    '''
    Sample entity type for monitoring Equipment.
    https://github.com/ibm-watson-iot/functions/blob/60002500117c4559ed256cb68204c71d2e62893d/iotfunctions/metadata.py#L2237
    '''


    def __init__(self,
                 name,
                 db,
                 db_schema=None,
                 description=None,
                 generate_days=0,
                 drop_existing=True,
                 generate_entities=None,
                 column_map = None,
                 table_name = None
                 ):
        if (len(sys.argv) > 0):
            entity_type_name = sys.argv[1]
            input_file = sys.argv[2]
            logging.debug("entity_type_name %s" % table_name)
            logging.debug("input_file %s" % input_file)
        else:
            logging.debug("Please provide path to csv file as script argument")
            exit()

        # Initialize Entity Type class variables
        self.db_schema = db_schema
        logging.debug("db_schema %s" %db_schema)
        self.db = db
        logging.debug("db %s" %table_name)
        self.table_name = table_name.upper()
        logging.debug("table_name %s" %table_name)
        self.entity_type = entity_type_name

        rows = []

        # Read CSV File with Entity Type Configuration
        print("Open File")
        with open(input_file, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            point_dimension_values = {
                "label": "",
                "units": "",
                "parameter_name": ""
            }
            metrics = []
            dims = []
            constants = []
            funs =[]

            for row in csv_reader:
                if line_count == 0:
                    logging.debug("Column names are %s" % {", ".join(row)})
                    line_count += 1
                else:
                    try:
                        parameter_name = row["Point"].replace(' ', '_')
                        logging.debug("Name %s" % parameter_name)
                        type = row["DataType"]
                        logging.debug("Type %s" % type)
                        parameter_value = row["Value"].replace(' ', '_')
                        logging.debug("Value %s" % parameter_value)

                        if parameter_name == "":
                            break # No more rows

                        # Create metric
                        if row["Point_Data_Type"] == "S":
                            logging.debug("________________________ Point point_data_type metric")
                            metric_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                            metrics.append(metric_to_add)

                        # Create dimension
                        if row["Point_Data_Type"] == "D":
                            logging.debug("________________________ Point point_data_type dimension")
                            dim_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                            dims.append(dim_to_add)

                        # Create Constant
                        if row["Point_Data_Type"] == "C":
                            logging.debug("________________________ Point point_data_type constant")
                            constant_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                            constants.append(constant_to_add)

                        # Create Function
                        if row["Point_Data_Type"] == "F":
                            logging.debug("________________________ Point point_data_type function")
                            fun_to_add = {'parameter_name': parameter_name, 'type': type, 'value':parameter_value}
                            funs.append(fun_to_add)
                    except:
                        logging.debug(sys.exc_info()[0])  # the exception instance
                        break

        # constants
        constants = []

        physical_name = entity_type_name.lower()

        # granularities
        granularities = []

        # columns
        columns = []

        # Add metrics
        for metric in metrics:
            logging.debug("Adding metric name to entity type %s" %metric['parameter_name'] )
            logging.debug("Adding metric type to entity type %s" %metric['type'] )
            logging.debug("Adding metric value to entity type %s" % metric['value'])
            unallowed_chars = "!@#$()"
            for char in unallowed_chars:
                metric['parameter_name'] = metric['parameter_name'].replace(char, "")
            logging.debug("Adding cleansed metric name to entity type %s" % metric['parameter_name'])
            if metric['type'] == "Float":
                columns.append(Column(metric['parameter_name'], Float()))
            if metric['type'] == "String":
                columns.append(Column(metric['parameter_name'], String(metric['value'])))
            if metric['type'] == "Integer":
                columns.append(Column(metric['parameter_name'], Integer()))
            if metric['type'] == "Timestamp":
                columns.append(Column(metric['parameter_name'], DateTime()))

        # Add dimensions
        self.dimension_columns = []
        for dim in dims:
            logging.debug("Adding dimension name to entity type %s" %dim['parameter_name'] )
            logging.debug("Adding metric type to entity type %s" %dim['type'] )
            unallowed_chars = "!@#$()"
            for char in unallowed_chars:
                dim['parameter_name'] = dim['parameter_name'].replace(char, "")
            self.dimension_columns.append(Column(dim['parameter_name'], String(50)))
            logging.debug("Adding cleansed dimension name to entity type %s" % dim['parameter_name'])

        # functions
        functions = []
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

        super().__init__(name=entity_type_name,
                         db = db,
                         constants = constants,
                         granularities = granularities,
                         columns=columns,
                         functions = functions,
                         dimension_columns = self.dimension_columns,
                         output_items_extended_metadata = output_items_extended_metadata,
                         generate_days = generate_days,
                         drop_existing = drop_existing,
                         description = description,
                        db_schema = db_schema)

    def pretty(d, indent=0):
        for key, value in d.items():
            print('\t' * indent + str(key))
            if isinstance(value, dict):
                pretty(value, indent + 1)
            else:
                print('\t' * (indent + 1) + str(value))

    def read_meter_data(self, input_file=None, first_row=None, column_map=None):
        '''
       # Check to make sure table was created
        source_table_name = "Equipment"
        logging.debug("DB Name %s " % source_table_name)
        logging.debug("DB Schema %s " % self.db_schema)

        df = self.db.read_table(table_name=source_table_name.upper(), schema=self.db_schema)
        logging.debug(df.head())
        df.to_csv('/Users/carlos.ferreira1ibm.com/ws/shell/data/Equipment.csv')
        '''

        # Ingest timeseries data from csv file
        first_row == True
        file_to_ingest = '/Users/carlos.ferreira1ibm.com/ws/shell/data/COMPRESSORS_D.csv'
        print("Open File  %s " %file_to_ingest)

        with open(file_to_ingest, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            fist_time = True
            timeseries_data = []
            data_dict = {
                "evt_timestamp": "",
                "deviceid": "",
                "devicetype": "",
                "logicalinterface_id": "",
                "asset_id": "",
                "entity_id": "",
                "drvn_t1": "",
                "drvn_p1": "",
                "predict_drvn_t1": "",
                "predict_drvn_p1": "",
                "drvn_t2": "",
                "drvn_p2": "",
                "predict_drvn_t2": "",
                "predict_drvn_p2": "",
                "drvn_flow": "",
                "compressor_in_y": "",
                "compressor_in_x": "",
                "compressor_out_y": "",
                "compressor_out_x": "",
                "run_status": "",
                "run_status_x": "",
                "run_status_y": "",
                "scheduled_maintenance": "",
                "unscheduled_maintenance": "",
                "maintenance_status_x": "",
                "maintenance_status_y": "",
                "drvr_rpm": ""
            }

            for row in csv_reader:
                if first_row == True:
                    logging.debug("Timeseries Data Column names are %s" % {", ".join(row)})
                    first_row = False
                    line_count += 1
                else:
                    #try:
                    if row["DEVICEID"] == "":
                        break  # No more rows
                    logging.debug("line_count %s " %line_count)
                    timeseries_data.append({
                        "deviceid": row["DEVICEID"],
                        "evt_timestamp": row["EVT_TIMESTAMP"],
                        "devicetype": row["DEVICETYPE"],
                        "logicalinterface_id": row["LOGICALINTERFACE_ID"],
                        "asset_id": row["ASSET_ID"],
                        "entity_id": row["ENTITY_ID"],
                        "drvn_t1": row["DRVN_T1"],
                        "drvn_p1": row["DRVN_P1"],
                        "predict_drvn_t1": row["PREDICT_DRVN_T1"],
                        "predict_drvn_p1": row["PREDICT_DRVN_P1"],
                        "drvn_t2": row["DRVN_T2"],
                        "drvn_p2": row["DRVN_P2"],
                        "predict_drvn_t2": row["PREDICT_DRVN_T2"],
                        "predict_drvn_p2": row["PREDICT_DRVN_P2"],
                        "drvn_flow": row["DRVN_FLOW"],
                        "compressor_in_y": row["COMPRESSOR_IN_Y"],
                        "compressor_in_x": row["COMPRESSOR_IN_X"],
                        "compressor_out_y": row["COMPRESSOR_OUT_Y"],
                        "compressor_out_x": row["COMPRESSOR_OUT_X"],
                        "run_status": row["RUN_STATUS"],
                        "run_status_x": row["RUN_STATUS_X"],
                        "run_status_y": row["RUN_STATUS_Y"],
                        "scheduled_maintenance": row["SCHEDULED_MAINTENANCE"],
                        "unscheduled_maintenance": row["UNSCHEDULED_MAINTENANCE"],
                        "maintenance_status_x": row["MAINTENANCE_STATUS_X"],
                        "maintenance_status_y": row["MAINTENANCE_STATUS_Y"],
                        "drvr_rpm": row["DRVR_RPM"],
                    })
                    logging.debug("Reading metric name deviceid %s" % row["DEVICEID"] )
                    logging.debug("Reading metric name evt_timesamp %s" % row["EVT_TIMESTAMP"])
                    logging.debug("Reading metric name devicetype %s" % row["DEVICETYPE"])
                    logging.debug("Reading metric name run_status %s" % row["RUN_STATUS"])
                    logging.debug("Reading metric name logicalinterface_id %s" % row["LOGICALINTERFACE_ID"])
                    line_count += 1
                    #except:
                    #    logging.debug(sys.exc_info()[0])  # the exception instance
                    #    break

        logging.debug("Imported Compressors Timeseries Data")
        logging.debug(timeseries_data)

        # Compare df_to_import  versus df using json object.
        pd.set_option('display.max_columns', None)
        df_to_import = pd.DataFrame(data=timeseries_data)
        logging.debug(df_to_import.head())

        # Transpose the rows and columns
        # use supplied column map to rename columns
        if column_map is None:
            column_map = {}
        self.column_map = column_map
        df = df_to_import.rename(self.column_map, axis='columns')
        # fill in missing columns with nulls
        required_cols = self.db.get_column_names(table=self.table_name, schema=self.db_schema)
        missing_cols = list(set(required_cols) - set(df.columns))
        if len(missing_cols) > 0:
            kwargs = {'missing_cols': missing_cols}
            self.trace_append(created_by=self, msg='CSV Timeseries Data was missing columns. Adding values.',
                                     log_method=logger.debug, **kwargs)
            for m in missing_cols:
                if m == self._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m == 'devicetype':
                    df[m] = entity_type.logical_name
                else:
                    df[m] = None

        # remove columns that are not required
        df = df[required_cols]
        logging.debug("Transposed DF---")
        logging.debug(df.head())

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
        '''

        '''        
        DEBUG
        response_back = [
            {'deviceid': '73005', 'evt_timesamp': '2020-05-16-00.02.35.464000', 'devicetype': 'GasTurbines3',
             'run_status': '0'}, {'deviceid': '73008', 'evt_timesamp': '2020-05-16-00.07.35.464000',
                                  'devicetype': 'GasTurbines3', 'run_status': '0'}, {'deviceid': '73003',
                                                                                     'evt_timesamp': '2020-05-16-00.12.35.464000',
                                                                                     'devicetype': 'GasTurbines3',
                                                                                     'run_status': '0'}, {
                'deviceid': '73003', 'evt_timesamp': '2020-05-16-00.17.35.464000', 'devicetype': 'GasTurbines3',
                'run_status': '-1'}, {'deviceid': '73000', 'evt_timesamp': '2020-05-16-00.22.35.464000',
                                      'devicetype': 'GasTurbines3', 'run_status': '0'}, {'deviceid': '73001',
                                                                                         'evt_timesamp': '2020-05-16-00.27.35.464000',
                                                                                         'devicetype': 'GasTurbines3',
                                                                                         'run_status': '0'}, {
                'deviceid': '73005', 'evt_timesamp': '2020-05-16-00.32.35.464000', 'devicetype': 'GasTurbines3',
                'run_status': '0'}, {'deviceid': '73009', 'evt_timesamp': '2020-05-16-00.37.35.464000',
                                     'devicetype': 'GasTurbines3', 'run_status': '-1'}, {'deviceid': '73007',
                                                                                         'evt_timesamp': '2020-05-16-00.42.35.464000',
                                                                                         'devicetype': 'GasTurbines3',
                                                                                         'run_status': '-2'}, {
                'deviceid': '73008', 'evt_timesamp': '2020-05-16-00.47.35.464000', 'devicetype': 'GasTurbines3',
                'run_status': '0'}]
        '''

        response_back = {"evt_timestamp" : ["2020-06-21T10:21:14.582", "2020-06-21T09:21:14.582", "2020-06-21T08:21:14.582", "2020-06-21T07:21:14.582", "2020-06-21T06:21:14.582"],
                        "deviceid": ["K-14040", "K-14041", "K-14042", "K-14043", "K-14044"],
                         "asset_id": ["K-14040", "K-14041", "K-14042", "K-14043", "K-14044"],
                         "entity_id": ["K-14040", "K-14041", "K-14042", "K-14043", "K-14044"],
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

        #df = pd.DataFrame(data=response_back)
        logging.debug("DF from timeseries data csv ")
        df =  pd.DataFrame(data=timeseries_data)
        logging.debug(df.head())

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
                    logging.debug("logical_name %s" %self.logical_name)
                    df[m] = self.logical_name
                else:
                    df[m] = None

        # remove columns that are not required
        df = df[required_cols]

        # write the dataframe to the database table
        self.db.write_frame(df=df, table_name=self.table_name.upper())
        kwargs = {'table_name': self.table_name.upper(), 'schema': self.db_schema, 'row_count': len(df.index)}
        self.trace_append(created_by=self, msg='Wrote data to table', log_method=logger.debug, **kwargs)
        return

    def make_sample_entity(self, db, schema=None, name='as_sample_entity', register=False, data_days=1, freq='1min',
                           entity_count=5, float_cols=5, string_cols=2, bool_cols=2, date_cols=2, drop_existing=True,
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

        # Assign dimensions to entities from EntityConfig.CSV


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
