import numpy as np
import pandas as pd
import time
import sys
import datetime


'''
stacking arrays
vertical stacking and horizontal stacking
'''
a = np.array( [ (1, 2,3), (3,4,5)  ]  )
b = np.array( [ (6, 7,8), (9,10,11)  ]  )

print ('np.hstack')
print (np.hstack( (a, b) ) )
'''
 [ [1, 2,3,6,7,8)]
    [3,4,5,9,10,11] ]
'''
print ('np.vstack')
print ( np.vstack((a, b)) )
'''
[   [1,2,3]
    [3,4,5]
    [6, 7,8]
    [9,10,11] 
'''

'''
matrix math

'''
a = np.array( [ (1, 2,3), (3,4,5)  ]  )
b = np.array( [ (1, 2,3), (3,4,5)  ]  )
print ( a + b )   # [ (2, 4, 6)  , (6, 8, 10) ]
print ( a * b )   # [ (1, 4, 9)  , (9, 16, 25) ]
print ( a / b )   # [ (1, 1, 1)  , (1, 1, 1) ]

'''
Sum of axis
            axis 0
            8,  9,
axis 1      10, 11
            12, 13
'''
b = np.array( [ (8,9), (10,11) , (12, 13) ]  )
print ( b.sum( axis=0 ) )   # [ 30 33 ]
print ( b.sum( axis=1 ) )   #  [ 17 21 25 ]
# Square root
print('square root')
print(np.sqrt(b) )
# STandard deviation
print('standard deviation' )
print(np.std(b) )

# Get  min , max and sum
a = np.array( [ (8,1,1,1), (1,1,1,15)]  )
print ( a.min() )   #1
print ( a.max() )   #15
print ( a.sum() )   #29

# Get 10 alues from 1 to 3
a =  np.linspace(1,3,10)
print(a)
'''
[1.         1.22222222 1.44444444 1.66666667 1.88888889 2.11111111
 2.33333333 2.55555556 2.77777778 3.        ]
'''

'''
Reshape

    8,  9,  10, 11      >>  8 ,9
    12, 13, 14, 15          10, 11
                            12, 13
                            14, 15

Slicing
     8,  9      >>  9 ,11
    10, 11
    12, 13
    14, 15

# Change shape of array
a = np.array( [ (8,9,10,11), (12,13,14,15)]  )
print('before reshape')
print(a)
print('after reshape and before slicing')
a = a.reshape(4,2)
print(a)

# slicing array
# Get size
# use : to get all rows of data including 0 for first one.   00:   gets all rows
print('after slicing')
print(a[0,1])  # 9
print(a[3,0])  # 14
print(a[2,1])  # 13
print(a[0:,1])  #  [9, 11, 13, 15]  Get all the rows when using 0:data from second column
print(a[0:,0])  #  [8, 10, 12, 14]   Get all the rows when using 0:  from first column
print(a[0:3,0])  #  [8, 10, 12  ]   Ignore items after the :
print(a[1:4,1])  #  [11, 13, 15  ]   Ignore items after the :

# Find size of array
a = np.array(  (1,2,3)   )
print('data size is  ')
print(a.size)

# Find shape of array
a = np.array( [ (1,2,3, 4, 5), (1,2,3, 4, 5)]   )
print('data shape is 2, 5 ')
print(a.shape)


# Find dimension of NP arrays self.assert_
a = np.array(  [ (1,2,3) , (2,3,4) ] )
print('dimension is 2 ')
print(a.ndim)

# Find dimensinos of NP arrays self.assert_
a = np.array(  (1,2,3)   )
print('dimension is 1 ')
print(a.ndim)

# Find dimensinos of NP arrays self.assert_
a = np.array(  (1,2,3)   )
print('data type is int64 ')
print(a.dtype)

# NP arrays are faster
SIZE =  1000
L1 =  range (SIZE)
L2 =  range (SIZE)

A1 =  np.arange(SIZE)
A2 =  np.arange(SIZE)

start = time.time()
result =  [(x,y) for x, y in zip(L1, L2) ]
print (  ( time.time() - start ) * 1000  )

start = time.time()
result = A1 + A2
print (  ( time.time() - start ) * 1000  )

# NP arrays are less memory
# Range is an array of  1000 numbers stored in S.
S = range(1000)
print( sys.getsizeof(5)*len(S) )

D = np.arange( 1000 )
print(D.size * D.itemsize)

# Single dimensional array
a = np.array ( [1,2,3] )
print(a)

# 2  dimensional array
print("2--")
numpy_data = np.array(  [ (1,2,3) , (2,3,4) ]     )
print(numpy_data)

# Convert np array into a dataframe
print("3--")
numpy_data = np.array([[1, 2], [3, 4]])
df = pd.DataFrame(data=numpy_data, index=["row1", "row2"], columns=["column1", "column2"])
print(df)

'''
# Slicing Panda Dataframes
print("Slicing Panda Dataframes")
df = pd.DataFrame(np.arange(20).reshape(5,4), columns=["A","B","C","D"])
print(df)

# Select a Panda Dataframe column
print( df.loc[:,"A"] )
print( df["A"])
print( df.A )

# To Select multiple Panda Dataframe column
print(  df.loc[:, ["A", "C"]] )
print( df[["A", "C"]] )

#  To select a row in a Pandas Dataframe  by its label
print( df.loc[1] )

#  To select multiple rows in a Pandas Dataframe  by its label
print( df.loc[[0,1]] )

# Accessing values by row and column label.
print(df.loc[0,"D"] )

# Accessing values in row for multiple column label.
print( df.loc[1,["A", "C"]] )

# Accessing values from multiple rows but same column.
print(df.loc[[0,1],"B"])

# You can select data from a Pandas DataFrame by its location. Note, Pandas indexing starts from zero.
# Select a row by index location.
print(df.iloc[0])

# Select data at the specified row and column location, index starts with 0.  format row, col
print( df.iloc[0,3] )

# Select list of rows and columns
print( df.iloc[[1,2],[0, 1]] )

# Slicing Rows and Columns using labels
# You can select a range of rows or columns using labels or by position.
# To slice by labels you use loc attribute of the DataFrame.

# Slice rows by label.  [ rows:rows, columns:columns]
print(  df.loc[1:3, :] )

# Slice by columns by label.  [ rows:rows, columns:columns]
print( df.loc[:, "B":"D"] )

# Slicing Rows and Columns by position
# To slice a Pandas dataframe by position use the iloc attribute. Remember index starts from 0 to (number of rows/columns - 1).
print ( df.iloc[0:2,:] )

# To slice columns by index position.
print ( df.iloc[:,1:3]  )

# To slice row and columns by index position.
#       df.iloc[row_start : row_finish, col_start:col_finish] )
print ( df.iloc[1:2,1:3] )
print ( df.iloc[:2,:2] )


#  Subsetting by boolean conditions
# You can use boolean conditions to obtain a subset of the data from the DataFrame.
print ( df[df.B == 9] )
print ( df.loc[df.B == 9] )

#  Return rows  in  with column B that have a 9 or 13
print ( df[df.B.isin([9,13])] )

# Rows that match multiple boolean conditions.
print (  df[(df.B == 5) | (df.C == 10)] )

# Select rows whose column does not contain the specified values.
print ( df[~df.B.isin([9,13])] )

# Select columns based on row value
# To select columns whose rows contain the specified value.
print ( df.loc[:,df.isin([9,12]).any()] )

# Subsetting using filter method
# Subsets can be created using the filter method like below.
print (  df.filter(items=["A","D"]) )

# Subsets of a rowu using index.
print ( df.filter(like="2", axis=0) )
#Subsets of a rowu using reg expression not  columns AB .
print ( df.filter(regex="[^AB]") )




def f(df, parameters=None):
    import numpy as np
    return np.where(df['run_status'] == 5, "Online", "Offline")

def m(df, parameters=None):
    # Converts a 0 1 integer array into a string value in a dataframe
    import numpy as np
    # = np.where(df== 0, "Online", "Offline")
    print ("fun m")
    print ( df['scheduled_maintenance'] )
    print ( df['unscheduled_maintenance'] )
    return np.where( (df['unscheduled_maintenance'] == 1) | (df['scheduled_maintenance'] == 1),  "Being Serviced", "Not being serviced")

def l(df, parameters=None):


    df = pd.DataFrame({'Age': [30, 20, 22, 40, 32, 28, 39],
                       'Color': ['Blue', 'Green', 'Red', 'White', 'Gray', 'Black',
                                 'Red'],
                       'Food': ['Steak', 'Lamb', 'Mango', 'Apple', 'Cheese',
                                'Melon', 'Beans'],
                       'Height': [165, 70, 120, 80, 180, 172, 150],
                       'Score': [4.6, 8.3, 9.0, 3.3, 1.8, 9.5, 2.2],
                       'State': ['NY', 'TX', 'FL', 'AL', 'AK', 'TX', 'TX']
                       },
                      index=['Jane', 'Nick', 'Aaron', 'Penelope', 'Dean',
                             'Christina', 'Cornelia'])

    print("\n -- loc -- \n")
    print(df.loc[df['Age'] < 30, ['Color', 'Height']])

    print("\n -- iloc -- \n")
    print(df.iloc[(df['Age'] < 30).values, [1, 3]])

    return


def score(df, parameters=None):


    left = pd.DataFrame({'evt_timestamp': [pd.Timestamp('2020-04-10 07:46:14.687196'), pd.Timestamp('2020-04-10 07:41:14.687196'), pd.Timestamp('2020-04-10 07:36:14.687196'), pd.Timestamp('2020-04-05 21:37:04.209610'),
                                         pd.Timestamp('2020-04-05 21:42:09.209610'), pd.Timestamp('2020-04-05 21:47:00.209610'), pd.Timestamp('2020-04-10 07:31:14.687196'), pd.Timestamp('2020-04-10 07:26:14.687196')],
                        'drvn_p1': [19.975879, 117.630665, 17.929952, 1.307068,
                                    0.653883, 0.701709, 16.500000, 16.001709],
                        'deviceid': ['73001', '73001', '73001', '73000',
                                     '73000', '73000', '73001', '73001'],
                        'drvr_rpm': [165, 999, 163, 30,
                                     31, 33, 150, 149],
                        'anomaly_score': [0, 0, 0, 0,
                                          0, 0, 0, 0]
                       },
                      index=[0, 1, 2, 3,
                             4, 5, 6, 7])


    right_a = pd.DataFrame({'anomaly_score': [1, -1, 1, 1, 1],
                            'drvn_p1': [19.975879, 117.630665, 17.929952, 16.500000, 16.001709],
                            'drvr_rpm': [165, 999, 163, 150, 149]
                       },
                      index=[0, 1, 2, 6, 7])


    # 'asset_id': ['73001', '73001', '73001', '73001']
    print('left.dtypes')
    print(left.dtypes)
    print("\n -- Left starts with   -- \n")
    print(left)

    # Using DataFrame.insert() to add a column
    #left.insert(2, "Age", [21, 23, 24, 25,21, 23, 24, 25], True)
    #print('left.added age')
    #print(left)

    print("\n -- Right starts with   -- \n")
    print('right_a.dtypes')
    print(right_a.dtypes)
    print(right_a)

    # Add asset_id to the right_a df
    right_a['deviceid'] = '73001'
    # Get the original evt_timestamp from the original df and insert it to the scored df so that you avoid column mismatch when merging.
    right_a.insert(1, 'evt_timestamp', left.loc[left['deviceid'] == '73001', ['evt_timestamp']], True)
    print("\n -- Right after adding deviceid column and evt_timestamp  -- \n")
    print(right_a)

    # Set Pandas display options
    # https://towardsdatascience.com/how-to-show-all-columns-rows-of-a-pandas-dataframe-c49d4507fcf
    pd.set_option('display.max_columns', None)
    # pd.reset_option(“max_columns”)
    pd.set_option('max_rows', None)
    #print(right_a)

    # Concat the rows of two panda data frames
    # https://www.datacamp.com/community/tutorials/joining-dataframes-pandas
    #df_rows = pd.concat([left, right_a], ignore_index=False)
    #print('df_rows')
    #print(df_rows)

    #  Concat the columns of two panda data frames
    # https://www.datacamp.com/community/tutorials/joining-dataframes-pandas
    #df_concat_cols = pd.concat([left, right_a], axis=1)
    #print('df_concat_cols')
    #print(df_concat_cols)

    # Merge the columns of two panda data frames
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
    # https://www.datacamp.com/community/tutorials/joining-dataframes-pandas
    #df_merg_cols = pd.merge(right_a, left, on='asset_id' )
    #print('df_cols merge')
    #print(df_merg_cols)

    #  Merge the columns of two panda data frames
    #  https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
    #df_merge_left = left.merge(right_a, how='left', on=['anomaly_score','asset_id','drvr_rpm','drvn_p1'])
    #print('df_merge_left')
    #print(df_merge_left)

    #  Merge the columns of two panda data frames
    #  https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html
    #df_merge_right = right_a.merge(right_a, how='right', on=['anomaly_score','asset_id','drvr_rpm','drvn_p1'])
    #print('df_merge_right')
    #print(df_merge_right)

    # how to convert to mintes
    # s = pd.to_timedelta(s_df['evt_timestamp'])
    # s_df['evt_timestamp'] = s / pd.offsets.Minute(1)

    # print("Converting a column to float")
    # print ( s )
    # s_df['evt_timestamp'] = s_df['evt_timestamp'].dt.strftime('%B %d, %Y, %r').astype(float)
    # s_df['evt_timestamp'] = s_df['evt_timestamp'].dt.strftime('%B %d, %Y, %r').astype(float)
    # s_df['evt_timestamp'] = pd.to_datetime(s_df['evt_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f000Z')
    # s_df['evt_timestamp'] = pd.to_datetime(df['evt_timestamp'], format='%Y-%m-%dT%H:%M:%S.%f000Z')

    # s_df['evt_timestamp'] = datetime.datetime.strptime( s_df['evt_timestamp'], '%Y-%m-%s %h:%m:%s').isoformat()

    #  Combine the columns of two panda data frames
    df_combine_first = right_a.combine_first(left)
    print('df_combine_first')
    print(df_combine_first)

    # Validate you are getting good data for a single asset by plotting data
    import matplotlib.ticker as ticker
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    from matplotlib.backends.backend_pdf import PdfPages
    with PdfPages(r'/Users/carlos.ferreira1ibm.com/ws/isolation-forestCharts.pdf') as export_pdf:
        #df_combine_first.loc[df_combine_first['deviceid'] == '73001']['evt_timestamp'].dt.strftime('%Y-%m-%d')
        #plt.plot(df_combine_first.loc[df_combine_first['deviceid'] == '73001']['drvn_p1'], color='green', label='drvn_p1')
        #plt.plot(df_combine_first.loc[df_combine_first['deviceid'] == '73001']['anomaly_score'] * 10, color='red', label='anomaly_score')

        # Create figure and plot space
        fig, ax = plt.subplots(figsize=(12, 12))

        # Add x-axis and y-axis use ax.plot for line and ax.bar for bar chart
        print(df[(df.B == 5) & (df['evt_timestamp'] < pd.Timestamp('2020-04-10 07:41:14.687196'))])

        ax.plot(df_combine_first.loc[df_combine_first['deviceid'] == '73001'].sort_values(by='evt_timestamp',ascending=True)['evt_timestamp'],
               df_combine_first.loc[df_combine_first['deviceid'] == '73001'].sort_values(by='evt_timestamp',ascending=True)['drvn_p1'],
               color='purple')

        # Set title and labels for axes
        ax.set(xlabel="Date",
               ylabel="drvn_p1 (psi)",
               title="Pressure\n Test")

        #plt.show()
        export_pdf.savefig()
        plt.close()

    # Test values for x axis labels
    print("Test values for x axis labels")
    print (df_combine_first.loc[df_combine_first['deviceid'] == '73001']['evt_timestamp'] )

    orig_left = left

    print("\n -- simple merge right  -- \n")
    #left['anomaly_score'] = left['anomaly_score'].astype(int)
    right_a['anomaly_score'] = right_a['anomaly_score'].astype(int)
    left['anomaly_score'] = left['anomaly_score'].astype(int)
    #left['evt_timestamp'] = left['evt_timestamp'].astype(datetime)
    left = left.merge(right_a, how="left")
    print(left)

    print("\n -- simple merge left  -- \n")
    right_a['anomaly_score'] = right_a['anomaly_score'].astype(int)
    orig_left['anomaly_score'] = orig_left['anomaly_score'].astype(int)

    orig_left = orig_left.merge(right_a, how="right")
    print(orig_left)

    print("\n -- before  -- \n")
    print(left.loc[left['deviceid'] == '73001', ['deviceid', 'evt_timestamp', 'drvn_p1', 'drvr_rpm']])
    print(left.loc[left['deviceid'] == '73000', ['deviceid','evt_timestamp', 'drvn_p1', 'drvr_rpm']])
    '''

    print("\n -- after merged_asof left , right -- \n")
    merged_asof = pd.merge_asof(left, right_a,
                                by = 'asset_id')
    #print(merged_asof.loc[merged_asof['asset_id'] == '73001', ['asset_id', 'evt_timestamp', 'drvn_p1', 'drvr_rpm', 'anomaly_score']])
    #print(merged_asof.loc[merged_asof['asset_id'] == '73000', ['asset_id', 'evt_timestamp', 'drvn_p1', 'drvr_rpm', 'anomaly_score']])
    print(merged_asof)
    '''
    return

def isolation(parameters=None):

    # Import csv file
    df = pd.read_csv("/Users/carlos.ferreira1ibm.com/ws/shell/data/turbine_data.csv", header=None)
    print(df.head())
    from sklearn.ensemble import IsolationForest
    X = [[-1.1], [0.3], [0.5], [100]]
    print (X)
    clf = IsolationForest(random_state=0).fit(X)
    clf.predict([[0.1], [0], [90]])
    array([1, 1, -1])
    return

def main(args):
    inputfile = args
    print ('Reading args %s', inputfile)

    '''
    # Metrics
    # run_status = 5 running  0 not running
	# scheduled_maintenance = 1 maintenance or 0 None
	# unscheduled_maintenance = 0 None or 1 unscheduled maintenance
	

    #  Create numpy dimensional array of run status single metric modes running ie 1  versus not running ie 0
    numpy_data = np.array([[0], [1], [0], [5], [0]])
    print("numpy_data  for f -----------------------------------")
    print(numpy_data)
    # Convert np array into a dataframe
    df = pd.DataFrame(data=numpy_data, index=["row1", "row2", "row3", "row4", "row5"], columns=["run_status"])
    print(df)

    val = f(df=df, parameters=None)
    print("Status returned as DF")
    print(val)

    #  Create numpy dimensional array of scheduuled_maintenance and scheduuled_maintenance metric
    #  Modes running ie 1  versus not running ie 0
    numpy_data = np.array([[0,0], [1,0], [0,0], [0,0], [0,0]])
    print("numpy_data  for maintenance -----------------------------------")
    print(numpy_data)
    # Convert np array into a dataframe
    df = pd.DataFrame(data=numpy_data, index=["row1", "row2", "row3", "row4", "row5"], columns=["scheduled_maintenance", "unscheduled_maintenance" ])
    print(df)
    val = m(df=df, parameters=None)
    print("Status returned as DF")
    print(val)
    '''
    #  Create numpy dimensional array of rpm and pressure to  separate df for scoring then merge the two DFs
    val = score(df=df, parameters=None)
    print("Status returned as DF for location test")
    print(val)
    print ( 'Done testing ' )


    #  Create numpy dimensional array of rpm and pressure to  separate df for scoring then merge the two DFs
    #val = isolation( parameters=None)
    #print ( 'Done testing Isolation Forest ' )


if __name__ == "__main__":
    main(sys.argv[1:])