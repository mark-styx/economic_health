from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#get file list
from glob import glob
files = glob('*.json')
print(files)

#execution timer
from datetime import datetime as dt
def ex_time(func):
    def execution_time(*args,**kw):
        start = dt.now()
        execution = func(*args,**kw)
        end = dt.now()
        print('''execution time: {0}
        '''.format(end-start))
        return execution
    return execution_time

#create dataframe function
@ex_time
def create_df(file_to_load):
    df = sqlContext.read.json(file_to_load)
    print(df.columns)
    sqlContext.registerDFasTable(df,str(file_to_load))
    return df

#create dataframes
df_dict = {}
for f in files:
    name = str(f).replace('.json','')
    df_dict[name] = create_df(f)

#table operations
df_dict[].sql('select col from tbl')