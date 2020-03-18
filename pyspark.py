from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

import os
from pyspark.sql.functions import lit

#get file list
from glob import glob
files = glob('/economic_health/*.json')
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
    file_to_load = os.path.basename(file_to_load)
    print('loading {0}...'.format(file_to_load))
    df = sqlContext.read.json(file_to_load)
    sqlContext.registerDataFrameAsTable(df,str(file_to_load).replace('.json',''))
    print('done.')
    return df


#create dataframes and store as dictionary
@ex_time
def build_df_dictionary():
    print('creating dictionary...')
    df_dict = {}
    print('creating {0} dataframes...'.format(len(files)))
    for f in files:
        name = str(f).replace('.json','')
        name = os.path.basename(name)
        df_dict[name] = create_df(f)
    print('dictionary ready.')
    return df_dict


#function to unstack g17 df columns
@ex_time
def unstack(df,col_list):
    i = 1
    merged = df.select(col_list[0])
    new_col = ['amt']
    new_columns = zip(merged.columns,new_col)
    for item in new_columns:
        print(item)
        merged = merged.withColumnRenamed(item[0],item[1])
    merged = merged.withColumn('year',lit(str(col_list[0])))
    while i < len(col_list)-1:
        print('{0} of {1}'.format(i,len(col_list)-2))
        try:
            temp = df.select(col_list[i])
            new_columns = zip(temp.columns,new_col)
            for item in new_columns:
                temp = temp.withColumnRenamed(item[0],item[1])
            temp = temp.withColumn('year',lit(str(col_list[i])))
            merged = merged.unionAll(temp)
            temp = None
        except:
            pass
        i = i + 1
    return merged


#operations
data = build_df_dictionary()
dataframes = data.keys()

#unstack g17
g17_col = data['g17_industrial_production_and_cap_utilization'].columns
g17_year_col = [x for x in g17_col if x.find('-') != -1]
g17 = unstack(data['g17_industrial_production_and_cap_utilization'],g17_year_col)
#summarize g17    
sqlContext.registerDataFrameAsTable(g17, "g17")
g17 = sqlContext.sql('''
select 
    avg(amt) as g17,
    cast(substring(year,1,4) as int) as year 
from g17 group by cast(substring(year,1,4) as int)''')
sqlContext.registerDataFrameAsTable(g17, "g17")

#summarize gdp
gdp = data['gdp_by_year'].select('gdp_in_billions_of_2012_dollars','year')
gdp = gdp.withColumnRenamed('gdp_in_billions_of_2012_dollars','gdp')
sqlContext.registerDataFrameAsTable(gdp, "gdp")
gdp = sqlContext.sql('select gdp,cast(year as int) as year from gdp')
sqlContext.registerDataFrameAsTable(gdp, "gdp")

#summarize consumer confidence index
cci = data['consumer_confidence_index'].select('TIME','Value')
sqlContext.registerDataFrameAsTable(cci, "cci")
cci = sqlContext.sql('''
select 
    avg(Value) as cci,
    cast(substring(TIME,1,4) as int) as year 
from cci group by cast(substring(TIME,1,4) as int)''')
sqlContext.registerDataFrameAsTable(cci, "cci")

#summarize retail trade
retail = data['retail_trade']
sqlContext.registerDataFrameAsTable(retail, "retail")
retail = sqlContext.sql('''
select
    sum(april+august+december+february+january+july+jun+march+may+november+october+september) as retail,
    cast(year as int) as year
from retail group by cast(year as int)''')
sqlContext.registerDataFrameAsTable(retail, "retail")

#summarize cons_credit
cons_credit = data['consumer_credit']
cons_credit = cons_credit.withColumnRenamed("Nonrevolving consumer credit owned and securitized, seasonally adjusted level","nonrev")
cons_credit = cons_credit.withColumnRenamed("Revolving consumer credit owned and securitized, seasonally adjusted level","rev")
cons_credit = cons_credit.withColumnRenamed("Series Description","year")
sqlContext.registerDataFrameAsTable(cons_credit, "cons_credit")
cons_credit = sqlContext.sql('''
select 
    sum(nonrev) as non_rev_cred,
    sum(rev) as rev_cred,
    cast(substring(year,1,4) as int) as year
from cons_credit group by cast(substring(year,1,4) as int)''')
sqlContext.registerDataFrameAsTable(cons_credit, "cons_credit")

#summarize median family income
mfi = data['median_family_income']
mfi = mfi.withColumnRenamed("total","mfi")
sqlContext.registerDataFrameAsTable(mfi, "mfi")

summary = sqlContext.sql('''
select
    gdp,gdp.year, --gdp
    g17,
    cci,
    retail,
    non_rev_cred,
    rev_cred,
    mfi
from gdp
left join g17 on g17.year = gdp.year
left join cci on cci.year = gdp.year
left join retail on retail.year = gdp.year
left join cons_credit on cons_credit.year = gdp.year
left join mfi on mfi.year = gdp.year
where gdp.year > 1999
''')
summary.show()

#write file
summary.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("/economic_health/summary.csv")

#write headers
with open('headers.txt', 'w') as f:
    for col in summary.columns:
        f.write(col)
        f.write(',')