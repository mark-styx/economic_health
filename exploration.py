import pandas as pd

### Consumer Confidence ###
### ___________________ ###
cci = pd.read_csv('consumer_confidence_index.csv')
cci[cci['TIME']=='1973-01']
cci = None
#contains multiple countries, will narrow scope to USA location code


### GDP ###
### ___ ###
cd_gdp = pd.read_excel('current_dollar_gdp.xlsx')
#formatting is rough, convert to csv and yank out the empty rows
#specify headers
cd_gdp_headers = ['year','gdp_in_billions_of_current_dollars','gdp_in_billions_of_2012_dollars','blank','year_quarter','gdp_in_billions_of_current_dollars2','gdp_in_billions_of_2012_dollars2','blank2']
cd_gdp.columns = cd_gdp_headers
cd_gdp.head()
cd_gdp = None

#split into separate dataframes
gdp_by_year = cd_gdp[['year','gdp_in_billions_of_current_dollars','gdp_in_billions_of_2012_dollars']][7:]
gdp_by_quarter = cd_gdp[['year_quarter','gdp_in_billions_of_current_dollars2','gdp_in_billions_of_2012_dollars2']][7:]

#drop na's
gdp_by_year.dropna(inplace=True)
gdp_by_year.shape
gdp_by_quarter.dropna(inplace=True)
gdp_by_quarter.shape

#function to clean gdp file
def gdp():
    cd_gdp = pd.read_excel('current_dollar_gdp.xlsx')
    #specify headers
    cd_gdp_headers = ['year','gdp_in_billions_of_current_dollars','gdp_in_billions_of_2012_dollars','blank','year_quarter','gdp_in_billions_of_current_dollars2','gdp_in_billions_of_2012_dollars2','blank2']
    cd_gdp.columns = cd_gdp_headers
    #split into separate dataframes
    gdp_by_year = cd_gdp[['year','gdp_in_billions_of_current_dollars','gdp_in_billions_of_2012_dollars']][7:]
    gdp_by_quarter = cd_gdp[['year_quarter','gdp_in_billions_of_current_dollars2','gdp_in_billions_of_2012_dollars2']][7:]
    #drop na's
    gdp_by_year.dropna(inplace=True)
    gdp_by_quarter.dropna(inplace=True)
    #dump to csv
    gdp_by_quarter.to_csv('gdp_by_quarter.csv',index=False)
    gdp_by_year.to_csv('gdp_by_year.csv',index=False)
    return


### Industrial Production and Capacity Utilization ###
### ______________________________________________ ###
g17 = pd.read_csv('g17_industrial_production_and_cap_utilization.csv')
g17.head()
g17 = None
#pivoted with quarters as the headers


### Consumer Credit Outstanding ###
### ___________________________ ###
g19 = pd.read_csv('g19_consumer_credit_outst.csv')
g19.head()
#yikes, messy
multiplier = g19.loc[1][1:].tolist()
multiplier
g19 = g19[7:]
g19.dtypes
for i,col in enumerate(g19.columns[1:]):
    g19[col] = pd.to_numeric(g19[col],errors='coerce')
    g19[col] = g19[col] * int(multiplier[i])
g19.to_csv('g19_consumer_credit_outst.csv',index=False)
g19 = None


### Median Family Income Report ###
### ___________________________ ###
med_fam_income = pd.read_excel('med_family_income_report.xls')
med_fam_income.head()
#doesn't only really contains one useful year of data, need to collect the rest and summarize
med_fam_income[[med_fam_income.columns[-1]]][1:].sum().values[0]
med_fam_income[[med_fam_income.columns[-1]]][0:1].values[0][0][:4]

#function to grab all median family income files and summarize
from glob import glob
def summarize_med_family_inc():
    mfi = {
        'year':[],
        'total':[]
    }
    files = glob('*med_family*')
    for f in files:
        df = pd.read_excel(f)
        total = df[[df.columns[-1]]][1:].sum().values[0]
        year = df[[df.columns[-1]]][0:1].values[0][0][:4]
        mfi['year'].append(year)
        mfi['total'].append(total)
    mfi = pd.DataFrame(mfi,columns=['year','total'])
    mfi.to_csv('median_family_income.csv',index=False)
    return


### Monthly Retail Trade Report ###
### ___________________________ ###
monthly_ret_trade = pd.read_excel('monthly_retail_trade_report.xls')
monthly_ret_trade.head()

#create function to split up the excel formatting
def retail_trade():
    columns=['business','january','february','march','april',
        'may','jun','july','august','september','october','november','december']
    df = pd.DataFrame(columns=columns)
    start_year = 1992
    for i in range(2020-start_year):
        year = str(start_year + i)
        ndf = pd.read_excel('monthly_retail_trade_report.xls',sheet_name=year)
        ndf = ndf[ndf.columns[1:14]][7:]
        ndf.dropna(inplace=True)
        ndf.columns = columns
        for i,col in enumerate(ndf.columns[1:]):
            ndf[col] = pd.to_numeric(ndf[col],errors='coerce')
        ndf['year'] = year
        df = df.append(ndf)
        ndf = None
    df.dropna(inplace=True)
    df.to_csv('retail_trade.csv',index=False)
    return