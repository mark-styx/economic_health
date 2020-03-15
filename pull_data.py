import requests as req
import pandas as pd
from glob import glob
import os

#execution timer
def ex_time(func):
    def execution_time(*args,**kw):
        start = pd.datetime.now()
        execution = func(*args,**kw)
        end = pd.datetime.now()
        print('''execution time: {0}
        '''.format(end-start))
        return execution
    return execution_time


class retrieve_data():

    data = {
        'g17_industrial_production_and_cap_utilization' : {
            'url':'https://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&series=1aa8d31f88df776eb8dc991ce0b8b974&lastobs=100&from=&to=&filetype=csv&label=include&layout=seriesrow',
            'output':'g17_industrial_production_and_cap_utilization.csv'
            },
        'g19_consumer_credit_outst' : {
            'url':'https://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&series=47b3133fcba3957706678b2a55cb5a97&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package',
            'output':'g19_consumer_credit_outst.csv'
            },
        'monthly_retail_trade_report' : {
            'url':'https://www.census.gov/retail/mrts/www/mrtssales92-present.xls',
            'output':'monthly_retail_trade_report.xls'
            },
        'consumer_confidence_index' : {
            'url':'https://stats.oecd.org/sdmx-json/data/DP_LIVE/.CCI.../OECD?contentType=csv&detail=code&separator=comma&csv-lang=en',
            'output':'consumer_confidence_index.csv'
            },
        'current_dollar_gdp' : {
            'url':'https://www.bea.gov/system/files/2020-02/gdplev.xlsx',
            'output':'current_dollar_gdp.xlsx'
            }
    }

    @ex_time
    def download_data(self,url,output):
        print('downloading {0}...'.format(url))
        response = req.get(url)
        print('done.')
        print('saving output {0}...'.format(output))
        with open(output,'wb') as f:
            f.write(response.content)
        print('done.')
        return

    def download_med_fam_income_data(self):
        #available range 1998-2019
        start_year = 1998
        for i in range(2020-start_year):
            year = str(start_year + i)[-2:]
            url = 'https://www.ffiec.gov/xls/msa{0}inc.xls'.format(year)
            output = 'med_family_income_report{0}.xls'.format(year)
            self.download_data(url,output)

    def __init__(self):
        data = self.data
        for item in data:
            self.download_data(data[item]['url'],data[item]['output'])
        self.download_med_fam_income_data()

@ex_time
class preprocessing():
    
    #function to clean gdp file
    def gdp(self):
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
        os.remove('current_dollar_gdp.xlsx')
        return
    
    #clean up consumer credit outstanding
    def consumer_credit(self):
        g19 = pd.read_csv('g19_consumer_credit_outst.csv')
        multiplier = g19.loc[1][1:].tolist()
        g19 = g19[7:]
        for i,col in enumerate(g19.columns[1:]):
            g19[col] = pd.to_numeric(g19[col],errors='coerce')
            g19[col] = g19[col] * int(multiplier[i])
        g19.to_csv('consumer_credit.csv',index=False)
        os.remove('g19_consumer_credit_outst.csv')
        return

    #function to grab all median family income files and summarize
    def summarize_med_family_inc(self):
        mfi = {
            'year':[],
            'total':[]
        }
        files = glob('*med_family*')
        for f in files:
            try:
                df = pd.read_excel(f)
                total = df[[df.columns[-1]]][1:].sum().values[0]
                year = df[[df.columns[-1]]][0:1].values[0][0][:4]
                if year == 'HUD ':
                    year = df[[df.columns[-1]]][0:1].values[0][0][14:18]
                mfi['year'].append(year)
                mfi['total'].append(total)
                os.remove(f)
            except:
                pass
        mfi = pd.DataFrame(mfi,columns=['year','total'])
        mfi.to_csv('median_family_income.csv',index=False)
        return

    #create function to split up the excel formatting
    def retail_trade(self):
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
        os.remove('monthly_retail_trade_report.xls')
        return
    
    def __init__(self):
        print('cleaning up files...')
        self.gdp()
        self.consumer_credit()
        self.summarize_med_family_inc()
        self.retail_trade()
        print('done.')



if __name__ == "__main__":
    @ex_time
    def run():
        print('processing...')
        retrieve_data()
        preprocessing()
        print('run complete.')
    run()