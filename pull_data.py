import requests as req
import pandas as pd

data = {
    'g17_industrial_production_and_cap_utilization' : {
        'url':'https://www.federalreserve.gov/datadownload/Output.aspx?rel=G17&series=1aa8d31f88df776eb8dc991ce0b8b974&lastobs=100&from=&to=&filetype=csv&label=include&layout=seriesrow',
        'output':'g17_industrial_production_and_cap_utilization.csv'
        },
    'g19_consumer_credit_outst' : {
        'url':'https://www.federalreserve.gov/datadownload/Output.aspx?rel=G19&series=47b3133fcba3957706678b2a55cb5a97&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package',
        'output':'g19_consumer_credit_outst.csv'
        },
    'med_family_income_report' : {
        'url':'https://www.ffiec.gov/xls/msa19inc.xls', #19 is the year total available range 1998-2019
        'output':'med_family_income_report.xls'
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

def ex_time(func):
    def execution_time(*args,**kw):
        start = pd.datetime.now()
        execution = func(*args,**kw)
        end = pd.datetime.now()
        print('''execution time: {0}
        '''.format(end-start))
        return execution
    return execution_time

@ex_time
def download_data(url,output):
    print('downloading {0}...'.format(url))
    response = req.get(url)
    print('done.')
    print('saving output {0}...'.format(output))
    with open(output,'wb') as f:
        f.write(response.content)
    print('done.')
    return

if __name__ == "__main__":
    for item in data:
        download_data(data[item]['url'],data[item]['output'])