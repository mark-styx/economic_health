import pandas as pd

#get headers
with open('headers.txt','r') as f:
    headers = f.read()
headers = headers.split(',')
headers = [x for x in headers if x != '']

#load data
df = pd.read_csv('summary.csv',names=headers)

#normalize data types
for field in df.columns:
    df[field] = pd.to_numeric(df[field],errors='coerce')

#check out the data
import matplotlib.pyplot as plt
import seaborn as sns

def plotvsyear(field):
    if field == 'year':
        return
    plt.plot(df['year'],df[field])
    plt.xlabel('year')
    plt.ylabel('field')
    plt.title('{0} by year'.format(field))
    plt.show()
    return

#plot by year
for field in df.columns:
    plotvsyear(field)

##the scaling is off, which will make it hard to visualize the differences
##we'll take the z score to evaluate

def zscores(field):
    if field == 'year':
        return
    mean = df[field].mean()
    std = df[field].std()
    xvalues = df[field].tolist()
    zscore = []
    for x in xvalues:
        zscore.append((x-mean)/std)
    new_field = str(field) + ' zscore'
    df[new_field] = zscore
    return

#generate zscores
for field in df.columns:
    zscores(field)

#plot zscores by year
for field in df.columns:
    if field.find('zscore') != -1:
        plotvsyear(field)

zscores = [x for x in df.columns if x.find('zscore') != -1]

#correlations
corr = df[zscores].corr(method='pearson')
sns.heatmap(corr)

#linear regression
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

y = df['gdp'].values.reshape(-1,1)
X = df['year'].values.reshape(-1,1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)
reg = LinearRegression()
gdp_pred = reg.fit(X_train,y_train)
results = gdp_pred.predict(X_test)
print('''
actual: {0}
predict: {1}'''.format(y_test,results))

#next five years gdp
next_five_years = pd.Series([2020,2021,2022,2023,2024]).values.reshape(-1,1)
n5 = gdp_pred.predict(next_five_years)
plt.plot(next_five_years,n5)
plt.xlabel('year')
plt.ylabel('gdp')
plt.title('predicted gdp')
plt.show()

#trend vs actual
trend = gdp_pred.predict(X)
df['gdp_trend'] = trend
plt.plot(df['year'],df[['gdp','gdp_trend']])
plt.xlabel('year')
plt.ylabel('gdp')
plt.title('gdp by year')
plt.legend(['gdp','gdp_trend'])
plt.show()