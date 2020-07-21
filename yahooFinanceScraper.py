import re
from io import StringIO
from datetime import datetime, timedelta

import requests
import pandas as pd
import time


class YahooFinanceHistory:
	timeout = 2
	crumb_link = 'https://finance.yahoo.com/quote/{0}/history?p={0}'
	crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
	quote_link = 'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={dfrom}&period2={dto}&interval=1d&events=history&crumb={crumb}'

	def __init__(self, symbol, days_back=7):
		self.symbol = symbol
		self.session = requests.Session()
		self.dt = timedelta(days=days_back)

	#get cookies crumb
	def get_crumb(self):
		response = self.session.get(self.crumb_link.format(self.symbol), timeout=self.timeout)
		response.raise_for_status()
		match = re.search(self.crumble_regex, response.text)
		if not match:
			raise ValueError('Could not get crumb from Yahoo Finance')
		else:
			self.crumb = match.group(1)

	#download
	def get_quote(self):
		if not hasattr(self, 'crumb') or len(self.session.cookies) == 0:
			self.get_crumb()
		now = datetime.utcnow()
		dateto = int(now.timestamp())
		datefrom = int((now - self.dt).timestamp())
		url = self.quote_link.format(quote=self.symbol, dfrom=datefrom, dto=dateto, crumb=self.crumb)
		response = self.session.get(url)
		response.raise_for_status()
		return pd.read_csv(StringIO(response.text), parse_dates=['Date'])
#crate a loop !
#import list of the stock in LQ45 index
index_list = pd.read_csv('retry_list.csv', header=None)

#get ticker dataframe
index_list = index_list.loc[:,[1]]

days = 365*10
#days = 40

y = len(index_list.index)

begin_range = input("range_begin: ")
begin_range = int(begin_range)
end_range = input("end_begin: ")
end_range = int(end_range)
wait = input("wait_time: ")
wait = int(wait)

error = pd.DataFrame([])
#=================================================[Loop]=================================================#
o = pd.DataFrame([])
h = pd.DataFrame([])
l = pd.DataFrame([])
c = pd.DataFrame([])
v = pd.DataFrame([])




#o = pd.read_csv("o.csv")
#h = pd.read_csv("h.csv")
#l = pd.read_csv("l.csv")
#c = pd.read_csv("c.csv")
#v = pd.read_csv("v.csv")
##init csv
#o["Date"] = pd.to_datetime(o["Date"])
#o = o.set_index("Date")
##print (o.dtypes)
##init csv
#h["Date"] = pd.to_datetime(h["Date"])
#h = h.set_index("Date")
##print (h.dtypes)
##init csv
#l["Date"] = pd.to_datetime(l["Date"])
#l = l.set_index("Date")
##print (l.dtypes)
##init csv
#c["Date"] = pd.to_datetime(c["Date"])
#c = c.set_index("Date")
##print (c.dtypes)
##init csv
#v["Date"] = pd.to_datetime(v["Date"])
#v = v.set_index("Date")
##print (c.dtypes)

for x in range(begin_range,end_range):


	try:
		ticker = index_list.at[x,1]
		df = YahooFinanceHistory(ticker+".JK", days_back=days).get_quote()

	except ValueError:

		print(x,"retry ValueError")
		err2 = [index_list.at[x,1],["ValueError"]]
		err2 = pd.DataFrame(err2)
		err2 = err2.T
		error = pd.concat([error,err2])
		error.to_csv("error_list.csv")

		continue

	except requests.exceptions.HTTPError:

		print(x,"retry requests.exceptions.HTTPError")
		err2 = [index_list.at[x,1],["HTTPError"]]
		err2 = pd.DataFrame(err2)
		err2 = err2.T
		error = pd.concat([error,err2])
		error.to_csv("error_list.csv")

		continue

	except requests.exceptions.ReadTimeout:
		print(x,"retry requests.exceptions.ReadTimeout")
		err2 = [index_list.at[x,1],["ReadTimeout"]]
		err2 = pd.DataFrame(err2)
		err2 = err2.T
		error = pd.concat([error,err2])
		error.to_csv("error_list.csv")
		continue

	except requests.exceptions.ConnectionError:
		print(x,"retry requests.exceptions.ConnectionError")
		err2 = [index_list.at[x,1],["ConnectionError"]]
		err2 = pd.DataFrame(err2)
		err2 = err2.T
		error = pd.concat([error,err2])
		error.to_csv("error_list.csv")
		continue


	#rename columns
	df.rename(columns={"Open":ticker+".O"},inplace=True)
	df.rename(columns={"High":ticker+".H"},inplace=True)
	df.rename(columns={"Low":ticker+".L"},inplace=True)
	df.rename(columns={"Close":ticker+".C"},inplace=True)
	df.rename(columns={"Volume":ticker+".V"},inplace=True)

	#create date index
	df1 = df.loc[:,["Date",ticker+".O"]]
	df1 = df1.set_index("Date")

	df2 = df.loc[:,["Date",ticker+".H"]]
	df2 = df2.set_index("Date")

	df3 = df.loc[:,["Date",ticker+".L"]]
	df3 = df3.set_index("Date")

	df4 = df.loc[:,["Date",ticker+".C"]]
	df4 = df4.set_index("Date")

	df5 = df.loc[:,["Date",ticker+".V"]]
	df5 = df5.set_index("Date")

	#create file
	o = pd.concat([o, df1],axis=1, join='outer')
	o.to_csv('o.csv')

	h = pd.concat([h, df2],axis=1, join='outer')
	h.to_csv('h.csv')

	l = pd.concat([l, df3],axis=1, join='outer')
	l.to_csv('l.csv')

	c = pd.concat([c, df4],axis=1, join='outer')
	c.to_csv('c.csv')

	v = pd.concat([v, df5],axis=1, join='outer')
	v.to_csv('v.csv')


	print (x," done")
	time.sleep(wait)

#get ticker
#ticker = index_list.at[2,1]

#df = YahooFinanceHistory(ticker+".JK", days_back=days).get_quote()
#print(df)


#print(index_list)

#for x in index_list:
#	print (index_list.at[2,1])

"""
for X in
#example
df = YahooFinanceHistory('UNVR.JK', days_back=365*4).get_quote()
time.sleep(10)
df2 = YahooFinanceHistory('UNTR.JK', days_back=365*4).get_quote()
df.rename(columns={"Close":"UNVR.JK"},inplace=True)
df = df.loc[:,['Date','UNVR.JK']]
df2 = df2.loc[:,['Close']]
df_r = pd.concat([df,df2],axis=1)
df_r.to_csv('test.csv',index=False)
print (df_r)

"""
