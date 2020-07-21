import re
from io import StringIO
from datetime import datetime, timedelta

import requests
import pandas as pd
import time


class YahooFinanceHistory:

	"""I got this from stack overflow T_T"""

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

class downloadLoop:
	"""docstring for downloadLoop."""

	def __init__(self):
		pass

	def loopArray(self, begin_range, end_range, days, wait):

		o = pd.DataFrame([])
		h = pd.DataFrame([])
		l = pd.DataFrame([])
		c = pd.DataFrame([])
		v = pd.DataFrame([])
		error = pd.DataFrame([])

		dfList = [o,h,l,c,v]

		for each in range(begin_range, end_range):

				try:
					#call class for downloading
					ticker = index_list.at[each,1]
					df = YahooFinanceHistory(ticker+".JK", days_back=days).get_quote()

				#exeptions just in case error
				except ValueError:

					print(x,"retry ValueError")
					err2 = [index_list.at[each,1],["ValueError"]]
					err2 = pd.DataFrame(err2)
					err2 = err2.T
					error = pd.concat([error,err2])
					error.to_csv("error_list.csv")

					continue

				except requests.exceptions.HTTPError:

					print(x,"retry requests.exceptions.HTTPError")
					err2 = [index_list.at[each,1],["HTTPError"]]
					err2 = pd.DataFrame(err2)
					err2 = err2.T
					error = pd.concat([error,err2])
					error.to_csv("error_list.csv")

					continue

				except requests.exceptions.ReadTimeout:
					print(x,"retry requests.exceptions.ReadTimeout")
					err2 = [index_list.at[each,1],["ReadTimeout"]]
					err2 = pd.DataFrame(err2)
					err2 = err2.T
					error = pd.concat([error,err2])
					error.to_csv("error_list.csv")
					continue

				except requests.exceptions.ConnectionError:
					print(x,"retry requests.exceptions.ConnectionError")
					err2 = [index_list.at[each,1],["ConnectionError"]]
					err2 = pd.DataFrame(err2)
					err2 = err2.T
					error = pd.concat([error,err2])
					error.to_csv("error_list.csv")
					continue

				#rename header
				col_dict = {
				"Open":ticker+".O",
				"High":ticker+".H",
				"Low":ticker+".L",
				"Close":ticker+".C",
				"Volume":ticker+".V"
				}
				df.rename(columns=col_dict,inplace=True)

				#create csv
				typeList = [
				ticker+".O",
				ticker+".H",
				ticker+".L",
				ticker+".C",
				ticker+".V",
				]


				dfListNameFile = ["o","h","l","c","v"]
				x = 0

				#create a file for each sucessive download (not so fast but reliable just incase)
				#todo create togleable refresh / download / bulk create
				for type in typeList:

					dfAppend = df.loc[:,["Date",type]]
					dfAppend = dfAppend.set_index("Date")

					dfList[x] = pd.concat([dfList[x], dfAppend],axis=1, join='outer')
					dfList[x].to_csv(dfListNameFile[x] + '.csv')
					x = x + 1

					pass

				print (each, " ", ticker, " "," done")
				time.sleep(wait)
				pass

		pass


#=================================================[execute :P]=================================================#
index_list = pd.read_csv('ticker.csv', header=None)

#get ticker dataframe
index_list = index_list.loc[:,[1]]

days = 365*10

y = len(index_list.index)

begin_range = input("range_begin: ")
begin_range = int(begin_range)
end_range = input("end_begin: ")
end_range = int(end_range)
wait = input("wait_time: ")
wait = int(wait)

looper = downloadLoop().loopArray(begin_range,end_range,days,wait)
