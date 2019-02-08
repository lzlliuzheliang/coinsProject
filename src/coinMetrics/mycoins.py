import json
import requests
import datetime
import calendar

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class Mycoin:
	__API_URL_BASE = 'https://coinmetrics.io/api/v1/'

	def __init__(self, api_base_url = __API_URL_BASE):
		self.api_base_url = api_base_url
		self.request_timeout = 120

		self.session = requests.Session()
		retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[ 502, 503, 504 ])
		self.session.mount('http://', HTTPAdapter(max_retries=retries))



	def __request(self, url):
		try:
			response = self.session.get(url, timeout = self.request_timeout)
			response.raise_for_status()
			#if response.status_code == 200:
			content = json.loads(response.content.decode('utf-8'))
			if 'error' in content:
				raise ValueError(content['error'])
			else:
				return content
		except Exception as e:
			raise


	def get_available_data_types_for_asset(self, asset):
		api_url = '{0}get_available_data_types_for_asset/{1}'.format(self.api_base_url, asset)
		return self.__request(api_url)['result']

	def get_asset_data_for_time_range(self, asset,  data_type,  begin_timestamp = None,  end_timestamp = None):
		begin_timestamp, end_timestamp = self.__check_timestamp(begin_timestamp, end_timestamp) 		
		api_url = '{0}get_asset_data_for_time_range/{1}/{2}/{3}/{4}'.format(self.api_base_url, asset, data_type, begin_timestamp,  end_timestamp)		
		return self.__request(api_url)['result']

	def get_all_data_types_for_asset(self, asset, begin_timestamp = None,  end_timestamp = None):
		begin_timestamp, end_timestamp = self.__check_timestamp(begin_timestamp, end_timestamp) 
		data_types = self.get_available_data_types_for_asset(asset)
		r = {}
		for d in data_types:
			r[d] = self.get_asset_data_for_time_range(asset, d, begin_timestamp,  end_timestamp)
		# Save all the data in a dictionary. In the dictionary, the key is timestamp, the value is all the data of that timestamp
		# dictionary{timestamp1: [data1, data2, ...], ...}
		data_dict = {}
		index = 1
		for type in data_types:
			for data in r[type]:
				if data[1] == None or data[1] == 0:
					continue
				if data[0] not in data_dict:
					#make a new empty list
					ls = get_list(len(data_types))
					ls[0] = data[0]
					data_dict[data[0]] = ls
				data_dict[data[0]][index] = str(data[1])
			index+=1
		return data_dict

	def __check_timestamp(self, begin_timestamp, end_timestamp):
		today = datetime.datetime.utcnow()
		# if both timestamps empty (None) set whole date
		if not (begin_timestamp or end_timestamp):
			begin_timestamp = int(datetime.datetime(2009, 1, 1).timestamp())
			end_timestamp = calendar.timegm(today.replace(hour = 23, minute = 59, second = 59).timetuple())
		elif begin_timestamp and not end_timestamp:
			end_timestamp = calendar.timegm(today.replace(hour = 23, minute = 59, second = 59).timetuple())
		return begin_timestamp, end_timestamp

	def init_type_names(self):
		self.typedict = {}
		type_names = ["btc", "bch", "ltc", "eth", "etc"]
		for name in type_names:
			self.typedict[name] = self.get_available_data_types_for_asset(name)


def get_list(num):
	ls = ["0"]
	for i in range(num):
		ls.append("")
	return ls