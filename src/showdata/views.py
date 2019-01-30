from django.shortcuts import render
from django.http import HttpResponse
import json
from db import mydatabase as database
from coinMetrics import mycoins
from django.views.decorators.csrf import csrf_exempt 
import datetime
import calendar

# Create your views here.
cm = mycoins.Mycoin()
mydb = database.Mydatabase()


def index(request):
	return HttpResponse("Hello, world. You're at the showdata index.")

def show(request):
	types_btc = cm.get_available_data_types_for_asset('btc')
	types_bch = cm.get_available_data_types_for_asset('bch')
	types_ltc = cm.get_available_data_types_for_asset('ltc')
	types_eth = cm.get_available_data_types_for_asset('eth')
	types_etc = cm.get_available_data_types_for_asset('etc')

	# print(type(types_btc))
	cointypes = {
		"btc": types_btc,
		"bch": types_bch,
		"ltc": types_ltc,
		"eth": types_eth,
		"etc": types_etc
	}
	return render(request, 'showdata/index.html', {'cointypes': json.dumps(cointypes)})

#updata
@csrf_exempt
def updateview(request):
	alldata=[]
	if request.method == 'POST':
		# get all data from post request
		asset = request.POST.get('asset')
		dataType = request.POST.get('dataType')
		startDate = request.POST.get('start')
		endDate = request.POST.get('end')
		dataType = dataType.replace("(", "_").replace(")", "")
		dataDict = {}
		
		print(endDate)

		try:
			begin_timestamp = calendar.timegm(datetime.datetime(int(startDate[0:4]), int(startDate[5:7]), int(startDate[8:10]), 00, 00).timetuple())
			end_timestamp = calendar.timegm(datetime.datetime(int(endDate[0:4]), int(endDate[5:7]), int(endDate[8:10]),23, 59).timetuple())
		except:
			dataDict["status"] = 0;
			dataDict["data"] = alldata;
			dataDict["message"] = "Input date is invalid!"
			return HttpResponse(json.dumps(dataDict))
		# Query data from database
		result = database.core_query_data(asset, dataType, mydb.engine, begin_timestamp, end_timestamp)
		for x in result:
			tup=[]
			tup.append(str(datetime.datetime.utcfromtimestamp(x[0])))
			tup.append(x[1])
			alldata.append(tup)

		return HttpResponse(json.dumps(alldata))

		# Old code
		# sql = "SELECT timestamp," + str(dataType) + " FROM " + str(asset) + " WHERE timestamp BETWEEN " + str(begin_timestamp) + " AND " + str(end_timestamp);
		# print(sql)
		# mycursor.execute(sql)
		# myresult = mycursor.fetchall()
		
		# for x in myresult:
		# 	tup = []
		# 	tup.append(str(datetime.datetime.utcfromtimestamp(x[0])))
		# 	tup.append(x[1])
		# 	print(x)
		# 	print(datetime.datetime.utcfromtimestamp(x[0]))
		# 	alldata.append(tup)

		# dataDict["status"] = 1;
		# dataDict["data"] = alldata;

	return HttpResponse(json.dumps(alldata))
