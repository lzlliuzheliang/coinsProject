from django.shortcuts import render
from django.http import HttpResponse
import json
from coinmetrics import CoinMetricsAPI
from initdatabase import mydatabase
from django.views.decorators.csrf import csrf_exempt 
import datetime

# Create your views here.
cm = CoinMetricsAPI()
mydb = mydatabase.Mydatabase(False)
mycursor = mydb.mycursor

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
	if request.method == 'POST':
		asset = request.POST.get('asset')
		dataType = request.POST.get('dataType')
		startDate = request.POST.get('start')
		endDate = request.POST.get('end')
		dataType = dataType.replace("(", "_").replace(")", "")
		dataDict = {}
		alldata = []
		
		print(endDate)
		try:
			begin_timestamp = int(datetime.datetime(int(startDate[0:4]), int(startDate[5:7]), int(startDate[8:10])).timestamp())
			#end_timestamp = int(datetime.datetime(2009, 1, 2, 23, 59).timestamp())
			end_timestamp = int(datetime.datetime(int(endDate[0:4]), int(endDate[5:7]), int(endDate[8:10]),23, 59).timestamp())
			# alldata = cm.get_all_data_types_for_assets(str(asset), begin_timestamp, end_timestamp)[str(asset)]
		except:
			dataDict["status"] = 0;
			dataDict["data"] = alldata;
			dataDict["message"] = "Input date is invalid!"
			return HttpResponse(json.dumps(dataDict))

		sql = "SELECT timestamp," + str(dataType) + " FROM " + str(asset) + " WHERE timestamp BETWEEN " + str(begin_timestamp) + " AND " + str(end_timestamp);
		print(sql)
		mycursor.execute(sql)
		myresult = mycursor.fetchall()
		
		for x in myresult:
			tup = []
			tup.append(str(datetime.datetime.fromtimestamp(x[0])))
			tup.append(x[1])
			print(x)
			print(datetime.datetime.fromtimestamp(x[0]))
			alldata.append(tup)

		dataDict["status"] = 1;
		dataDict["data"] = alldata;

	return HttpResponse(json.dumps(alldata))
