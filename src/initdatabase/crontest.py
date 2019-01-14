import mydatabase

mydb = mydatabase.Mydatabase(False)
mycursor = mydb.mycursor

try:
	mydatabase.store_data_for_asset("btc", mydatabase.get_yesterday_data_for_asset("ltc"), mydb, mycursor)
except Exception as e:
	print(str(e))

try:
	mydatabase.store_data_for_asset("bch", mydatabase.get_yesterday_data_for_asset("ltc"), mydb, mycursor)
except Exception as e:
	print(str(e))

try:
	mydatabase.store_data_for_asset("ltc", mydatabase.get_yesterday_data_for_asset("ltc"), mydb, mycursor)
except Exception as e:
	print(str(e))

try:
	mydatabase.store_data_for_asset("eth", mydatabase.get_yesterday_data_for_asset("ltc"), mydb, mycursor)
except Exception as e:
	print(str(e))
try:
	mydatabase.store_data_for_asset("etc", mydatabase.get_yesterday_data_for_asset("ltc"), mydb, mycursor)
except Exception as e:
	print(str(e))