import mydatabase

mydb = mydatabase.Mydatabase(True)
mycursor = mydb.mycursor


mydatabase.createtable("btc", mycursor)
mydatabase.store_data_for_asset("btc", mydatabase.get_history_data_for_asset("btc"), mydb, mycursor)

mydatabase.createtable("bch", mycursor)
mydatabase.store_data_for_asset("bch", mydatabase.get_history_data_for_asset("bch"), mydb, mycursor)

mydatabase.createtable("etc", mycursor)
mydatabase.store_data_for_asset("etc", mydatabase.get_history_data_for_asset("etc"), mydb, mycursor)

mydatabase.createtable("eth", mycursor)
mydatabase.store_data_for_asset("eth", mydatabase.get_history_data_for_asset("eth"), mydb, mycursor)

mydatabase.createtable("ltc", mycursor)
mydatabase.store_data_for_asset("ltc", mydatabase.get_history_data_for_asset("ltc"), mydb, mycursor)

