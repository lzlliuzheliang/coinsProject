import sys
sys.path.append("..")
import db.mydatabase as database
from coinMetrics import mycoins
import datetime
import calendar

mc = mycoins.Mycoin()

mydb = database.Mydatabase()
# testdata = [1234, 0, 12]
# testdata2 = [0, 0, 12]
# md.insert_data("btc", mydb.DBSession, testdata)
# md.insert_data("btc", mydb.DBSession, testdata2)

# result = md.query_data("btc", "blockcount", mydb.DBSession, 0, 1235)
# print(result)

# res = md.get_max_timestamp(mydb.DBSession)
# print(res)


# to_do 
# if database is empty set a begin_timestamp, otherwise use the max_timestamp as begin_timestamp
begin_timestamp = database.get_max_timestamp(mydb.DBSession)+1
if begin_timestamp == 0:
	begin_timestamp = int(datetime.datetime(2009, 1, 1).timestamp())

begin_timestamp = 1547182800
# end_timestamp = 1547442000
# begin_timestamp = None
# end_timestamp = None
print(begin_timestamp)
# get data and store data to database

# btc_data = mc.get_all_data_types_for_asset('btc', begin_timestamp, end_timestamp)
btc_data = mc.get_all_data_types_for_asset('btc', begin_timestamp)
database.core_bulk_insert_data('btc', mydb.engine, btc_data)

# bch_data = mc.get_all_data_types_for_asset('bch', begin_timestamp, end_timestamp)
bch_data = mc.get_all_data_types_for_asset('bch', begin_timestamp)
database.core_bulk_insert_data('bch', mydb.engine, bch_data)

# ltc_data = mc.get_all_data_types_for_asset('ltc', begin_timestamp, end_timestamp)
ltc_data = mc.get_all_data_types_for_asset('ltc', begin_timestamp)
database.core_bulk_insert_data('ltc', mydb.engine, ltc_data)

# eth_data = mc.get_all_data_types_for_asset('eth', begin_timestamp, end_timestamp)
eth_data = mc.get_all_data_types_for_asset('eth', begin_timestamp)
database.core_bulk_insert_data('eth', mydb.engine, eth_data)

# etc_data = mc.get_all_data_types_for_asset('etc', begin_timestamp, end_timestamp)
etc_data = mc.get_all_data_types_for_asset('etc', begin_timestamp)
# for data in etc_data.values():
database.core_bulk_insert_data('etc', mydb.engine, etc_data)

# print(begin_timestamp)