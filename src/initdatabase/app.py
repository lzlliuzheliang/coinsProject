import sys
sys.path.append("..")
import db.mydatabase as database
from coinMetrics import mycoins
import datetime
import calendar

mc = mycoins.Mycoin()

mydb = database.Mydatabase()

# to_do 
# if database is empty set a begin_timestamp, otherwise use the max_timestamp as begin_timestamp
begin_timestamp = database.get_max_timestamp(mydb.DBSession)+1

first_run = False
if begin_timestamp == 0:
	first_run = True
	begin_timestamp = int(datetime.datetime(2009, 1, 1).timestamp())

print("Begin timestamp: " + begin_timestamp)

btc_data = mc.get_all_data_types_for_asset('btc', begin_timestamp)
bch_data = mc.get_all_data_types_for_asset('bch', begin_timestamp)
ltc_data = mc.get_all_data_types_for_asset('ltc', begin_timestamp)
eth_data = mc.get_all_data_types_for_asset('eth', begin_timestamp)
etc_data = mc.get_all_data_types_for_asset('etc', begin_timestamp)

if first_run:
	database.core_bulk_insert_data('btc', mydb.engine, btc_data)
	database.core_bulk_insert_data('bch', mydb.engine, bch_data)
	database.core_bulk_insert_data('ltc', mydb.engine, ltc_data)
	database.core_bulk_insert_data('eth', mydb.engine, eth_data)
	database.core_bulk_insert_data('etc', mydb.engine, etc_data)
else:
	database.core_single_insert_data('btc', mydb.engine, btc_data)
	database.core_single_insert_data('bch', mydb.engine, bch_data)
	database.core_single_insert_data('ltc', mydb.engine, ltc_data)
	database.core_single_insert_data('eth', mydb.engine, eth_data)
	database.core_single_insert_data('etc', mydb.engine, etc_data)

# eth_data = mc.get_all_data_types_for_asset('eth', begin_timestamp, end_timestamp)



# etc_data = mc.get_all_data_types_for_asset('etc', begin_timestamp, end_timestamp)

# for data in etc_data.values():



