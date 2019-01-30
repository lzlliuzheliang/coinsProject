from sqlalchemy import *
import sys
sys.path.append("..")
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import coinMetrics.mycoins
from sqlalchemy.sql import select

mc = coinMetrics.mycoins.Mycoin()

CONFIG = {
	"host": "localhost",
	"user": "root",
	"passwd": "zheliang415",
	"database": "mytest"
}

default_url = "mysql://"+CONFIG['user']+":"+CONFIG['passwd']+"@"+CONFIG['host']
db_url = default_url + "/"+CONFIG['database']

Base = declarative_base()
class Mydatabase:
	def __init__(self):
		self.engine = get_database_engine(False)
		self.DBSession = sessionmaker(bind=self.engine)
		


def get_database_engine(rebuild_database = True):
	engine = create_engine(default_url, echo = False)
	if rebuild_database:
		try:
			drop_sql="DROP DATABASE "+CONFIG['database']
			engine.execute(drop_sql)
		except:
			print("Can't drop DATABASE "+CONFIG['database']+", it NOT exists")
	try:
		create_sql="CREATE DATABASE "+CONFIG["database"]
		engine.execute(create_sql)
		# engine.execute("USE mytest")
	except Exception as e:
		print("Database exists!")
		engine = create_engine(db_url, echo = False)
		create_tables(engine)
		return engine

	# new Database, create tables
	engine = create_engine(db_url, echo = False)
	# Base = declarative_base()
	create_tables(engine)
	return engine


# engine = get_database_engine(True)
# Base = declarative_base()
# DBSession = sessionmaker(bind=engine)
# session = DBSession()

# Define Class
class btc(Base):
    __tablename__ = 'btc'
    timestamp=Column(Integer,primary_key=True, autoincrement=False)

class bch(Base):
    __tablename__ = 'bch'
    timestamp=Column(Integer,primary_key=True, autoincrement=False)

class ltc(Base):
    __tablename__ = 'ltc'
    timestamp=Column(Integer,primary_key=True, autoincrement=False)

class eth(Base):
    __tablename__ = 'eth'
    timestamp=Column(Integer,primary_key=True, autoincrement=False)

class etc(Base):
    __tablename__ = 'etc'
    timestamp=Column(Integer,primary_key=True, autoincrement=False)


def create_tables(engine):
	#btc
	for type in mc.get_available_data_types_for_asset("btc"):
		type = type.replace("(", "_")
		type = type.replace(")", "")
		setattr(btc,str(type),(Column(str(type), String(255))))

	#bch
	for type in mc.get_available_data_types_for_asset("bch"):
		type = type.replace("(", "_")
		type = type.replace(")", "")
		setattr(bch,str(type),(Column(str(type), String(255))))

	#ltc
	for type in mc.get_available_data_types_for_asset("ltc"):
		type = type.replace("(", "_")
		type = type.replace(")", "")
		setattr(ltc,str(type),(Column(str(type), String(255))))
	#eth
	for type in mc.get_available_data_types_for_asset("eth"):
		type = type.replace("(", "_")
		type = type.replace(")", "")
		setattr(eth,str(type),(Column(str(type), String(255))))

	#etc
	for type in mc.get_available_data_types_for_asset("etc"):
		type = type.replace("(", "_")
		type = type.replace(")", "")
		setattr(etc,str(type),(Column(str(type), String(255))))

	Base.metadata.create_all(bind=engine)



def orm_insert_data(table_name, DBSession, all_data):
	print(data)
	session = DBSession()
	switcher = {
		'btc': btc(),
		'bch': bch(),
		'ltc': ltc(),
		'eth': eth(),
		'etc': etc(),
	}
	for data in all_data.values():
		new_tuple = switcher.get(table_name)
		new_tuple.timestamp = data[0]
		index = 1
		for type in mc.get_available_data_types_for_asset(table_name):
			# new_tuple[type] = data[index]
			# new_tuple[str(type)] = "nothing"
			type = type.replace("(", "_")
			type = type.replace(")", "")
			setattr(new_tuple, str(type), str(data[index]))
			# print(getattr(new_tuple, str(type)))
			index+=1

		session.add(new_tuple)
		session.commit()		

	session.close()

def core_bulk_insert_data(table_name, engine, all_data):
	table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
	data_list = []
	conn = engine.connect()
	for data in all_data.values():
		tuple_dict = {}
		tuple_dict["timestamp"] = data[0]
		index = 1
		for type in mc.get_available_data_types_for_asset(table_name):
			type = type.replace("(", "_")
			type = type.replace(")", "")
			tuple_dict[type] = data[index]
			index+=1
		data_list.append(tuple_dict)
		print(str(tuple_dict))

	try:
		conn.execute(table.insert(), data_list)
	except Exception as e:
		print(e)

	conn.close()

def core_single_insert_data(table_name, engine, all_data):
	table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
	conn = engine.connect()
	for data in all_data.values():
		tuple_dict = {}
		tuple_dict["timestamp"] = data[0]
		index = 1
		for type in mc.get_available_data_types_for_asset(table_name):
			type = type.replace("(", "_")
			type = type.replace(")", "")
			tuple_dict[type] = data[index]
			index+=1
		try:
			conn.execute(table.insert(), tuple_dict)
		except Exception as e:
			print(e)

	conn.close()
	
def core_query_data(table_name, datatype, engine, begin_timestamp,  end_timestamp):
	table = Table(table_name, Base.metadata, autoload=True, autoload_with=engine)
	print(table.c)
	conn = engine.connect()
	print("time stampes:")
	print(begin_timestamp)
	print(end_timestamp)
	s = select([table]).where(table.c.timestamp.between(int(begin_timestamp), int(end_timestamp)))
	# s = select([table])
	result = conn.execute(s)
	rows = result.fetchall()
	print(result)
	alldata = []
	for row in rows:
		print(row)
		data=[]
		data.append(row['timestamp'])
		data.append(row[str(datatype)])
		alldata.append(data)

	conn.close()
	return alldata


def query_data(table_name, datatype, DBSession, begin_timestamp, end_timestamp):
	session = DBSession()
	switcher = {
		'btc': session.query(btc).filter(btc.timestamp.between(begin_timestamp, end_timestamp)),
		'bch': session.query(bch).filter(bch.timestamp.between(begin_timestamp, end_timestamp)),
		'ltc': session.query(ltc).filter(ltc.timestamp.between(begin_timestamp, end_timestamp)),
		'eth': session.query(eth).filter(eth.timestamp.between(begin_timestamp, end_timestamp)),
		'etc': session.query(etc).filter(etc.timestamp.between(begin_timestamp, end_timestamp)),
	}
	tuples = switcher.get(table_name)
	data = {}
	for t in tuples:
		data[t.timestamp] = getattr(t, datatype)

	session.close()
	return data

def get_max_timestamp(DBSession):
	session = DBSession()
	btc_max = session.query(func.max(btc.timestamp).label("max")).one().max
	bch_max = session.query(func.max(bch.timestamp).label("max")).one().max
	ltc_max = session.query(func.max(ltc.timestamp).label("max")).one().max
	etc_max = session.query(func.max(etc.timestamp).label("max")).one().max
	eth_max = session.query(func.max(eth.timestamp).label("max")).one().max
	try:
		result = min(btc_max, bch_max, ltc_max, etc_max, eth_max)
	except:
		session.close()
		return -1
	session.close()
	return int(result)
