# coinsProject
This is an assignment for Athanor interview
### 1. Install coin metrics python wrapper in the coinsProject folder， here is the link: https://github.com/man-c/coinmetrics. (In the /coinmetrics/setup.py file, replace "from distutils.core import setup" with "from setuptools import setup")


### 2. Install modules 
```
pip install mysql-connector-python-rf
pip install django
pip install mysqlclient
```
### 3. Modify database setting for your mysql database in /coinsProject/src/initdatabase/mydatabase.py

Here is an example:
```
CONFIG = {
	"host": "localhost",
  	"user": "root",
 	"passwd": "zheliang415",
 	"database": "coindb"
}
```

### 4. Run /initdatabase/initial.py
```
python initial.py
```

### 5. Start the django server in /coinsProject/src/ folder (for test)
```
python manage.py runserver
```

### 6. Open http://127.0.0.1:8000/

### 7. You can run /coinsProject/initdatabase/crontest.py in a cron way to maintain the data

### 8. My virtualenv for reference (run pip freeze):

atomicwrites==1.2.1
attrs==18.2.0
certifi==2018.11.29
chardet==3.0.4
coinmetrics==0.0.1
colorama==0.4.1
Django==2.1.5
idna==2.8
more-itertools==5.0.0
mysql-connector-python-rf==2.2.2
mysqlclient==1.3.14
pluggy==0.8.1
py==1.7.0
pytest==4.1.1
pytz==2018.9
requests==2.21.0
responses==0.10.5
six==1.12.0
urllib3==1.24.1
