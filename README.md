# coinsProject
This is an assignment for Athanor interview

### 1. Install modules with requirements.txt
```
pip install -r requirements.txt

```
### 3. Modify database setting for your mysql database in /coinsProject/src/db/mydatabase.py

Here is an example:
```
CONFIG = {
	"host": "localhost",
  	"user": "root",
 	"passwd": "zheliang415",
 	"database": "coindb"
}
```

### 4. Run /initdatabase/app.py
```
python app.py
```

### 5. Start the django server in /coinsProject/src/ folder (for test)
```
python manage.py runserver
```

### 6. Open http://127.0.0.1:8000/

### 7. My environment
python version: 3.7.0
pip version: 19.0.1
