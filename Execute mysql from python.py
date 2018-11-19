#execute SQL from python
import mysql.connector
import csv
import re
import os
import ntpath

mysql_password = os.environ['MysqlPassword']
db= mysql.connector.connect ( user='root' , password=mysql_password ,
                                host='127.0.0.1' ,
                                database='import' ,
                                charset='utf8' ,
                                use_unicode=True )

cursor=db.cursor()

cursor.execute("select * from import.population_density_from_agency_profile")

cursor.execute("select AgencyID from import.chosenAgencies")