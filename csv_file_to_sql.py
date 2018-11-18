import mysql.connector
import csv
import re
import os
import ntpath
import sys
sys.path.insert(0, './python_util')
import mysql_util as ms #Mike-written function (not packages or built in)

# tableName will be auto-set if empty
tableName = ""
#file = ".\\Population Density from Agency Profile\\2005 Agency Profile.csv"
#file=r"C:\Z-Work\Transit project\Transit agency profile\Population Density from Agency Profile\Chosen Agencies.csv"
file=r".\Population Density from Agency Profile\Chosen Agencies.csv"

if tableName == "":
    tableName = ms.to_sql_name(ms.get_file_name(file))
    print("tableName = " + tableName)

files = [file]

ms.create_table_from_files(tableName, files)
