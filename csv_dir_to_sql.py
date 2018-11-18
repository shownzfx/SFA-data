import mysql.connector
import csv
import re
import os
import ntpath
import sys
sys.path.insert(0, './python_util')
import mysql_util as ms

# tableName will be auto-set if empty
tableName = ""
dir = ".\\Population Density from Agency Profile"

# Reads all csv files in dir and imports them into a sql table
# file names should start with the year.

if not dir.endswith('\\'):
    dir = dir + '\\'

if tableName == "":
    tableName = ms.to_sql_name(ms.get_dir_name(dir))
    print("tableName = " + tableName)

files = ms.get_csv_files(dir)

ms.create_table_from_files(tableName, files)
