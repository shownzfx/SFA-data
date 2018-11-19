import mysql.connector
import csv
import re
import os
import ntpath
import sys
import os
import importlib.util
sys.path.insert(0, './python_util')
import mysql_util as ms #Mike-written function save in this directory (not packages or built in)

spec=importlib.util.spec_from_file_location("mysql_util", r"C:\Z-Work\Transit project\Transit agency profile\python_util\mysql_util.py")

ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)



# tableName will be auto-set if empty
#tableName = ""

#file=r".\Population Density from Agency Profile\Chosen Agencies.csv"

tableName=""
dir="C:\\Z-Work\\Transit project\\Transit agency profile\\Employees\\"
filename=[i for i in os.listdir(dir) if re.search("fixed",i) is not None]


def writeFile(csvFiles):
    for file in csvFiles:
        if tableName == "":
            tableName=ms.to_sql_name(ms.get_file_name(file))
        files=[file]
        ms.create_table_from_files(tableName,files)

writeFile(filename)

# if tableName == "":
#     tableName = ms.to_sql_name(ms.get_file_name(file))
#     print("tableName = " + tableName)
#
# files = [file]
# ms.create_table_from_files(tableName, files)
