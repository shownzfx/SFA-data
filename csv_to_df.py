import os
import glob
import re
import csv
import pandas as pd
import sys
import copy
sys.path.insert(0, './python_util')
import mysql_util
from collections import Counter

dir="C:\\Z-Work\\Transit project\\Transit agency profile\\Energy consumption\\"
os.chdir(dir)

paths=[i for i in glob.glob("*.{}".format("csv"))]


def csv_to_df(path):
    with open(path, newline="") as f:
        rows = []
        reader=csv.DictReader(f)
        for row in reader:
            rows.append(row)
        return pd.DataFrame(rows)


class DfInfo:
    def __init__(self, year, path):
        self.df = csv_to_df(path)
        self.year = year


energy=[]
for file in paths:
    path=dir+file
    year = mysql_util.parse_year(path)
    dfInfo = DfInfo(year, path)
    energy.append(dfInfo)

type(energy[0].df)  #access element element


df=copy.deepcopy(energy[0].df)
df.dtypes


for col in df.columns:
    df[col]=df[col].str.replace ( ',' , '' )
    try:
        df[col]=df[col].astype(int)
    except:
        print('"' + col + '" could not be converted to a number')



def str_to_number (array):
    for idx in range (0,len(array)):
        temp=array[idx].df
        for col in temp.columns:
            temp[col]=temp[col].str.replace(",","")
        try:
            temp[col]=temp[col].astype(str).astype(int)
        except:
            print("'"+col+"'could not be converted to a number")







str_to_number(energy)

for idx in range ( 0 , len ( energy ) ):
    temp = energy[idx].df
    print(temp.dtypes)

def colNum(array):
    a=[]
    for idx in range (0,len(energy)):
        temp=energy[idx].df
        a.append(len(temp.columns))
    return a


colCount=colNum(energy)
Counter(colCount)




