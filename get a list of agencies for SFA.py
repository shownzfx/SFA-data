#get a list of agencies

import csv
import re
import os
import ntpath
import pandas as pd
import numpy as np
import copy

from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

path=r'C:\Z-Work\Transit project\Transit agency profile\Population Density from Agency Profile\2014 Agency Profile.csv'
path2=r"C:\Z-Work\Transit project\Transit agency profile\Energy consumption\2005 Energy Consumption.csv"


def readcsv(f):
    global rows
    rows = []
    with open(path) as f:
        reader=csv.DictReader(f)
        for row in reader:
            rows.append(row)
        return rows

readcsv(path)
agency2016=pd.DataFrame.from_dict(rows)





rows1=[]
with open ( path2 ) as f:
    reader = csv.DictReader ( f )
    for row in reader:
        rows1.append ( row )
energy2005=pd.DataFrame.from_dict(rows1)

energy2005=energy2005[energy2005['  mode_cd']=="MB"]
energy2005.rename(columns={'  mode_cd':'mode'}, inplace=True)
energy2005["mode"].value_counts()
energy2005["AgencyID"].value_counts().sort_values(ascending=False)
energy2005.shape  #366 19



agency2016.columns[agency2016.columns.str.contains("state",case=False)]

agency2016["Reporter Type"].value_counts()

chosenAgencies=agency2016[agency2016["Reporter Type"]=="Full Reporter: Operating"]
states=['CT','ME','MA','NH','NJ','NY','PA','RI','VT','IL','IN','IA','KS','MI','MN','MO','NE','ND','OH','SD','WI']
chosenAgencies= chosenAgencies[chosenAgencies["State"].isin(states)]

chosenAgencies.shape  #266 31

agency1=set(agency2016['4 digit NTDID'])
agency2=set(energy2005.AgencyID)

agencyCommon=[x for x in agency1 if x in agency2]

print(map(len,[agency1,agency2,agencyCommon]))
[x for x in map(len,[agency1,agency2,agencyCommon])]   #[846, 366, 329

chosenAgencies=chosenAgencies[chosenAgencies["4 digit NTDID"].isin(agencyCommon)]

chosenAgencies.shape  #130
chosenAgencies["AgencyID"]=chosenAgencies["4 digit NTDID"]
chosenAgencies["AgencyID"].duplicated().value_counts()

ids=chosenAgencies.AgencyID


chosenAgencies[chosenAgencies.duplicated(['AgencyID'], keep=False)]



is_numeric_dtype(chosenAgencies.VOMS)
is_string_dtype(chosenAgencies.VOMS)

chosenAgencies.VOMS=chosenAgencies.VOMS.str.replace(",","")


chosenAgencies.VOMS= pd.to_numeric(pd.Series(chosenAgencies.VOMS))
chosenAgencies.VOMS=chosenAgencies.VOMS.astype(int)
is_numeric_dtype(chosenAgencies.VOMS)


chosenAgencies.sort_values(by=["VOMS"], ascending=False)

chosenAgencies.VOMS.sort_values()

chosenAgencies.columns


chosenAgencies.loc[chosenAgencies.VOMS<30,["Reporter Name"]]

chosenAgencies.loc[chosenAgencies["Reporter Name"]=="Greater Portland Transit District","State"]
chosenAgencies_backup=copy.deepcopy(chosenAgencies)


chosenAgencies=chosenAgencies[chosenAgencies.VOMS>30]
chosenAgencies.shape  #105 32

chosenAgencies_backup.shape #130

filename_export="C:\Z-Work\Transit project\Transit agency profile\Population Density from Agency Profile\Chosen Agencies.csv"


chosenAgencies.to_csv(filename_export,header=True,index=False)




