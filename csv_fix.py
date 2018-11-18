import csv
import re
import os
import ntpath
import glob

class Column:
    def __init__ ( self , name, csvIndex):
        self.name = name
        self.csvIndex = csvIndex
        self.lastVal = ''

    def getVal( self, row ):
        return row[self.csvIndex]

    def fix(self, row ):
        val = row[self.csvIndex].strip()
        if val == '':
            row[self.csvIndex] = self.lastVal
        else:
            self.lastVal = val



def fix_csv(path):
    rows = []
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        orgTypeCol = None
        columns = []
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                for idx in range(0, len(row)):
                    val = row[idx]
                    if val == 'State' or val == 'Agency' or val == 'AgencyID':
                        col = Column(val, idx)
                        columns.append(col)
                    else:
                        if val == 'OrgType':
                            orgTypeCol = Column(val, idx)
                line_count += 1
            else:
                for col in columns:
                    col.fix(row)
                line_count += 1
            orgVal = orgTypeCol.getVal(row).strip()
            if orgVal != 'Total' and orgVal != '':
                rows.append(row)
        print(f'Processed {line_count} lines.')

    with open (path.replace('.csv', '.fixed1117.csv'), 'w',newline='') as f:
        writer = csv.writer ( f )
        writer.writerows ( rows )

dir = "C:\\Z-Work\\Transit project\\Transit agency profile\\Employees\\"
files=[]

# for file in os.listdir(dir):
#     if file.endswith(".csv") and re.search('201(3|4|5|6)',file) is not None:
#         path=dir+file
#         files.append(path)


for file in os.listdir(dir):
    if file.endswith(".csv") and re.match("201[567]", file) is not None :
        path = dir + file
        fix_csv(path)


# path = dir + "2005 Employee Count, Work Hours and VOMs.csv"
# fix_csv(path)





