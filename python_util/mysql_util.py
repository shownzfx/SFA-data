import mysql.connector
import csv
import re
import os
import ntpath


def chunks(list, n):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(list), n):
        yield list[i:i + n]


def get_csv_files(dir):
    files = os.listdir(dir)
    files = filter(lambda x: x.endswith('.csv'), files)
    files = list(map(lambda x: dir + x, files))
    files = sorted(files, reverse=True)
    return files


def get_db_columns(files):
    columns = []
    for path in files:
        print("analyzing " + path)
        table = read_csv('tmp_table', path, columns)
        columns = table.columns

    columns = [x for x in columns if x.sqlName != 'total']
    analyze_column_types(columns)
    return columns


def parse_year(path):
    fileName = ntpath.basename(path)
    yearResult = re.match('(^|[^0-9])([0-9]{4})([^0-9]|$)', fileName)
    if yearResult is not None:
        return yearResult.groups()[1]


def is_number(val):
    return re.match(r'^([0-9,\.]+)$', val) is not None and (len(val) == 1 or val[0] != '0')


def get_file_name(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_dir_name(path):
    return os.path.basename(os.path.dirname(path))


def to_sql_name(csvName):
    csvName = csvName.strip()

    sqlName = ''
    for c in csvName:
        if re.match('[A-Za-z0-9_]', c):
            sqlName += c
        else:
            sqlName += '_'
    c = sqlName[0]
    if not re.match('[A-Za-z_]', c):
        sqlName = "_" + sqlName
    sqlName = sqlName.lower()

    # HACK NAMES
    if (sqlName == 'agency'):
        return 'agency_name'
    if (sqlName == 'liquefied_natural_gas'):
        return 'liquefied_nat_gas'

    return sqlName


class DbColumn:  # only read each column
    """A database column"""
    csvIndex = 0
    csvName = ''
    sqlName = ''
    sqlType = 'numeric'
    nullable = True
    numNumericRows = 0
    numDecimals = 0
    numDigits = 0
    numTextRows = 0
    includeInTotal = True

    def __init__(self, csvIndex, csvName, sqlType='numeric'):
        self.csvIndex = csvIndex
        self.sqlType = sqlType
        self.csvName = csvName.strip()
        self.sqlName = to_sql_name(self.csvName)
        if(self.sqlName == 'other_fuel_description'):
            self.sqlType = 'text'
        if(self.sqlName == '_5_digit_ntd_id' or self.sqlName == 'legacy_ntd_id'):
            self.sqlType = 'text'

    def should_insert(self):
        return self.csvIndex != -1

    def getSqlType(self):
        if self.sqlType == 'numeric':
            return 'decimal(16,' + str(self.numDecimals) + ')'
        return self.sqlType

    def toCreateSql(self):
        sql = self.sqlName + ' ' + self.getSqlType()
        if not self.nullable:
            sql += ' not null'
        return sql

    def getSqlValue(self, row):
        val = row[self.csvIndex]
        if(self.sqlType == 'numeric'):
            # if(isinstance(val, str) and val.strip() == ''):
             #   return None
            val = val.replace(',', '').strip()
            if(val == ''):
                return None
            try:
                return float(val)
            except:
                raise ValueError(
                    '"' + val + '" could not be converted to a number')

        return val

    def testDataType(self, row):
        if(self.csvIndex == -1):
            return
        val = row[self.csvIndex].strip()
        if(val == ''):
            return
        if is_number(val):
            self.numNumericRows = self.numNumericRows + 1
            self.numDigits = max(len(val), self.numDigits)
            if '.' in val:
                self.numDecimals = max(
                    len(val) - val.find('.'), self.numDecimals)
        else:
            if self.sqlName == 'bunker_fuel':
                print('hi')
            self.numTextRows = self.numTextRows + 1


class YearColumn(DbColumn):
    def __init__(self, year):
        self.csvIndex = None
        self.csvName = None
        self.sqlName = 'year'
        self.sqlType = 'numeric'
        self.nullable = True
        self.year = year
        self.includeInTotal = False

    def should_insert(self):
        return True

    def getSqlValue(self, row):  # why row
        return self.year

    def testDataType(self, row):
        return


class TotalColumn(DbColumn):
    def __init__(self, columns):
        # include is a func defined later
        self.colsToSum = [i for i in columns if self.include(i)]
        self.numDecimals = max(
            self.colsToSum, key=lambda x: x.numDecimals).numDecimals
        self.csvIndex = None
        self.csvName = None
        self.sqlName = 'total'
        self.sqlType = 'numeric'
        self.nullable = False
        self.includeInTotal = False

    def should_insert(self):
        return True

    def include(self, column):
        return column.sqlType != 'text' and column.sqlName != 'total' and column.includeInTotal

    def getSqlValue(self, row):
        sum = 0
        for col in self.colsToSum:
            if(col.csvIndex != -1):  # what is col.csvIndex
                val = col.getSqlValue(row)
                if val is not None:
                    sum += val
        return sum

    def testDataType(self, row):
        return


def getCol(columns, colName):
    for col in columns:  # col.sqlName how
        if col.sqlName == colName:
            return col


class DbTable:
    def __init__(self, name, columns, rows=[]):
        self.name = name
        self.columns = columns
        self.rows = rows

    def hasCol(self, columns, colName):
        for col in columns:
            if col.sqlName == colName:  # here to
                return True
        return False

    def toCreateSql(self):
        """ Should fail if table exists since we don't want duplicate data. Manually drop it first. """
        sql = 'create table ' + self.name + ' (\n'

        needsComma = False

        for c in self.columns:
            if(needsComma):
                sql += ',\n'
            needsComma = True
            sql += c.toCreateSql()  # what does this mean

        sql += '\n);'
        return sql

    def drop(self, cursor):
        cursor.execute('drop table if exists ' + self.name)

    def getInsertArray(self, rows):
        arr = []
        for row in rows:
            dict = {}
            for col in self.columns:
                dict[col.sqlName] = col.getSqlValue(row)
            arr.append(dict)
        return arr

    def run_insert_commands(self, rows, cursor):
        columns = [x for x in self.columns if x.should_insert()]
        placeholders = ', '.join(['%s'] * len(columns))
        # how did you get sqlName
        columnNames = ', '.join(map(lambda x: x.sqlName, columns))
        for row in rows:
            values = list(map(lambda x: x.getSqlValue(row), columns))
            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (
                self.name, columnNames, placeholders)
            cursor.execute(sql, values)


def read_csv(tableName, path, columns=[]):
    for col in columns:
        col.csvIndex = -1
    with open(path, errors='ignore') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        rows = []
        for row in csv_reader:
            if line_count == 0:
                for idx in range(0, len(row)):
                    val = row[idx]
                    if(val.strip() != ''):
                        sqlName = to_sql_name(val)
                        dbCol = getCol(columns, sqlName)
                        if dbCol is None:
                            dbCol = DbColumn(idx, val)
                            columns.append(dbCol)
                        dbCol.csvIndex = idx
                line_count += 1
            else:
                rows.append(row)
                for col in [x for x in columns if x.csvIndex != -1]:
                    col.testDataType(row)
                line_count += 1
        print(f'Processed {line_count} lines.')
        return DbTable(tableName, columns, rows)


def analyze_column_types(columns):
    for col in columns:
        if(col.numTextRows == 0):
            if(col.numNumericRows == 0):
                print('col had no data: ' + col.csvName)
            else:
                col.sqlType = 'numeric'
        else:
            col.sqlType = 'text'
            if(col.numTextRows < 20):
                print('col had not much data ' + col.csvName)


logFile = "log.txt"


def remove_log_file():
    try:
        os.remove(logFile)
    except:
        return


def log_to_file(msg):
    with open(logFile, "a") as myfile:
        myfile.write(msg)


def create_table_from_files(tableName, files):
    mysql_password = os.environ['MysqlPassword']
    cnx = mysql.connector.connect(user='root', password=mysql_password,
                                  host='127.0.0.1',
                                  database='import',
                                  charset='utf8',
                                  use_unicode=True)

    try:
        remove_log_file()
        cursor = cnx.cursor()

        columns = get_db_columns(files)

        yearColumn = YearColumn(0)
        columns.append(yearColumn)
        columns.append(TotalColumn(columns))
        table = DbTable(tableName, columns)

        print("dropping table " + tableName)
        table.drop(cursor)

        createSql = table.toCreateSql()
        log_to_file(createSql)
        cursor.execute(createSql)

        for path in files:
            print("processing " + path)
            table = read_csv(tableName, path, columns)

            yearColumn.year = parse_year(path)

            rowChunks = chunks(table.rows, 50)

            for chunk in rowChunks:
                table.run_insert_commands(chunk, cursor)

        cnx.commit()
        cursor.close()
        cnx.close()
        print("Inserted " + str(len(table.rows)) + ' rows')

    finally:
        cnx.close()
