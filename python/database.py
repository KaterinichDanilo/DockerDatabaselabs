import logging
import time
import os

import chardet
import pandas
import psycopg2

# host = "database"
# host = "localhost"
# databaseName = "ZNOdata"
# username = "postgres"
# password = "0000"

username=os.environ["POSTGRES_USER"]
password=os.environ["POSTGRES_PASSWORD"]
databaseName=os.environ["POSTGRES_DB"]
host=os.environ["POSTGRES_HOST"]
N = 1000

def getConnection():
    connection = psycopg2.connect(
        database=databaseName,
        user=username,
        password=password,
        host=host
    )
    return connection

filenamecsv = 'Odata____File.csv'
path = '/resources'
tableName = 'znodata'

def readAllData(filename, encoding):
    print(f'Reading data in {filename}...')
    logging.info('Reading data...')
    stime = time.time()
    dataColumnsName = list(
                pandas.read_csv(filename, encoding=encoding, sep=';', decimal=',', skiprows=0, nrows=1).iloc[[0]])
    dataColumnsName = [x.lower() for x in dataColumnsName]
    dataN = pandas.read_csv(filename, encoding=encoding, sep=';', decimal=',', skiprows=1, names=dataColumnsName)
    dataN = dataN.replace("'", '’', regex=True)
    # dataN = dataN.where(pandas.notnull(dataN), 'null')
    logging.info(f'Reading finished. {(time.time() - stime):.2f} s')
    print(f'Reading finished. {(time.time() - stime):.2f} s')
    return dataN

def readData(dataN, start, amount = N):
    if start + amount > len(dataN):
        return dataN[start: start + len(dataN) - start]
    return dataN[start: start + amount]

def checkColInDB(data):
    getColumnsQuery = f"select column_name from information_schema.columns where table_schema = 'public' and table_name='{tableName}'"
    try:
        conn = getConnection()
        curs = conn.cursor()
        curs.execute(getColumnsQuery)
        colNamesDBSet = set(row[0] for row in curs)
        colNamesDBSet.remove('year')
        colNamesDataSet = set(data.columns)

        if colNamesDataSet <= colNamesDBSet:
            return
        addColQuery = getAddColumnsDBQuery(colNamesDataSet - colNamesDBSet, data.dtypes)
        curs.execute(addColQuery)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(str(error))
    finally:
        if conn is not None:
            conn.close()


def getAddColumnsDBQuery(colList, colTypes):
    addColQuery = f'''ALTER TABLE {tableName} '''
    for col in colList:
        addColQuery += f'ADD COLUMN {col} {getType(col, colTypes)}, '

    addColQuery = addColQuery[:-2] + ';'
    return addColQuery

def getType(colName, cTypes):
    colName = colName.lower()
    if cTypes[colName] == 'int64':
        return 'BIGINT'
    if cTypes[colName] == 'float64':
        return 'REAL'
    return 'CHAR(300)'

def getCreateTableQuery(colNames, colTypes):
    createTableQuery = f'''CREATE TABLE {tableName}( year INT, '''
    for col in colNames:
        createTableQuery += f'{col.lower()} {getType(col, colTypes)}, '
    createTableQuery = createTableQuery[:-2] + ', PRIMARY KEY (outid));'
    return createTableQuery

def createTable(colNames, colTypes):
    deleteTableQuery = f'''DROP TABLE IF EXISTS {tableName};'''
    createTableQuery = getCreateTableQuery(colNames, colTypes)
    # createTableQuery = f"CREATE TABLE {tableName} ();"
    try:
        conn = getConnection()
        cursor = conn.cursor()
        # cursor.execute(deleteTableQuery)
        cursor.execute(createTableQuery)
        print('Table created')
        logging.info('Table created')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('Database error: ' + str(error))
        print(str(error))
    finally:
        if conn is not None:
            conn.close()

def getInsertQuery(colNames, year):
    sr = time.time()
    insertQuery = f"INSERT INTO {tableName} (year, {', '.join(colNames)}) VALUES ({year}, {'%s,' * len(colNames)})"[:-2] + ")"
    return insertQuery

def insertIntoTable(data, i, insertQuery):
    startTime = time.time()

    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()

        for n in range(0, len(data), 1):
            cursor.execute(insertQuery, (data.iloc[[n]].values[0]))
        conn.commit()
        print(f'Committed {len(data)} {N * (i+1)}. Time: {(time.time() - startTime):.2f} seconds')
    except psycopg2.OperationalError as err:
        raise err
    except (Exception, psycopg2.DatabaseError) as error:
        print('error in insertIntoTable')
        logging.error('Database error: ' + str(error))

def insertDataIntoDB(filename, year):
    checkTableExistsQuery = f'''SELECT EXISTS (
    SELECT FROM 
        pg_tables
    WHERE 
        schemaname = 'public' AND 
        tablename  = '{tableName}'
    );'''

    getTableSizeQuery = f'''SELECT COUNT(*) FROM {tableName} WHERE year = {year};'''

    with open(filename, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(10000))
    enc = result.get('encoding')
    dataN = readAllData(filename, enc)
    colTypes = dataN.dtypes
    inserQuery = getInsertQuery(dataN.columns.values, year)

    checkColInDB(dataN)

    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(checkTableExistsQuery)
        tableExists = cursor.fetchall()[0][0]
        i = 0
        if tableExists:
            print(f'Table {tableName} exists! Сontinuing to download...')
            cursor.execute(getTableSizeQuery)
            tableSize = cursor.fetchall()[0][0]
            if tableSize != 0:
                i = tableSize // N
        else:
            colNames = list(
                pandas.read_csv(filename, encoding=enc, sep=';', decimal=',', skiprows=0, nrows=1).iloc[[0]])
            createTable(colNames, colTypes)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('Database error: ' + str(error))
    finally:
        if conn is not None:
            conn.close()

    stime = time.time()
    while True:
        try:

            data = readData(dataN, i * N)
            insertIntoTable(data, i, inserQuery)

            if len(data.values) < N:
                print(f'All data committed to the database {tableName}')
                f = open("timefile.txt", "a")
                f.write(f'{filename} {(time.time() - stime):.2f} s')
                f.close()
                break
            i += 1
        except psycopg2.OperationalError as error:
            logging.error('Connection to DB lost. Try connect in 5 s')
            time.sleep(5)


def compareQueryToCsv():
    compareQuery = f'''SELECT t1.year, TRIM(t1.regname) as regname, ROUND(CAST(t1.avgball AS numeric), 2) as physavgball, t3.year, TRIM(t3.regname) as regname, ROUND(CAST(t3.avgball AS numeric), 2) as physavgball  FROM
	(SELECT year, regname, AVG(physball100) as avgball
	 FROM {tableName}
	 WHERE physball100 != 'NaN' AND physteststatus = 'Зараховано' AND year = 2019
	 GROUP BY year, regname) AS t1
    INNER JOIN 
	(SELECT t2.year as year, TRIM(t2.regname) as regname, t2.avgball as avgball 
 	 FROM 
	 	(SELECT year, regname, AVG(physball100) AS avgball
	 	 FROM {tableName}
		 WHERE physball100 != 'NaN' AND physteststatus = 'Зараховано' AND year = 2021
		 GROUP BY year, regname) AS t2) AS t3
    ON t1.regname = t3.regname
    ORDER BY t1.regname
    '''

    try:
        conn = getConnection()
        cursor = conn.cursor()

        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(compareQuery)
        with open('result.csv', 'w') as f:
            cursor.copy_expert(outputquery, f)
        logging.info('Data wrote to csv!')

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(str(error))
    finally:
        if conn is not None:
            conn.close()
