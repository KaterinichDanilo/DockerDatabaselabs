import logging
import time
import os

import chardet
import pandas
import psycopg2

host = "database"
host = "localhost"
databaseName = "ZNOdata"
username = "postgres"
password = "0000"

# username=os.environ["POSTGRES_USER"]
# password=os.environ["POSTGRES_PASSWORD"]
# databaseName=os.environ["POSTGRES_DB"]
# host=os.environ["POSTGRES_HOST"]
N = 1000

subjectsList = ['uml', 'ukr', 'hist', 'math', 'mathst', 'phys', 'chem', 'bio', 'geo', 'eng', 'fra', 'deu', 'spa']
regionsSybCol = ['ptname', 'ptregname', 'ptareaname', 'pttername']
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

def getType(colName, cTypes):
    colName = colName.lower()
    if cTypes[colName] == 'int64':
        return 'BIGINT'
    if cTypes[colName] == 'float64':
        return 'REAL'
    return 'CHAR(300)'

def getInsertQuery(tableName, colNames):
    if(tableName == 'math_test' and colNames[-1].startswith('mathst')):
        colNames = [x for x in colNames if not x.startswith('mathst')]
    insertQuery = f"INSERT INTO {tableName} (eo_id, student_id,  {', '.join(colNames)}) VALUES ({'%s,' * (len(colNames) + 2)})"[:-2] + ")"
    return insertQuery

def insertIntoTable(data, insertQuery):
    startTime = time.time()

    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()

        for n in range(0, len(data), 1):
            cursor.execute(insertQuery, (data.iloc[[n]].values[0]))
        conn.commit()
        print(f'Committed {len(data)}. Time: {(time.time() - startTime):.2f} seconds')
    except psycopg2.OperationalError as err:
        raise err
    except (Exception, psycopg2.DatabaseError) as error:
        print('error in insertIntoTable')
        logging.error('Database error: ' + str(error))
    finally:
        conn.close()

def getRegionId(regname, areaname, tername):
    query = f"SELECT id FROM regions WHERE regname='{regname}' AND areaname='{areaname}' AND tername='{tername}';"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regId = cursor.fetchall()[0][0]
        return regId
    except (Exception, psycopg2.DatabaseError) as error:
        return -1
    finally:
        conn.close()

def insertRegion(regname, areaname, tername, tertypename):
    query = f"INSERT INTO regions(regname, areaname, tername, tertypename) VALUES ('{regname}', '{areaname}', '{tername}', '{tertypename}');"
    print(query)
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegion: ' + str(error))
    finally:
        conn.close()

def getRegionEoId(eoregname, eoareaname, eotername):
    query = f"SELECT id FROM regions_eo WHERE eoregname='{eoregname}' AND eoareaname='{eoareaname}' AND eotername='{eotername}';"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regId = cursor.fetchall()[0][0]
        return regId
    except (Exception, psycopg2.DatabaseError) as error:
        return -1
    finally:
        conn.close()

def insertRegioneo(eoregname, eoareaname, eotername):
    query = f"INSERT INTO regions_eo(eoregname, eoareaname, eotername) VALUES ('{eoregname}', '{eoareaname}', '{eotername}');"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegionEo: ' + str(error))
    finally:
        conn.close()

def getEoId(eoname, eotypename, eoparent, regeo_id):
    query = f"SELECT id FROM educationalorganizations WHERE eoname='{eoname}' AND eotypename='{eotypename}' AND eoparent='{eoparent}' AND regeo_id='{regeo_id}';"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regId = cursor.fetchall()[0][0]
        return regId
    except (Exception, psycopg2.DatabaseError) as error:
        return -1
    finally:
        conn.close()

def insertEo(eoname, eotypename, eoparent, regeo_id):
    query = f"INSERT INTO educationalorganizations(eoname, eotypename, eoparent, regeo_id) VALUES ('{eoname}', '{eotypename}', '{eoparent}', '{regeo_id}');"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEo: ' + str(error))
    finally:
        conn.close()

def insertRegFromData(data):
    query = f"SELECT id, TRIM(regname), TRIM(areaname), TRIM(tername), TRIM(tertypename)  FROM regions;"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regInDbWithId = cursor.fetchall()
        regInDbNoId = [list(x[1:]) for x in regInDbWithId]
        df2 = data[["regname", "areaname", "tername", "tertypename"]]
        for d in df2.values:
            if not list(d) in regInDbNoId:
                insertRegion(d[0], d[1], d[2], d[3])
                regInDbNoId.append(list(d))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegFromData: ' + str(error))
    finally:
        conn.close()

def insertRegeoFromData(data):
    query = f"SELECT id, TRIM(eoregname), TRIM(eoareaname), TRIM(eotername) FROM regions_eo;"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regeoInDbWithId = cursor.fetchall()
        regeoInDbNoId = [list(x[1:]) for x in regeoInDbWithId]
        df2 = data[["eoregname", "eoareaname", "eotername"]]
        for d in df2.values:
            if not list(d) in regeoInDbNoId:
                insertRegioneo(d[0], d[1], d[2])
                regeoInDbNoId.append(list(d))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegeoFromData: ' + str(error))
    finally:
        conn.close()

def insertEoFromData(data):
    query = f"SELECT id, TRIM(eoname), TRIM(eotypename), TRIM(eoparent) FROM educationalorganizations;"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        eoInDbWithId = cursor.fetchall()
        eoInDbNoId = [list(x[1:]) for x in eoInDbWithId]
        df2 = data[["eoname", "eotypename", "eoparent", "eoregname", "eoareaname", "eotername"]]
        for d in df2.values:
            print(d)
            if not list(d)[:-3] in eoInDbNoId:
                regeoId = getRegionEoId(d[3], d[4], d[5])
                insertRegioneo(d[0], d[1], d[2], regeoId)
                eoInDbNoId.append(list(d))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEoFromData: ' + str(error))
    finally:
        conn.close()

def insertStudentsFromData(data):
    try:
        df2 = data[["outid", "eotypename", "eoparent", "eoregname", "eoareaname", "eotername"]]
        for d in df2.values:
            print(d)
            if not list(d)[:-3] in eoInDbNoId:
                regeoId = getRegionEoId(d[3], d[4], d[5])
                insertRegioneo(d[0], d[1], d[2], regeoId)
                eoInDbNoId.append(list(d))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEoFromData: ' + str(error))
    finally:
        conn.close()


def insertDataIntoDB(filename, year):
    getTableSizeQuery = f'''SELECT COUNT(*) FROM students WHERE year = {year};'''

    with open(filename, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(10000))
    enc = result.get('encoding')
    dataN = readAllData(filename, enc)
    insertRegFromData(dataN)

    subInsQueries = {}
    for sub in subjectsList:
        # subLst = [col for col in dataN.columns if col.startswith(sub)]
        subLst  = ['outid']
        subLst += [col for col in dataN.columns if (col.startswith(sub) and (col.replace(sub, '') not in regionsSybCol))]
        subInsQueries[sub] = getInsertQuery(sub + '_test', subLst)

    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(getTableSizeQuery)
        tableSize = cursor.fetchall()[0][0]
        i = tableSize // N
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('Database error: ' + str(error))
    finally:
        if conn is not None:
            conn.close()

    stime = time.time()
    insertRegFromData(dataN)
    insertRegeoFromData(dataN)
    insertEoFromData(dataN)

    while True:
        try:
            data = readData(dataN, i * N)
            insertIntoTable(data, subInsQueries)

            if len(data.values) < N:
                print(f'All data committed to the database!')
                f = open("timefile.txt", "a")
                f.write(f'{filename} {(time.time() - stime):.2f} s')
                f.close()
                break
            i += 1
        except psycopg2.OperationalError as error:
            logging.error('Connection to DB lost. Try connect in 5 s')
            time.sleep(5)


def compareQueryToCsv():
    # compareQuery = f'''SELECT t1.year, TRIM(t1.regname) as regname, ROUND(CAST(t1.avgball AS numeric), 2) as physavgball, t3.year, ROUND(CAST(t3.avgball AS numeric), 2) as physavgball  FROM
	# (SELECT year, regname, AVG(physball100) as avgball
	#  FROM {tableName}
	#  WHERE physball100 != 'NaN' AND physteststatus = 'Зараховано' AND year = 2019
	#  GROUP BY year, regname) AS t1
    # INNER JOIN
	# (SELECT t2.year as year, TRIM(t2.regname) as regname, t2.avgball as avgball
 	#  FROM
	#  	(SELECT year, regname, AVG(physball100) AS avgball
	#  	 FROM {tableName}
	# 	 WHERE physball100 != 'NaN' AND physteststatus = 'Зараховано' AND year = 2021
	# 	 GROUP BY year, regname) AS t2) AS t3
    # ON t1.regname = t3.regname
    # ORDER BY t1.regname
    # '''

    # try:
    #     conn = getConnection()
    #     cursor = conn.cursor()
    #
    #     outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(compareQuery)
    #     with open('result.csv', 'w') as f:
    #         cursor.copy_expert(outputquery, f)
    #     logging.info('Data wrote to csv!')
    #
    # except (Exception, psycopg2.DatabaseError) as error:
    #     logging.error(str(error))
    # finally:
    #     if conn is not None:
    #         conn.close()
    return

# print(getRegionId('Днропетровська область', 'м.Кам’янське', 'Дніпровський район міста'))
# da = readAllData('resources/Odata2021File.csv', 'cp1251')
# insertRegFromData(da)
