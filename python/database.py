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
# password = "1234"

username=os.environ["POSTGRES_USER"]
password=os.environ["POSTGRES_PASSWORD"]
databaseName=os.environ["POSTGRES_DB"]
host=os.environ["POSTGRES_HOST"]
N = 1000

subjectsList = ['uml', 'ukr', 'hist', 'math', 'mathst', 'phys', 'chem', 'bio', 'geo', 'eng', 'fra', 'deu', 'spa']
sub1 = ['hist', 'phys', 'chem', 'bio', 'geo']
subLang = ['eng', 'fra', 'deu', 'spa']
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

def insertReg(regname, conn):
    query = f"INSERT INTO regs(regname) VALUES ('{regname}');"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertReg: ' + str(error))
        raise error

def insertArea(areaname, conn):
    query = f"INSERT INTO areas(areaname) VALUES ('{areaname}');"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertArea: ' + str(error))
        raise error

def insertTer(tername, tertypename, conn):
    query = f"INSERT INTO ters(tername, tertypename) VALUES ('{tername}', '{tertypename}');"
    if tertypename is None: query = f"INSERT INTO ters(tername, tertypename) VALUES ('{tername}', null);"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertTer: ' + str(error))
        raise error

def insertEo(conn, eoname, eotypename, eoparent, regname, areaname, tername):
    if isinstance(eoname, float):
        return
    query = f"INSERT INTO eo (eoname, eotypename, eoparent, regname, areaname, tername) VALUES ('{eoname}', '{eotypename}', '{eoparent}', '{regname}', '{areaname}', '{tername}');"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEo: ' + str(error))
        raise error

def insertPt(conn, ptname, ptregname, ptareaname, pttername):
    query = f"INSERT INTO pt (name, regname, areaname, tername) VALUES ('{ptname}', '{ptregname}', '{ptareaname}', '{pttername}');"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertPt: ' + str(error))
        raise error
def getEoId(eoname, eotypename, eoparent, regname):
    query = f"SELECT id FROM educationalorganizations WHERE eoname='{eoname}' AND eotypename='{eotypename}' AND eoparent='{eoparent}' AND regname='{regname}';"
    try:
        conn = getConnection()
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regId = cursor.fetchall()[0][0]
        return regId
    except (Exception, psycopg2.DatabaseError) as error:
        return -1

def insertStudent(conn, outid, birth, year, sextypename, classprofilename, classlangname, regtypename, eo_id,
                  regname, areaname, tername):
    if eo_id == -1: eo_id = 'null'
    query = f'''INSERT INTO students(
    	id, birth, year, sextypename, classprofilename, classlangname, regtypename, eo_id, regname, areaname, tername)
    	VALUES ('{outid}', {birth}, {year}, '{sextypename}', '{classprofilename}', '{classlangname}', '{regtypename}', {eo_id},
                  '{regname}', '{areaname}', '{tername}')'''
    try:
        cursor = conn.cursor()
        cursor.execute(query)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegFromData: ' + str(error))
        raise error

def insertRegFromData(data, conn):
    query = f"SELECT TRIM(regname) FROM regs;"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        regInDb = cursor.fetchall()
        regInDb = [list(r)[0] for r in regInDb]
        df2 = [col for col in data.columns if 'regname' in col]
        stacked = data[df2].stack().reset_index(drop=True).drop_duplicates()
        for r in stacked.values:
            if not r in regInDb:
                insertReg(r, conn)
                regInDb.append(r)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertRegFromData: ' + str(error))
        raise error

def insertAreaFromData(data, conn):
    query = f"SELECT TRIM(areaname) FROM areas;"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        arInDb = cursor.fetchall()
        arInDb = [list(r)[0] for r in arInDb]
        df2 = [col for col in data.columns if 'areaname' in col]
        stacked = data[df2].stack().reset_index(drop=True).drop_duplicates()
        for r in stacked.values:
            if not r in arInDb:
                insertArea(r, conn)
                arInDb.append(r)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertAreaFromData: ' + str(error))
        raise error

def insertTerFromData(data, conn):
    query = f"SELECT TRIM(tername) FROM ters;"
    try:
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        cursor.execute(query)
        locInDb = cursor.fetchall()
        locInDb = [list(r)[0] for r in locInDb]
        df2 = data[['tername', 'tertypename']].drop_duplicates('tername')
        for r in df2[['tername']].values:
            if not r in locInDb:
                tt = df2.loc[data['tername'] == r[0]][['tertypename']].iloc[0]['tertypename']
                insertTer(r[0], tt, conn)
                locInDb.append(r)

        df2 = [col for col in data.columns if 'tername' in col]
        stacked = data[df2].stack().reset_index(drop=True).drop_duplicates()
        for r in stacked.values:
            if not r in locInDb:
                insertTer(r, None, conn)
                locInDb.append(r)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertTerFromData: ' + str(error))
        raise error

def insertEoFromData(data, conn):
    query = f"SELECT TRIM(eoname), TRIM(eotypename), TRIM(eoparent), TRIM(regname) FROM eo;"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        eoInDb = cursor.fetchall()
        eoInDb = [list(r) for r in eoInDb]
        df2 = data[["eoname", "eotypename", "eoparent", "eoregname", "eoareaname", "eotername"]].drop_duplicates(['eoname', 'eotypename'])
        for d in df2.values:
            if not list(d)[:-2] in eoInDb:
                insertEo(conn, d[0], d[1], d[2], d[3], d[4], d[5])
                eoInDb.append(list(d)[:-4])
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEoFromData: ' + str(error))
        raise error

def insertPtFromData(data, conn):
    query = f"SELECT TRIM(name) FROM pt;"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        ptInDb = cursor.fetchall()
        ptInDb = [r[0] for r in ptInDb]
        for s in subjectsList:
            colname = s + 'ptname'
            if colname in data.columns:
                df2 = data[
                    [colname, s+'ptregname', s+"ptareaname", s+"pttername"]].drop_duplicates(subset=[colname])
                for d in df2.values:
                    if isinstance(d[0], float):
                        continue
                    d = [r.rstrip() for r in d]
                    if not d[0] in ptInDb:
                        insertPt(conn, d[0], d[1], d[2], d[3])
                        ptInDb.append(d[0])
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertPtFromData: ' + str(error))
        raise error

def insertStudentsFromData(data, conn, year):
    query = f"SELECT id, TRIM(eoname), TRIM(eotypename), TRIM(regname) FROM eo;"
    queryS = f"SELECT TRIM(id) FROM students;"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        eoInDb = cursor.fetchall()
        eoInDb = [list(e) for e in eoInDb]
        cursor.execute(queryS)
        stInDb = cursor.fetchall()
        stInDb = [list(e)[00] for e in stInDb]
        eoId = {}
        for e in eoInDb:
            eoId[e[0]] = e[1:]
        df2 = data[["outid", 'birth', 'sextypename', 'regname', 'areaname', 'tername', 'regtypename', 'classprofilename', 'classlangname', 'eoname', 'eotypename', 'eoregname']]
        id_list = list(eoId.keys())
        val_list = list(eoId.values())
        for d in df2.values:
            # ti = time.time()
            # eo_id = getEoId(d[9], d[10], d[11], d[12])
            if d[0] in stInDb: continue
            if isinstance(d[-3], float): eo_id = 'null'
            else:
                eo_id = id_list[val_list.index(list(d)[-3:])]

            insertStudent(conn, d[0], d[1], year, d[2], d[7],
                          d[8], d[6], eo_id, d[3], d[4], d[5])
            # print(time.time() - ti)
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertEoFromData: ' + str(error))
        raise error

def insertUmlFromData(data, conn):
    query = '''INSERT INTO uml_test(
	student_id, test, teststatus, ball100, ball12, ball, adaptscale, ptname)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM uml_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]
        df2 = data[['outid', 'umltest', 'umlteststatus', 'umlball100', 'umlball12', 'umlball', 'umladaptscale', 'umlptname']]
        for d in df2.values:
            if(isinstance(d[1], float) or d[0] in stInDb): continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        return

def insertUkrFromData(data, conn):
    query = '''INSERT INTO ukr_test(
            	student_id, test, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname)
            	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM ukr_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]

        collist = ['outid', 'ukrtest', 'ukrsubtest', 'ukrteststatus', 'ukrball100',
                    'ukrball12', 'ukrball', 'ukradaptscale', 'ukrptname']
        colInDf = []
        for c in collist:
            if c in data.columns:
                colInDf.append(c)
            else:
                query = query.replace(c.replace('ukr', '') + ',', '')
                query = query.replace('%s,', '', 1)
        df2 = data[colInDf]
        for d in df2.values:
            if(isinstance(d[1], float) or d[0] in stInDb): continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertUkrFromData: ' + str(error))
        raise error

def insertMathFromData(data, conn):
    query = '''INSERT INTO math_test(
	student_id, test, lang, teststatus, ball100, ball12, ball, dpalevel, ptname)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM math_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]
        collist = ['outid', 'mathtest', 'mathlang', 'mathteststatus', 'mathball100',
                    'mathball12', 'mathball', 'mathdpalevel', 'mathptname']
        colInDf = []
        for c in collist:
            if c in data.columns:
                colInDf.append(c)
            else:
                query = query.replace(c.replace('math', '') + ',', '')
                query = query.replace('%s,', '', 1)
        df2 = data[colInDf]
        for d in df2.values:
            if(isinstance(d[1], float) or d[0] in stInDb): continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertMathFromData: ' + str(error))
        raise error

def insertMathstFromData(data, conn):
    query = '''INSERT INTO mathst_test(
	student_id, test, lang, teststatus, ball12, ball, ptname)
	VALUES (%s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM mathst_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]
        df2 = data[['outid', 'mathsttest', 'mathstlang', 'mathstteststatus',
                    'mathstball12', 'mathstball', 'mathstptname']]
        for d in df2.values:
            if(isinstance(d[1], float) or d[0] in stInDb): continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertMathstFromData: ' + str(error))
        raise error

def insertSubFromData(data, sub, conn):
    if not sub + 'test' in data.columns: return
    query = f'''INSERT INTO {sub}_test(
	student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM {sub}_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]
        df2 = data[['outid', sub+'test', sub+'lang', sub+'teststatus', sub+'ball100',
                    sub+'ball12', sub+'ball', sub+'ptname']]
        for d in df2.values:
            if(isinstance(d[1], float) or d[0] in stInDb): continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertMathFromData: ' + str(error))
        raise error

def insertSubLFromData(data, sub, conn):
    if not sub + 'test' in data.columns: return
    query = f'''INSERT INTO {sub}_test(
	student_id, test, teststatus, ball100, ball12, ball, dpalevel, ptname)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'''
    queryS = f"SELECT TRIM(student_id) FROM {sub}_test;"
    try:
        curs = conn.cursor()
        curs.execute(queryS)
        stInDb = curs.fetchall()
        stInDb = [list(e)[0] for e in stInDb]
        df2 = data[['outid', sub+'test', sub+'teststatus', sub+'ball100',
                    sub+'ball12', sub+'ball', sub+'dpalevel', sub+'ptname']]
        for d in df2.values:
            if isinstance(d[1], float) or d[0] in stInDb: continue
            curs = conn.cursor()
            curs.execute(query, list(d))
    except psycopg2.OperationalError as error:
        raise error
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('insertSubLFromData: ' + str(error))
        raise error

def insertDataIntoDB(filename, year):
    getTableSizeQuery = f'''SELECT COUNT(*) FROM students WHERE year = {year};'''

    with open(filename, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(10000))
    enc = result.get('encoding')
    dataN = readAllData(filename, enc)
    i = 0
    try:
        conn = getConnection()
        cursor = conn.cursor()
        cursor.execute(getTableSizeQuery)
        tableSize = cursor.fetchall()[0][0]
        i = tableSize // N
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error('Database error: ' + str(error))
    finally:
        if conn is not None:
            conn.close()


    while True:
        try:
            conn = getConnection()
            data = readData(dataN, i * N)
            totaltime = time.time()
            stime = time.time()
            insertRegFromData(data, conn)
            print('Insert reg: ',time.time()-stime)
            stime = time.time()
            insertAreaFromData(data, conn)
            print('Insert area: ',time.time() - stime)
            stime = time.time()
            insertTerFromData(data, conn)
            print('Insert ter: ',time.time() - stime)
            stime = time.time()
            insertEoFromData(data, conn)
            print('Insert eo: ', time.time() - stime)
            stime = time.time()
            insertPtFromData(data, conn)
            print('Insert pt: ',time.time() - stime)
            stime = time.time()
            insertStudentsFromData(data, conn, year)
            print('Insert students: ',time.time() - stime)
            stime = time.time()
            if 'umltest' in dataN.columns:
                insertUmlFromData(data, conn)
                print('Insert uml: ',time.time() - stime)
                stime = time.time()
            insertUkrFromData(data, conn)
            print('Insert ukr: ',time.time() - stime)
            stime = time.time()
            if 'mathtest' in dataN.columns:
                insertMathFromData(data, conn)
                print('Insert math: ', time.time() - stime)
                stime = time.time()
            if 'mathsttest' in dataN.columns:
                insertMathstFromData(data, conn)
                print('Insert mathst: ', time.time() - stime)
                stime = time.time()

            for sub in sub1:
                if sub+'test' in dataN.columns:
                    insertSubFromData(data, sub, conn)
            for sub in subLang:
                if sub+'test' in dataN.columns:
                    insertSubLFromData(data, sub, conn)
            print('Insert other: ',time.time() - stime)
            print(f"Insert {i*N}. Total time: {(time.time()-totaltime):.2f}" )
            print('_____________________')
            conn.commit()

            if len(data.values) < N or i > 3:
                print(f'All data committed to the database!')
                break
            i += 1
        except psycopg2.OperationalError as error:
            logging.error('Connection to DB lost. Try connect in 5 s')
            time.sleep(5)
        except (Exception, psycopg2.DatabaseError) as error:
            i += 1
            conn.rollback()


def compareQueryToCsv():
    compareQuery = f'''SELECT year, TRIM(regname), ROUND(CAST(AVG(phys_test.ball100) AS numeric), 2) FROM students
        LEFT JOIN phys_test ON phys_test.student_id=students.id
        WHERE phys_test.teststatus='Зараховано'
        GROUP BY year, regname
        ORDER BY regname'''

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
