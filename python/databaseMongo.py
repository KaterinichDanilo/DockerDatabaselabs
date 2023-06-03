from pymongo import MongoClient
import csv
from bson.son import SON
import chardet

client = MongoClient('mongodb://mongodb:27017/', unicode_decode_error_handler='ignore')
# client = MongoClient('mongodb://localhost:27017/', unicode_decode_error_handler='ignore')
database = client['ZNOdata']
collection = database['znodata']

def writeDataToMongoDB(filename, year):
    print(f"Writing data from {filename} to MongoDB")
    with open(filename, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(10000))
    enc = result.get('encoding')
    i = 0
    with open(filename, 'r', encoding=enc) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        fieldnames = [key.lower() for key in reader.fieldnames]
        reader.fieldnames = fieldnames
        for row in reader:
            try:
                if i > 10: break
                i += 1
                outid = row['outid']
                existing_record = collection.find_one({'outid': outid})
                if existing_record:
                    print(f"Record with outid {outid} already exists. Skipping insertion.")
                else:
                    row['_id'] = outid
                    row['year'] = year
                    row = {key: value.replace(',', '.') if isinstance(value, str) else value for key, value in
                           row.items()}
                    collection.insert_one(row)
            except Exception as e:
                print(e)

        print("Writing to MongoDB finished")
        print(f'Amount: {collection.count_documents({})}')

def getAvgSub(subject):
    if subject == 'mathst':
        subject = 'mathstball12'
    else: subject += 'ball100'
    pipeline = [
        {
            '$match': {
                f'{subject}': {'$ne': 'null'}
            }
        },
        {"$group": {
            "_id": {
                "year": "$year",
                "regname": "$regname"
            },
            'averagebal': {'$avg': {'$toDouble': f'${subject}'}}
        }},
        {"$sort": SON([("_id.regname", 1), ("_id.year", 1)])}
    ]

    result = collection.aggregate(pipeline)
    resLst = []
    for doc in result:
        r = []
        r.append(doc["_id"]["regname"])
        r.append(doc["_id"]["year"])
        r.append(doc["averagebal"])
        if r[2] is None: continue
        resLst.append(r)
        # year = doc["_id"]["year"]
        # region = doc["_id"]["regname"]
        # average_phys_bal = doc["averagebal"]
        # print(f"Year: {year}, Region: {region}, Average phys_bal: {average_phys_bal}")
    return resLst

def addNewStudent(outid, birth, year, sextypename, classprofilename, classlangname, regtypename,
                  regname, areaname, tername, eoname=None, eotypename=None, eoparent=None, eoregname=None, eoareaname=None, eotername=None):
    student = {
        '_id': outid,
        'outid': outid,
        'birth': birth,
        'year': year,
        'sextypename': sextypename,
        'classprofilename': classprofilename,
        'classlangname': classlangname,
        'regtypename': regtypename,
        'eoname': eoname,
        'eotypename': eotypename,
        'eoparent': eoparent,
        'eoregname': eoregname,
        'eoareaname': eoareaname,
        'eotername': eotername,
        'regname': regname,
        'areaname': areaname,
        'tername': tername,
    }

    for key, value in student.items():
        if value == '':
            student[key] = None

    try:
        collection.insert_one(student)
    except Exception as e:
        print(e)
        raise e

def deleteStudent(outid):
    collection.delete_one({"outid": outid})

def updateStudent(outid, birth, year, sextypename, classprofilename, classlangname, regtypename,
                  eoname=None, eotypename=None, eoparent=None, eoregname=None, eoareaname=None, eotername=None):
    update_fields = {}

    if birth is not None and birth != '':
        update_fields["birth"] = birth
    if year is not None and year != '':
        update_fields["year"] = year
    if sextypename is not None and sextypename != '':
        update_fields["sextypename"] = sextypename
    if classprofilename is not None and classprofilename != '':
        update_fields["classprofilename"] = classprofilename
    if classlangname is not None and classlangname != '':
        update_fields["classlangname"] = classlangname
    if regtypename is not None and regtypename != '':
        update_fields["regtypename"] = regtypename
    if eoname is not None:
        update_fields["eoname"] = eoname
    if eotypename is not None:
        update_fields["eotypename"] = eotypename
    if eoparent is not None:
        update_fields["eoparent"] = eoparent
    if eoareaname is not None:
        update_fields["eoareaname"] = eoareaname
    if eoregname is not None:
        update_fields["eoregname"] = eoregname
    if eotername is not None:
        update_fields["eotername"] = eotername
    if update_fields:
        collection.update_one(
            {"outid": outid},
            {"$set": update_fields}
        )

def getStudentsByParams(outid, year, regname, eoname=None, eoparent=None, eoregname=None):
    query = {}

    if outid is not None and outid != '':
        query["outid"] = outid
    if year is not None and year != '':
        query["year"] = int(year)
    if eoname is not None and eoname != '':
        query["eoname"] = eoname
    if eoregname is not None and eoregname != '':
        query["eoregname"] = eoregname
    if eoparent is not None and eoparent != '':
        query["eoparent"] = eoparent
    if regname is not None and regname != '':
        query["regname"] = regname

    resList = list(collection.find(query))
    print(f'getStudentsByParams : {outid}, {year}, {regname}, {eoname}, {eoparent}, {eoregname}')
    print(query)
    print(f'return list size = {len(resList)}')
    return resList

def getSubByParams(sub, outid, teststatus, ptname):
    query = {}

    if outid is not None and outid != '':
        query["outid"] = outid
    if teststatus is not None and teststatus != '':
        query[sub + "teststatus"] = teststatus
    if ptname is not None and ptname != '':
        query[sub+"ptname"] = ptname

    resList = list(collection.find(query))
    print(f'getSubByParams : {sub}, {outid}, {teststatus}, {ptname}')
    print(query)
    print(f'return list size = {len(resList)}')

    return resList

def addUmlTest(student_id, teststatus, ball100, ball12, ball, adaptscale, ptname):
    uml_fields = {}

    if teststatus != '': uml_fields["umlteststatus"] = teststatus
    else: uml_fields["umlteststatus"] = None
    if ball100 != '': uml_fields["umlball100"] = ball100
    else: uml_fields["umlball100"] = None
    if ball12 != '': uml_fields["umlball12"] = ball12
    else: uml_fields["umlball12"] = None
    if ball != '': uml_fields["umlball"] = ball
    else: uml_fields["umlball"] = None
    if ptname != '': uml_fields["umlptname"] = ptname
    else: uml_fields["umlptname"] = None
    if adaptscale != '': uml_fields["umladaptscale"] = adaptscale
    else: uml_fields["umladaptscale"] = None
    uml_fields['umltest'] = "Українська мова і література"

    if uml_fields:
        collection.update_one(
            {"outid": student_id},
            {"$set": uml_fields}
        )
    print(f'addUmlTest query: {uml_fields}')

def updateUmlTest(student_id, teststatus, ball100, ball12, ball, adaptscale, ptname):
    update_fields = {}

    if teststatus is not None and teststatus != '':
        update_fields["umlteststatus"] = teststatus
    if ball100 is not None and ball100 != '':
        update_fields["umlball100"] = ball100
    if ball12 is not None and ball12 != '':
        update_fields["umlball12"] = ball12
    if ball is not None and ball != '':
        update_fields["umlball"] = ball
    if adaptscale is not None and adaptscale != '':
        update_fields["umladaptscale"] = adaptscale
    if ptname is not None and ptname != '':
        update_fields["umlptname"] = ptname
    if update_fields:
        collection.update_one(
            {"outid": student_id},
            {"$set": update_fields}
        )
    print(f'updateUmlTest query: {update_fields}')

def deleteTest(outid, sub):
    query = {'outid': outid}
    update = {'$unset':
                {sub+'test': "null",
                sub+'teststatus': "null",
                sub+'ball100': "null",
                sub+'ball12': "null",
                sub+'ball': "null",
                sub+'adaptscale': "null",
                sub+'ptname': "null",
                sub+"ptregname": "null",
                sub+"ptareaname": "null",
                sub+"pttername": "null",
                 sub +'lang': "null",
                 sub +'dpalevel': "null",
                 sub+"subtest": "null"
    }
              }
    collection.update_one(query, update)

def addUkrTest(student_id, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname):
    ukr_fields = {}

    if subtest != '': ukr_fields["ukrsubtest"] = subtest
    else: ukr_fields["ukrsubtest"] = None
    if teststatus != '': ukr_fields["ukrteststatus"] = teststatus
    else: ukr_fields["ukrteststatus"] = None
    if ball100 != '': ukr_fields["umlball100"] = ball100
    else: ukr_fields["ukrball100"] = None
    if ball12 != '': ukr_fields["umlball12"] = ball12
    else: ukr_fields["ukrball12"] = None
    if ball != '': ukr_fields["ukrball"] = ball
    else: ukr_fields["ukrball"] = None
    if ptname != '': ukr_fields["ukrptname"] = ptname
    else: ukr_fields["ukrptname"] = None
    if adaptscale != '': ukr_fields["ukradaptscale"] = adaptscale
    else: ukr_fields["ukradaptscale"] = None
    ukr_fields['ukrtest'] = "Українська мова"

    if ukr_fields:
        collection.update_one(
            {"outid": student_id},
            {"$set": ukr_fields}
        )

    print(f'addUkrTest query: {ukr_fields}')

def updateUkrTest(student_id, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname):
    update_fields = {}
    if subtest != '': update_fields["ukrsubtest"] = subtest
    if teststatus is not None and teststatus != '':
        update_fields["ukrteststatus"] = teststatus
    if ball100 is not None and ball100 != '':
        update_fields["ukrball100"] = ball100
    if ball12 is not None and ball12 != '':
        update_fields["ukrball12"] = ball12
    if ball is not None and ball != '':
        update_fields["ukrball"] = ball
    if adaptscale is not None and adaptscale != '':
        update_fields["ukradaptscale"] = adaptscale
    if ptname is not None and ptname != '':
        update_fields["ukrptname"] = ptname
    if update_fields:
        collection.update_one(
            {"outid": student_id},
            {"$set": update_fields}
        )
    print(f'updateUkrTest query: {update_fields}')

# print(getStudentsByParams("8a2abef7-625a-4253-8c14-000fe8a856e5", '', ''))
# print(getSubByParams('uml', "8a2abef7-625a-4253-8c14-000fe8a856e5", '', ''))
# updateUmlTest(student_id="8a2abef7-625a-4253-8c14-000fe8a856e5",
#               teststatus='TESSTSTATUS', ball100='999', ball12='', ball='', adaptscale='', ptname='')
# print(getSubByParams('uml', "8a2abef7-625a-4253-8c14-000fe8a856e5", '', ''))