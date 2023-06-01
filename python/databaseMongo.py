from pymongo import MongoClient
import csv
from bson.son import SON
import chardet

client = MongoClient('mongodb://localhost:27017/', unicode_decode_error_handler='ignore')
database = client['ZNOdataM']  # Замініть 'mydatabase' на назву вашої бази даних
collection = database['znodata']

def writeDataToMongoDB(filename, year):
    with open(filename, 'rb') as rawdata:
        result = chardet.detect(rawdata.read(10000))
    enc = result.get('encoding')
    i = 0
    with open(filename, 'r', encoding=enc) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        fieldnames = [key.lower() for key in reader.fieldnames]
        reader.fieldnames = fieldnames
        for row in reader:
            if i > 1000: break
            i += 1
            row = {key: value.replace(',', '.') if isinstance(value, str) else value for key, value in
                           row.items()}
            row['year'] = year
            collection.insert_one(row)

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
        if r['averagebal'] is None: continue
        resLst.append(r)
        # year = doc["_id"]["year"]
        # region = doc["_id"]["regname"]
        # average_phys_bal = doc["averagebal"]
        # print(f"Year: {year}, Region: {region}, Average phys_bal: {average_phys_bal}")
    print(resLst)
    return resLst

def addNewStudent(outid, birth, year, sextypename, classprofilename, classlangname, regtypename,
                  regname, areaname, tername, eoname=None, eotypename=None, eoparent=None, eoregname=None, eoareaname=None, eotername=None):
    student = {
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
        query["year"] = year
    if eoname is not None and eoname != '':
        query["eoname"] = eoname
    if eoregname is not None and eoregname != '':
        query["eoregname"] = eoregname
    if eoparent is not None and eoparent != '':
        query["eoparent"] = eoparent
    if regname is not None and regname != '':
        query["regname"] = regname

    return list(collection.find(query))

def getSubByParams(sub, outid, teststatus, ptname):
    query = {}

    if outid is not None and outid != '':
        query["outid"] = outid
    if teststatus is not None and teststatus != '':
        query[sub + "teststatus"] = teststatus
    if ptname is not None and ptname != '':
        query[sub+"ptname"] = ptname

    return list(collection.find(query))

# file = 'resources/Odata2019File.csv'
# writeDataToMongoDB(file, 2019)
# file = 'resources/Odata2021File.csv'
# writeDataToMongoDB(file, 2021)
# makeRequest('umlball100')
# result = collection.delete_many({})
