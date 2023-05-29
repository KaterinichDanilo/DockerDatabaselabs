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

def makeRequest(subject):
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

def addNewStudent(outid, birth, year, sextypename, classprofilename, classlangname, regtypename):
    return
# file = 'resources/Odata2019File.csv'
# writeDataToMongoDB(file, 2019)
# file = 'resources/Odata2021File.csv'
# writeDataToMongoDB(file, 2021)
makeRequest('umlball100')
# result = collection.delete_many({})