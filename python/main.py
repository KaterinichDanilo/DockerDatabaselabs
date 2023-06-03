import os
import os.path
import shutil
import time
import logging

import requests
import yaml
from py7zr import unpack_7zarchive

from database import insertDataIntoDB, compareQueryToCsv
import app
from  databaseMongo import writeDataToMongoDB

url = 'https://zno.testportal.com.ua/yearstat/uploads/'
filename = 'OpenDataZNO____.7z'
filenamecsv = 'Odata____File.csv'
years = [2021, 2019]
path = '/resources'
tableName = 'znodata'

def downloadFiles():
    shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
    startTime = time.time()

    try:
        for year in years:
            if os.path.isfile(path + '/' + filenamecsv.replace('____', str(year))):
                print(f'File {filenamecsv.replace("____", str(year))} already downloaded!')
                continue

            print(f'''Downloading {filename.replace('____', str(year))}...''')
            r = requests.get(url + filename.replace('____', str(year)), stream=True)
            print(f'File downloaded. Time {(time.time() - startTime):.2f} s')

            if r.status_code == 200:
                print(f'''Unpacking {filename.replace('____', str(year))}...''')
                with open(filename.replace('____', str(year)), 'wb') as out:
                    out.write(r.content)
                shutil.unpack_archive(filename.replace('____', str(year)), path)
                print(f'Unpacking finished. Time {(time.time() - startTime):.2f} s')
            else:
                print('Request failed: %d' % r.status_code)
    except Exception as e:
        raise e



def writeDataToDb():
    for y in years:
        insertDataIntoDB(path + '/' + filenamecsv.replace('____', str(y)), y)
        writeDataToMongoDB(path + '/' + filenamecsv.replace('____', str(y)), y)


if __name__ == '__main__':
    logging.info('Python started!!!!')
    try:
        downloadFiles()
        writeDataToDb()
        app.run()
    except Exception as e:
        print(e)
