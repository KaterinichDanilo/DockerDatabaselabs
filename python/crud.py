from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import redis
import json

redisHash = redis.Redis(host='redis', port=6379)

engine = create_engine('postgresql://postgres:1234@database:5432/ZNOdata')
# engine = create_engine('postgresql://postgres:1234@localhost:5432/ZNOdata')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

def put_in_hash_list_of_lists(hashName, lst, key, nameList):
    for i, l in enumerate(lst):
        keyHash = f'{key}{i}'
        s = ','.join(map(str, l))
        redisHash.hset(hashName, keyHash, s)

def get_from_hash_list_of_lists(res_in_hash):
    res = []

    for k, v in res_in_hash.items():
        subList = []
        for el in v.decode().split(','):
            if el.isdigit():
                subList.append(int(el))
            else:
                subList.append(el)
        res.append(subList)
    return res

def sessionRollback():
    session.rollback()
    session.begin()

def sessionCommit():
    session.commit()

class Reg(Base):
    __tablename__ = 'regs'
    regname = Column(String, primary_key=True)
    def to_string(self):
        return {
            'regname': self.regname
        }

class Area(Base):
    __tablename__ = 'areas'
    areaname = Column(String, primary_key=True)
    def to_string(self):
        return {
            'areaname': self.areaname
        }

class Ter(Base):
    __tablename__ = 'ters'
    tername = Column(String, primary_key=True)
    tertypename = Column(String)
    def to_string(self):
        return {
            'tername': self.tername,
            'tertypename': self.tertypename
        }

class Eo(Base):
    __tablename__ = 'eo'
    id = Column(Integer, primary_key=True, autoincrement=True)
    eoname = Column(String)
    eotypename = Column(String)
    eoparent = Column(String)
    regname = Column(String, ForeignKey('regs.regname'))
    areaname = Column(String, ForeignKey('areas.areaname'))
    tername = Column(String, ForeignKey('ters.tername'))
    def to_string(self):
        return {
            'id': self.id,
            'eoname': self.eoname,
            'eotypename': self.eotypename,
            'eoparent': self.eoparent,
            'regname': self.regname,
            'areaname': self.areaname,
            'tername': self.tername
        }

class Pt(Base):
    __tablename__ = 'pt'
    name = Column(String, primary_key=True)
    regname = Column(String, ForeignKey('regs.regname'))
    areaname = Column(String, ForeignKey('areas.areaname'))
    tername = Column(String, ForeignKey('ters.tername'))
    def to_string(self):
        return {
            'name': self.name,
            'regname': self.regname,
            'areaname': self.areaname,
            'tername': self.tername
        }

class Student(Base):
    __tablename__ = 'students'
    id = Column(String, primary_key=True)
    birth = Column(Integer)
    year = Column(Integer)
    sextypename = Column(String)
    classprofilename = Column(String)
    classlangname = Column(String)
    regtypename = Column(String)
    eo_id = Column(Integer, ForeignKey('eo.id'))
    regname = Column(String, ForeignKey('regs.regname'))
    areaname = Column(String, ForeignKey('areas.areaname'))
    tername = Column(String, ForeignKey('ters.tername'))

    def to_string(self):
        return {
            'id': self.id,
            'birth': self.birth,
            'year': self.year,
            'sextypename': self.sextypename,
            'classprofilename': self.classprofilename,
            'classlangname': self.classlangname,
            'regtypename': self.regtypename,
            'eo_id': self.eo_id,
            'regname': self.regname,
            'areaname': self.areaname,
            'tername': self.tername
        }

class Uml_test(Base):
    __tablename__ = 'uml_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    adaptscale = Column(Integer)
    ptname = Column(String, ForeignKey('pt.name'))
    def to_string(self):
        return {
            'id': self.id,
            'student_id': self.student_id,
            'test': self.test,
            'teststatus': self.teststatus,
            'ball100': self.ball100,
            'ball12': self.ball12,
            'ball': self.ball,
            'adaptscale': self.adaptscale,
            'ptname': self.ptname
        }

class Ukr_test(Base):
    __tablename__ = 'ukr_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    subtest = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    adaptscale = Column(Integer)
    ptname = Column(String, ForeignKey('pt.name'))

class Hist_test(Base):
    __tablename__ = 'hist_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Math_test(Base):
    __tablename__ = 'math_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    dpalevel = Column(String)
    ptname = Column(String, ForeignKey('pt.name'))

class Mathst_test(Base):
    __tablename__ = 'mathst_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Phys_test(Base):
    __tablename__ = 'phys_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Chem_test(Base):
    __tablename__ = 'chem_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Bio_test(Base):
    __tablename__ = 'bio_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Geo_test(Base):
    __tablename__ = 'geo_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    lang = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    ptname = Column(String, ForeignKey('pt.name'))

class Eng_test(Base):
    __tablename__ = 'eng_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    dpalevel = Column(String)
    ptname = Column(String, ForeignKey('pt.name'))

class Fra_test(Base):
    __tablename__ = 'fra_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    dpalevel = Column(String)
    ptname = Column(String, ForeignKey('pt.name'))

class Deu_test(Base):
    __tablename__ = 'deu_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    dpalevel = Column(String)
    ptname = Column(String, ForeignKey('pt.name'))

class Spa_test(Base):
    __tablename__ = 'spa_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String, ForeignKey('students.id'), unique=True)
    test = Column(String)
    teststatus = Column(String)
    ball100 = Column(Float)
    ball12 = Column(Float)
    ball = Column(Float)
    dpalevel = Column(String)
    ptname = Column(String, ForeignKey('pt.name'))

def create_reg(regname):
    reg = Reg(regname=regname)
    session.add(reg)
    session.commit()

def create_area(name):
    area = Area(areaname=name)
    session.add(area)
    session.commit()

def create_ter(tername, tertypename):
    ter = Ter(tername=tername, tertypename=tertypename)
    session.add(ter)
    session.commit()

def create_eo(eoname, eotypename, eoparent, regname, areaname, tername):
    eo = Eo(eoname=eoname, eotypename=eotypename, eoparent=eoparent,
            regname=regname, areaname=areaname, tername=tername)
    session.add(eo)
    session.commit()

def create_pt(name, regname, areaname, tername):
    pt = Pt(name=name, regname=regname, areaname=areaname, tername=tername)
    session.add(pt)
    session.commit()

def create_student(id, birth, year, sextypename, classprofilename, classlangname,
                   regtypename, eo_id, regname, areaname, tername):
    student = Student(id=id, birth=birth, year=year, sextypename=sextypename, classprofilename=classprofilename,
                      classlangname=classlangname, regtypename=regtypename, eo_id=eo_id,
                      regname=regname, areaname=areaname, tername=tername)
    session.add(student)
    session.commit()
def get_eo_by_id(id):
    eo = session.get(Eo, id)
    return eo

def get_student_by_id(id):
    eo = session.get(Student, id)
    return eo

def get_reg_by_name(name):
    reg = session.get(Reg, name)
    return reg

def get_uml_test_by_st_id(st_id):
    uml = session.query(Uml_test).filter(Uml_test.student_id == st_id).first()
    return uml

def get_students_by_params(id, year, regname, eo_id):
    keyHash = f'studentsList:{id}:{year}:{regname}:{eo_id}'
    students = redisHash.get(keyHash)
    # students = None
    print(students)
    if students:
        print('return json')
        print(json.loads(students))
        return json.loads(students)
    query = session.query(Student)
    if id != '':
        query = query.filter(Student.id == id)
    if year != '':
        query = query.filter(Student.year == year)
    if regname != '':
        query = query.filter(Student.regname == regname)
    if eo_id != '':
        query = query.filter(Student.eo_id == int(eo_id))
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results

def create_student(id, birth, year, sextypename, classprofilename, classlangname,
                   regtypename, eo_id, regname, areaname, tername):
    try:
        id = id if id != '' else None
        birth = birth if birth != '' else None
        year = year if year != '' else None
        sextypename = sextypename if sextypename != '' else None
        classprofilename = classprofilename if classprofilename != '' else None
        classlangname = classlangname if classlangname != '' else None
        regtypename = regtypename if regtypename != '' else None
        eo_id = eo_id if eo_id != '' else None
        regname = regname if regname != '' else None
        areaname = areaname if areaname != '' else None
        tername = tername if tername != '' else None

        new_student = Student(id=id, birth=birth, year=year, sextypename=sextypename, classprofilename=classprofilename,
                              classlangname=classlangname, regtypename=regtypename, eo_id=eo_id, regname=regname,
                              areaname=areaname, tername=tername)
        session.add(new_student)
        print('created st')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def update_student(student, birth, year, sextypename, classprofilename, classlangname,
                   regtypename, eo_id, regname, areaname, tername):
    if birth != '': student.birth = birth
    if year != '': student.year = year
    if sextypename != '': student.sextypename = sextypename
    if classprofilename != '': student.classprofilename = classprofilename
    if classlangname != '': student.classlangname = classlangname
    if regtypename != '': student.regtypename = regtypename
    if eo_id != '': student.eo_id = eo_id
    if regname != '': student.regname = regname
    if areaname != '': student.areaname = areaname
    if tername != '': student.tername = tername
    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_student(student):
    try:
        session.delete(student)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e

def get_regs_by_params(regname):
    keyHash = f'regsList:{regname}'
    regs = redisHash.get(keyHash)
    print(regs)
    if regs:
        return json.loads(regs)
    query = session.query(Reg)
    if regname != '':
        query = query.filter(Reg.regname == regname)
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results

def create_reg(regname):
    try:
        regname = regname if regname != '' else None
        new_reg = Reg(regname=regname)
        session.add(new_reg)
        print('created reg')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def update_reg(region, new_regname):
    if new_regname != '': region.regname = new_regname
    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_reg(reg):
    try:
        session.delete(reg)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e

def get_area_by_name(areaname):
    area = session.get(Area, areaname)
    return area
def get_areas_by_params(areaname):
    keyHash = f'areasList:{areaname}'
    areas = redisHash.get(keyHash)
    print(areas)
    if areas:
        return json.loads(areas)
    query = session.query(Area)
    if areaname != '':
        query = query.filter(Area.areaname == areaname)
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results

def create_area(areaname):
    try:
        areaname = areaname if areaname != '' else None
        new_area = Area(areaname=areaname)
        session.add(new_area)
        print('created area')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def update_area(area, new_areaname):
    if new_areaname != '': area.regname = new_areaname
    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_area(area):
    session.delete(area)
    session.commit()
    redisHash.flushall()
    session.commit()

def get_ters_by_params(tername, tertypename):
    keyHash = f'tersList:{tername}:{tertypename}'
    ters = redisHash.get(keyHash)
    print(ters)
    if ters:
        return json.loads(ters)
    query = session.query(Ter)
    if tername != '':
        query = query.filter(Ter.tername == tername)
    if tertypename != '':
        query = query.filter(Ter.tertypename == tertypename)
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results

def get_ter_by_name(tername):
    ter = session.get(Ter, tername)
    return ter
def create_ter(tername, tertypename):
    try:
        tername = tername if tername != '' else None
        tertypename = tertypename if tertypename != '' else None
        new_ter = Ter(tername=tername, tertypename=tertypename)
        session.add(new_ter)
        print('created ter')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def update_ter(ter, new_tername, new_tertypename):
    if new_tername != '': ter.tername = new_tername
    if new_tertypename != '': ter.tertypename = new_tertypename
    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_ter(ter):
    try:
        session.delete(ter)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e

nameListAvg = ['regname', 'year', 'avg']
def get_avg_uml():
    hashName = 'avg_uml'
    keyHash = 'avg_uml_key'
    avg_umlL = redisHash.hgetall(hashName)
    if avg_umlL:
        print('return json avg_uml')
        res = get_from_hash_list_of_lists(avg_umlL)
        print(res)
        return res

    results = session.query(Pt.regname, Student.year, func.avg(Uml_test.ball100)). \
        select_from(Student). \
        join(Uml_test, Student.id == Uml_test.student_id). \
        join(Pt, Uml_test.ptname == Pt.name).filter(Uml_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()

    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results



def get_avg_ukr():
    hashName = 'avg_ukr'
    keyHash = 'avg_ukr_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        print('return json avg_ukr')
        res = get_from_hash_list_of_lists(avg)
        print(res)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Ukr_test.ball100)).select_from(Student)\
        .join(Ukr_test, Student.id == Ukr_test.student_id). \
        join(Pt, Ukr_test.ptname == Pt.name).filter(Ukr_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_math():
    hashName = 'avg_math'
    keyHash = 'avg_math_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        print('return json avg_math')
        res = get_from_hash_list_of_lists(avg)
        print(res)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Math_test.ball100)). \
        select_from(Student). \
        join(Math_test, Student.id == Math_test.student_id). \
        join(Pt, Math_test.ptname == Pt.name).filter(Math_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_mathst():
    hashName = 'avg_mathst'
    keyHash = 'avg_mathst_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        print('return json avg_math')
        res = get_from_hash_list_of_lists(avg)
        print(res)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Mathst_test.ball12)). \
        select_from(Student). \
        join(Mathst_test, Student.id == Mathst_test.student_id). \
        join(Pt, Mathst_test.ptname == Pt.name).filter(Mathst_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_hist():
    hashName = 'avg_hist'
    keyHash = 'avg_hist_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Hist_test.ball100)). \
        select_from(Student). \
        join(Hist_test, Student.id == Hist_test.student_id). \
        join(Pt, Hist_test.ptname == Pt.name).filter(Hist_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_phys():
    hashName = 'avg_phys'
    keyHash = 'avg_phys_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Phys_test.ball100)). \
        select_from(Student). \
        join(Phys_test, Student.id == Phys_test.student_id). \
        join(Pt, Phys_test.ptname == Pt.name).filter(Phys_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_chem():
    hashName = 'avg_chem'
    keyHash = 'avg_chem_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Chem_test.ball100)). \
        select_from(Student). \
        join(Chem_test, Student.id == Chem_test.student_id). \
        join(Pt, Chem_test.ptname == Pt.name).filter(Chem_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_bio():
    hashName = 'avg_bio'
    keyHash = 'avg_bio_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Bio_test.ball100)). \
        select_from(Student). \
        join(Bio_test, Student.id == Bio_test.student_id). \
        join(Pt, Bio_test.ptname == Pt.name).filter(Bio_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_geo():
    hashName = 'avg_geo'
    keyHash = 'avg_geo_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Geo_test.ball100)). \
        select_from(Student). \
        join(Geo_test, Student.id == Geo_test.student_id). \
        join(Pt, Geo_test.ptname == Pt.name).filter(Geo_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_eng():
    hashName = 'avg_eng'
    keyHash = 'avg_eng_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Eng_test.ball100)). \
        select_from(Student). \
        join(Eng_test, Student.id == Eng_test.student_id). \
        join(Pt, Eng_test.ptname == Pt.name).filter(Eng_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_fra():
    hashName = 'avg_fra'
    keyHash = 'avg_fra_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Fra_test.ball100)). \
        select_from(Student). \
        join(Fra_test, Student.id == Fra_test.student_id). \
        join(Pt, Fra_test.ptname == Pt.name).filter(Fra_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_deu():
    hashName = 'avg_deu'
    keyHash = 'avg_deu_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Deu_test.ball100)). \
        select_from(Student). \
        join(Deu_test, Student.id == Deu_test.student_id). \
        join(Pt, Deu_test.ptname == Pt.name).filter(Deu_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

def get_avg_spa():
    hashName = 'avg_spa'
    keyHash = 'avg_spa_key'
    avg = redisHash.hgetall(hashName)
    if avg:
        res = get_from_hash_list_of_lists(avg)
        return res
    results = session.query(Pt.regname, Student.year, func.avg(Spa_test.ball100)). \
        select_from(Student). \
        join(Spa_test, Student.id == Spa_test.student_id). \
        join(Pt, Spa_test.ptname == Pt.name).filter(Spa_test.teststatus == 'Зараховано'). \
        group_by(Pt.regname, Student.year).order_by(Pt.regname).all()
    put_in_hash_list_of_lists(hashName, results, keyHash, nameListAvg)
    return results

# EO
def get_eo_by_params(id, eoname, eotypename, regname, areaname, tername):
    try:
        keyHash = f'eoList:{id}:{eoname}:{eotypename}:{regname}:{areaname}:{tername}'
        eo = redisHash.get(keyHash)
        print(eo)
        if eo:
            return json.loads(eo)
        query = session.query(Eo)
        if id != '':
            query = query.filter(Eo.id == id)
        if eoname != '':
            query = query.filter(Eo.eoname == eoname)
        if eotypename != '':
            query = query.filter(Eo.eotypename == eotypename)
        if regname != '':
            query = query.filter(Eo.regname == regname)
        if areaname != '':
            query = query.filter(Eo.areaname == areaname)
        if tername != '':
            query = query.filter(Eo.tername == tername)
        results = query.all()
        results = [x.to_string() for x in results]
        print(results)
        redisHash.set(keyHash, json.dumps(results))
    except Exception as e:
        print(e)
    return results

def create_eo(eoname, eotypename, eoparent, regname, areaname, tername):
    try:
        eoname = eoname if eoname != '' else None
        eotypename = eotypename if eotypename != '' else None
        eoparent = eoparent if eoparent != '' else None
        regname = regname if regname != '' else None
        areaname = areaname if areaname != '' else None
        tername = tername if tername != '' else None

        new_eo = Eo(eoname=eoname, eotypename=eotypename, eoparent=eoparent, regname=regname,
                         areaname=areaname, tername=tername)
        session.add(new_eo)
        print('created eo')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def update_eo(eo, eotypename, eoparent, regname, areaname, tername):
    if eotypename != '': eo.eotypename = eotypename
    if eoparent != '': eo.eoparent = eoparent
    if regname != '': eo.regname = regname
    if areaname != '': eo.areaname = areaname
    if tername != '': eo.tername = tername

    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
        raise e

def delete_eo(eo):
    try:
        session.delete(eo)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e

# PT
def get_pt_by_params(ptname, regname, areaname, tername):
    keyHash = f'ptList:{ptname}:{regname}:{areaname}:{tername}'
    pt = redisHash.get(keyHash)
    print(pt)
    if pt:
        return json.loads(pt)
    query = session.query(Pt)
    if ptname != '':
        query = query.filter(Pt.eoname == ptname)
    if regname != '':
        query = query.filter(Eo.regname == regname)
    if areaname != '':
        query = query.filter(Eo.areaname == areaname)
    if tername != '':
        query = query.filter(Eo.tername == tername)
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results

def create_pt(ptname, regname, areaname, tername):
    try:
        ptname = ptname if ptname != '' else None
        regname = regname if regname != '' else None
        areaname = areaname if areaname != '' else None
        tername = tername if tername != '' else None

        new_pt = Pt(name=ptname, regname=regname,
                    areaname=areaname, tername=tername)
        session.add(new_pt)
        print('created pt')
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e


def update_pt(pt, ptname, regname, areaname, tername):
    if ptname != '': pt.ptname = ptname
    if regname != '': pt.regname = regname
    if areaname != '': pt.areaname = areaname
    if tername != '': pt.tername = tername

    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_pt(pt):
    try:
        session.delete(pt)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e


# UML
def get_uml_by_id(id):
    uml = session.get(Uml_test, id)
    return uml
def get_uml_by_params(id, teststatus, ptname):
    keyHash = f'ptList:{id}:{teststatus}:{ptname}'
    uml = redisHash.get(keyHash)
    print(uml)
    if uml:
        return json.loads(uml)
    query = session.query(Uml_test)
    if ptname != '':
        query = query.filter(Uml_test.student_id == id)
    if teststatus != '':
        query = query.filter(Uml_test.teststatus == teststatus)
    if ptname != '':
        query = query.filter(Uml_test.ptname == ptname)
    results = query.all()
    results = [x.to_string() for x in results]
    print(results)
    redisHash.set(keyHash, json.dumps(results))
    return results


def create_uml(student_id, teststatus, ball100, ball12, ball, adaptscale, ptname):
    student_id = student_id if student_id != '' else None
    teststatus = teststatus if teststatus != '' else None
    ball100 = ball100 if ball100 != '' else None
    ball12 = ball12 if ball12 != '' else None
    ball = ball if ball != '' else None
    adaptscale = adaptscale if adaptscale != '' else None
    ptname = ptname if ptname != '' else None

    try:
        new_uml = Uml_test(student_id=student_id, teststatus=teststatus,
                           ball100=ball100, ball12=ball12, ball=ball, adaptscale=adaptscale, ptname=ptname)
        session.add(new_uml)
        print('created uml')
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e

def update_uml(uml, student_id, teststatus, ball100, ball12, ball, adaptscale, ptname):
    if student_id != '': uml.student_id = student_id
    if teststatus != '': uml.teststatus = teststatus
    if ball100 != '': uml.ball100 = ball100
    if ball12 != '': uml.ball12 = ball12
    if ball != '': uml.ball = ball
    if adaptscale != '': uml.adaptscale = adaptscale
    if ptname != '': uml.ptname = ptname

    try:
        session.commit()
        redisHash.flushall()
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

def delete_uml(uml):
    try:
        session.delete(uml)
        session.commit()
        redisHash.flushall()
    except Exception as e:
        raise e