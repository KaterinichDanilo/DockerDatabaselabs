from flask import Flask, render_template, redirect, url_for, request, jsonify
import crud

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/getallstudents", methods=['GET'])
def get_all_students():
    studList = crud.get_all_students()
    print("Get")
    return render_template("index.html", students=studList)

@app.route("/getstudentsbyparams", methods=['GET'])
def get_students_by_params():
    print(request)
    id = request.args['id']
    year = request.args['year']
    regname = request.args['regname']
    eo_id = request.args['eo_id']
    studList = crud.get_students_by_params(id, year, regname, eo_id)
    size = len(studList)
    return render_template('index.html', size=size, students=studList)
    # data = {'studentsList': studList, 'size': size}
    # return jsonify(data)


@app.route("/newstudent", methods=['POST'])
def new_student():
    print(request)
    id = request.form['id']
    birth = request.form['birth']
    year = request.form['year']
    sextypename = request.form['sextypename']
    classprofilename = request.form['classprofilename']
    classlangname = request.form['classlangname']
    regtypename = request.form['regtypename']
    eo_id = request.form['eo_id']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    try:
        crud.create_student(id, birth, year, sextypename, classprofilename, classlangname,
                            regtypename, eo_id, regname, areaname, tername)
        return render_template('index.html', addStudentMessage='Студента додано')
    except Exception as e:
        print("Помилка при додаванні студента:", str(e))
        return render_template('index.html', addStudentMessage='Сталася помилка')

@app.route("/updatestudent", methods=['POST'])
def update_student():
    id = request.form['id']
    birth = request.form['birth']
    year = request.form['year']
    sextypename = request.form['sextypename']
    classprofilename = request.form['classprofilename']
    classlangname = request.form['classlangname']
    regtypename = request.form['regtypename']
    eo_id = request.form['eo_id']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    student = crud.get_student_by_id(id)
    if student:
        try:
            crud.update_student(student, birth, year, sextypename, classprofilename, classlangname,
                   regtypename, eo_id, regname, areaname, tername)
            return render_template('index.html', updateStudentMessage='Студента оновлено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', updateStudentMessage='Сталася помилка')
    else: return render_template('index.html', updateStudentMessage='Студента з таким id не знайдено')

@app.route("/deletestudent", methods=['POST'])
def delete_student():
    print(request)
    id = request.form['id']
    student = crud.get_student_by_id(id)
    if student:
        try:
            crud.delete_student(student)
            return render_template('index.html', deleteStudentMessage='Студента видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deleteStudentMessage='Сталася помилка')
    else: return render_template('index.html', deleteStudentMessage='Студента з таким id не знайдено')

@app.route("/getavgball", methods=['GET'])
def get_avg_ball():
    subjectsDict = {'uml': crud.get_avg_uml,
                    'ukr': crud.get_avg_ukr,
                    'hist': crud.get_avg_hist,
                    'math': crud.get_avg_math,
                    'mathst': crud.get_avg_mathst,
                    'phys': crud.get_avg_phys,
                    'chem': crud.get_avg_chem,
                    'bio': crud.get_avg_bio,
                    'geo': crud.get_avg_geo,
                    'eng': crud.get_avg_eng,
                    'fra': crud.get_avg_fra,
                    'deu': crud.get_avg_deu,
                    'spa': crud.get_avg_spa}

    sub = request.args['sub']
    print(sub)
    reg = request.args['regname']
    print(reg)
    results = subjectsDict.get(sub)()
    print(results)
    results.sort(key=lambda x: (x[0], x[1]))
    if reg == 'Всі':
        return render_template('index.html', avgBallList=results)
    results = [x for x in results if x[0].strip() == reg]
    print(results)
    return render_template('index.html', avgBallList=results)

@app.route("/getregsbyparams", methods=['GET'])
def get_regs_by_params():
    print(request)
    regname = request.args['regname']
    regList = crud.get_regs_by_params(regname)
    size = len(regList)
    return render_template('index.html', size=size, regs=regList)

@app.route("/newreg", methods=['POST'])
def new_reg():
    print(request)
    regname = request.form['regname']

    try:
        crud.create_reg(regname)
        return render_template('index.html', addRegMessage='Регіон додано')
    except Exception as e:
        print("Помилка при додаванні регіону:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addRegMessage='Сталася помилка')

@app.route("/updatereg", methods=['POST'])
def update_reg():
    regname = request.form['regname']
    newregname = request.form['newregname']
    reg = crud.get_reg_by_name(regname)
    if reg:
        try:
            crud.update_reg(reg, newregname)
            return render_template('index.html', updateRegMessage='Регіон оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updateRegMessage='Сталася помилка')
    else: return render_template('index.html', updateRegMessage='Регіон з такою назвою не знайдено')

@app.route("/deletereg", methods=['POST'])
def delete_reg():
    print(request)
    regname = request.form['regname']
    reg = crud.get_reg_by_name(regname)
    if reg:
        try:
            crud.delete_reg(reg)
            return render_template('index.html', deleteRegMessage='Регіон видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deleteRegMessage='Сталася помилка')
    else: return render_template('index.html', deleteRegMessage='Регіон з такою назвою не знайдено')
# Area
@app.route("/getareasbyparams", methods=['GET'])
def get_areas_by_params():
    print(request)
    areaname = request.args['areaname']
    areaList = crud.get_areas_by_params(areaname)
    size = len(areaList)
    return render_template('index.html', size=size, areas=areaList)

@app.route("/newarea", methods=['POST'])
def new_area():
    print(request)
    areaname = request.form['areaname']

    try:
        crud.create_area(areaname)
        return render_template('index.html', addAreaMessage='Область додано')
    except Exception as e:
        print("Помилка при додаванні області:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addAreaMessage='Сталася помилка')

@app.route("/updatearea", methods=['POST'])
def update_area():
    areaname = request.form['areaname']
    newareaname = request.form['newareaname']
    area = crud.get_area_by_name(areaname)
    if area:
        try:
            crud.update_area(area, newareaname)
            return render_template('index.html', updateAreaMessage='Область оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updateAreaMessage='Сталася помилка')
    else: return render_template('index.html', updateAreaMessage='Область з такою назвою не знайдено')

@app.route("/deletearea", methods=['POST'])
def delete_area():
    print(request)
    areaname = request.form['areaname']
    area = crud.get_area_by_name(areaname)
    if area:
        try:
            crud.delete_area(area)
            return render_template('index.html', deleteAreaMessage='Область видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deleteAreaMessage='Сталася помилка')
    else: return render_template('index.html', deleteAreaMessage='Область з такою назвою не знайдено')
# Ter
@app.route("/getterbyparams", methods=['GET'])
def get_ters_by_params():
    print(request)
    tername = request.args['tername']
    tertypename = request.args['tertypename']
    terList = crud.get_ters_by_params(tername, tertypename)
    size = len(terList)
    return render_template('index.html', size=size, ters=terList)

@app.route("/newter", methods=['POST'])
def new_ter():
    print(request)
    tername = request.form['tername']
    tertypename = request.form['tertypename']

    try:
        crud.create_ter(tername, tertypename)
        return render_template('index.html', addTerMessage='Населений пункт додано')
    except Exception as e:
        print("Помилка при додаванні населеного пункту:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addTerMessage='Сталася помилка')

@app.route("/updateter", methods=['POST'])
def update_ter():
    tername = request.form['tername']
    newtername = request.form['newtername']
    tertypename = request.form['tertypename']
    ter = crud.get_ter_by_name(tername)
    if ter:
        try:
            crud.update_ter(ter, newtername, tertypename)
            return render_template('index.html', updateTerMessage='Населений пункт оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updateTerMessage='Сталася помилка')
    else: return render_template('index.html', updateTerMessage='Населений пункт з такою назвою не знайдено')

@app.route("/deleteter", methods=['POST'])
def delete_ter():
    print(request)
    tername = request.form['tername']
    ter = crud.get_ter_by_name(tername)
    if ter:
        try:
            crud.delete_ter(ter)
            return render_template('index.html', deleteTerMessage='Населений пункт видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deleteTerMessage='Сталася помилка')
    else: return render_template('index.html', deleteTerMessage='Населений пункт з такою назвою не знайдено')

# EO
@app.route("/geteobyparams", methods=['GET'])
def get_eo_by_params():
    print(request)
    id = request.args['id']
    eoname = request.args['eoname']
    eotypename = request.args['eotypename']
    regname = request.args['regname']
    areaname = request.args['areaname']
    tername = request.args['tername']
    eoList = crud.get_eo_by_params(id, eoname, eotypename, regname, areaname, tername)
    size = len(eoList)
    return render_template('index.html', size=size, eoList=eoList)

@app.route("/neweo", methods=['POST'])
def new_eo():
    print(request)
    eoname = request.form['eoname']
    eotypename = request.form['eotypename']
    eoparent = request.form['eoparent']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    try:
        crud.create_eo(eoname, eotypename, eoparent, regname, areaname, tername)
        return render_template('index.html', addEoMessage='Навчальний заклад додано')
    except Exception as e:
        print("Помилка при додаванні навчального закладу:", str(e))
        return render_template('index.html', addEoMessage='Сталася помилка')

@app.route("/updateeo", methods=['POST'])
def update_eo():
    id = request.form['id']
    eoname = request.form['eoname']
    eotypename = request.form['eotypename']
    eoparent = request.form['eoparent']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    eo = crud.get_eo_by_id(id)
    if eo:
        try:
            crud.update_eo(eo, eoname, eotypename, eoparent, regname, areaname, tername)
            return render_template('index.html', updateEoMessage='Навчальний заклад оновлено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', updateEoMessage='Сталася помилка')
    else: return render_template('index.html', updateEoMessage='Навчальний заклад з таким id не знайдено')

@app.route("/deleteeo", methods=['POST'])
def delete_eo():
    print(request)
    id = request.form['id']
    eo = crud.get_eo_by_id(id)
    if eo:
        try:
            crud.delete_eo(eo)
            return render_template('index.html', deleteEoMessage='Навчальний заклад видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deleteEoMessage='Сталася помилка')
    else: return render_template('index.html', deleteEoMessage='Навчальний заклад з таким id не знайдено')
# PT
@app.route("/getptbyparams", methods=['GET'])
def get_pt_by_params():
    print(request)
    name = request.args['ptname']
    regname = request.args['regname']
    areaname = request.args['areaname']
    tername = request.args['tername']
    ptList = crud.get_pt_by_params(name, regname, areaname, tername)
    size = len(ptList)
    return render_template('index.html', size=size, ptList=ptList)

@app.route("/newpt", methods=['POST'])
def new_pt():
    print(request)
    name = request.form['ptname']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    try:
        crud.create_pt(name, regname, areaname, tername)
        return render_template('index.html', addPtMessage='Пункт тестування додано')
    except Exception as e:
        print("Помилка при додаванні пункту тестування:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addPtMessage='Сталася помилка')

@app.route("/updatept", methods=['POST'])
def update_pt():
    name = request.form['ptname']
    new_name = request.form['newptname']
    regname = request.form['regname']
    areaname = request.form['areaname']
    tername = request.form['tername']

    pt = crud.get_pt_by_id(name)
    if pt:
        try:
            crud.update_pt(pt, new_name, regname, areaname, tername)
            return render_template('index.html', updatePtMessage='Пункт тестування оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updatePtMessage='Сталася помилка')
    else: return render_template('index.html', updatePtMessage='Пункт тестування з таким id не знайдено')

@app.route("/deletept", methods=['POST'])
def delete_pt():
    print(request)
    ptname = request.form['ptname']
    pt = crud.get_pt_by_id(ptname)
    if pt:
        try:
            crud.delete_pt(pt)
            return render_template('index.html', deletePtMessage='Пункт тестування видалено')
        except Exception as e:
            crud.sessionRollback()
            return render_template('index.html', deletePtMessage='Сталася помилка')
    else: return render_template('index.html', deletePtMessage='Пункт тестування з таким id не знайдено')

# UML
@app.route("/getumlbyparams", methods=['GET'])
def get_uml_by_params():
    print(request)
    id = request.args['id']
    teststatus = request.args['teststatus']
    ptname = request.args['ptname']
    umlList = crud.get_uml_by_params(id, teststatus, ptname)
    size = len(umlList)
    return render_template('index.html', size=size, umlList=umlList)

@app.route("/newuml", methods=['POST'])
def new_uml():
    print(request)
    student_id = request.form['student_id']
    teststatus = request.form['teststatus']
    ball100 = request.form['ball100']
    ball12 = request.form['ball12']
    ball = request.form['ball']
    adaptscale = request.form['adaptscale']
    ptname = request.form['ptname']

    try:
        crud.create_uml(student_id, teststatus, ball100, ball12, ball, adaptscale, ptname)
        return render_template('index.html', addUmlMessage='Uml_test додано')
    except Exception as e:
        print("Помилка при додаванні uml:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addUmlMessage='Сталася помилка')

@app.route("/updateuml", methods=['POST'])
def update_uml():
    student_id = request.form['student_id']
    teststatus = request.form['teststatus']
    ball100 = request.form['ball100']
    ball12 = request.form['ball12']
    ball = request.form['ball']
    adaptscale = request.form['adaptscale']
    ptname = request.form['ptname']

    uml = crud.get_uml_by_student_id(student_id)
    if uml:
        try:
            crud.update_uml(uml, teststatus, ball100, ball12, ball, adaptscale, ptname)
            return render_template('index.html', updateUmlMessage='Uml оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updateUmlMessage='Сталася помилка')
    else: return render_template('index.html', updateUmlMessage='Uml з таким id не знайдено')

@app.route("/deleteuml", methods=['POST'])
def delete_uml():
    print(request)
    id = request.form['id']
    uml = crud.get_uml_by_student_id(id)
    if uml:
        try:
            crud.delete_uml(uml)
            return render_template('index.html', deleteUmlMessage='Uml видалено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', deleteUmlMessage='Сталася помилка')
    else: return render_template('index.html', deleteUmlMessage='Uml з таким id не знайдено')

# UKR
@app.route("/getukrbyparams", methods=['GET'])
def get_ukr_by_params():
    print(request)
    id = request.args['id']
    teststatus = request.args['teststatus']
    ptname = request.args['ptname']
    ukrList = crud.get_ukr_by_params(id, teststatus, ptname)
    size = len(ukrList)
    return render_template('index.html', size=size, ukrList=ukrList)

@app.route("/newukr", methods=['POST'])
def new_ukr():
    print(request)
    student_id = request.form['student_id']
    subtest = request.form['subtest']
    teststatus = request.form['teststatus']
    ball100 = request.form['ball100']
    ball12 = request.form['ball12']
    ball = request.form['ball']
    adaptscale = request.form['adaptscale']
    ptname = request.form['ptname']

    try:
        crud.create_ukr(student_id, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname)
        return render_template('index.html', addUkrMessage='Ukr_test додано')
    except Exception as e:
        print("Помилка при додаванні ukr:", str(e))
        crud.sessionRollback()
        return render_template('index.html', addUkrMessage='Сталася помилка')

@app.route("/updateukr", methods=['POST'])
def update_ukr():
    student_id = request.form['student_id']
    subtest = request.form['subtest']
    teststatus = request.form['teststatus']
    ball100 = request.form['ball100']
    ball12 = request.form['ball12']
    ball = request.form['ball']
    adaptscale = request.form['adaptscale']
    ptname = request.form['ptname']

    ukr = crud.get_ukr_by_student_id(student_id)
    if ukr:
        try:
            crud.update_ukr(ukr, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname)
            return render_template('index.html', updateUkrMessage='Ukr оновлено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', updateUkrMessage='Сталася помилка')
    else: return render_template('index.html', updateUkrMessage='Uml з таким id не знайдено')

@app.route("/deleteukr", methods=['POST'])
def delete_ukr():
    print(request)
    id = request.form['id']
    ukr = crud.get_ukr_by_student_id(id)
    if ukr:
        try:
            crud.delete_ukr(ukr)
            return render_template('index.html', deleteUkrMessage='Ukr видалено')
        except Exception as e:
            print(e)
            crud.sessionRollback()
            return render_template('index.html', deleteUkrMessage='Сталася помилка')
    else: return render_template('index.html', deleteUkrMessage='Ukr з таким id не знайдено')


def run():
    app.run(host='0.0.0.0', port=8080, debug=False)

# run()
