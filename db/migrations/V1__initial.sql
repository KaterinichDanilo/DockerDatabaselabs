DROP TABLE regions, regions_eo, students, educationalorganizations, uml_test, hist_test,
 ukr_test, math_test, mathst_test, spa_test, fra_test, deu_test, geo_test, bio_test, eng_test, chem_test, phys_test CASCADE;

CREATE TABLE regions(
        id bigserial PRIMARY KEY,
        regname CHAR(250),
        areaname  CHAR(250),
        tername CHAR(250),
        tertypename CHAR(20),
        UNIQUE(regname, areaname, tername)
);

CREATE TABLE regions_eo(
        id bigserial PRIMARY KEY,
        eoregname  CHAR(250),
        eoareaname CHAR(250),
        eotername CHAR(250),
        UNIQUE(eoregname, eoareaname, eotername)
);

CREATE TABLE educationalorganizations(
        id bigserial PRIMARY KEY,
        eoname CHAR(300),
        eotypename CHAR(250),
        eoparent CHAR(300),
        regeo_id INT,
        UNIQUE(eoname, eotypename, eoparent),
        FOREIGN KEY(regeo_id)
	        REFERENCES regions_eo(id)
);

CREATE TABLE students(
        id CHAR(50) PRIMARY KEY,
        birth INT,
        year INT,
        sextypename CHAR(8),
        classprofilename CHAR(250),
        classlangname CHAR(250),
        regtypename CHAR(250),
		eo_id INT,
        reg_id INT,
        FOREIGN KEY(reg_id)
	        REFERENCES regions(id)
);

CREATE TABLE uml_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        adaptscale INT,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE ukr_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        subtest CHAR(3),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        adaptscale INT,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE hist_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE math_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE mathst_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE phys_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE chem_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE bio_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE geo_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE eng_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE fra_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE deu_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

CREATE TABLE spa_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        eo_id BIGINT,
        UNIQUE (student_id),
        FOREIGN KEY(student_id)
	        REFERENCES students(id),
	    FOREIGN KEY(eo_id)
	        REFERENCES educationalorganizations(id)
);

INSERT INTO regions (regname, areaname, tername, tertypename)
SELECT DISTINCT ON (regname, areaname, tername) regname, areaname, tername, tertypename
FROM znodata
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO regions_eo (eoregname, eoareaname, eotername)
SELECT DISTINCT ON (eoregname, eoareaname, eotername) eoregname, eoareaname, eotername
FROM znodata
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO educationalorganizations (eoname, eotypename, eoparent, regeo_id)
SELECT DISTINCT ON(eoname, eotypename, eoparent) eoname, eotypename, eoparent, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.eoregname
            AND regions_eo.eoareaname=znodata.eoareaname AND regions_eo.eotername=znodata.eotername
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO students (id, year, birth, sextypename, classprofilename, classlangname, regtypename, reg_id, eo_id)
SELECT outid, year, birth, sextypename, classprofilename, classlangname, regtypename, reg_id, eo_id FROM
        (SELECT outid, year, birth, sextypename, classprofilename, classlangname, regtypename, regions.id AS reg_id, educationalorganizations.id AS eo_id FROM znodata
        LEFT JOIN regions ON regions.regname=znodata.regname
            AND regions.areaname=znodata.areaname AND regions.tername=znodata.tername
		LEFT JOIN educationalorganizations ON educationalorganizations.eoname=znodata.eoname
		 AND educationalorganizations.eoparent=znodata.eoparent
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata')

INSERT INTO uml_test (student_id, test, teststatus, ball100, ball12, ball, adaptscale, eo_id)
SELECT outid, umltest, umlteststatus, umlball100, umlball12, umlball, umladaptscale, educationalorganizations.id FROM znodata
		LEFT JOIN educationalorganizations ON educationalorganizations.eoname=znodata.umlptname
WHERE umltest!='NaN' AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO ukr_test (student_id, test, subtest, teststatus, ball100, ball12, ball, adaptscale, eo_id)
	SELECT outid, ukrtest, ukrsubtest, ukrteststatus, ukrball100, ukrball12, ukrball, ukradaptscale, educationalorganizations.id FROM znodata
        LEFT JOIN educationalorganizations ON educationalorganizations.eoname=znodata.ukrptname
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata') AND ukrtest!='NaN';

INSERT INTO hist_test (student_id, test, lang, teststatus, ball100, ball12, ball, eo_id)
SELECT outid, histtest, histlang, histteststatus, histball100, histball12, histball, educationalorganizations.id FROM znodata
        LEFT JOIN educationalorganizations ON educationalorganizations.eoname=znodata.histptname
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata') AND histtest!='NaN';

INSERT INTO math_test (student_id, test, lang, teststatus, ball100, ball12, ball, dpalevel, eo_id)
SELECT outid, mathtest, mathlang, mathteststatus, mathball100, mathball12, mathball, mathdpalevel, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.mathptregname
            AND regions_eo.eoareaname=znodata.mathptareaname AND regions_eo.eotername=znodata.mathpttername
        WHERE mathtest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO mathst_test (student_id, test, lang, teststatus, ball12, ball, eo_id)
SELECT outid, mathsttest, mathstlang, mathstteststatus, mathstball12, mathstball, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.mathstptregname
            AND regions_eo.eoareaname=znodata.mathstptareaname AND regions_eo.eotername=znodata.mathstpttername
        WHERE mathsttest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO phys_test (student_id, test, lang, teststatus, ball100, ball12, ball, eo_id)
SELECT outid, phystest, physlang, physteststatus, physball100, physball12, physball, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.physptregname
            AND regions_eo.eoareaname=znodata.physptareaname AND regions_eo.eotername=znodata.physpttername
        WHERE phystest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO chem_test (student_id, test, lang, teststatus, ball100, ball12, ball, eo_id)
SELECT outid, chemtest, chemlang, chemteststatus, chemball100, chemball12, chemball, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.chemptregname
            AND regions_eo.eoareaname=znodata.chemptareaname AND regions_eo.eotername=znodata.chempttername
        WHERE chemtest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO bio_test (student_id, test, lang, teststatus, ball100, ball12, ball, eo_id)
SELECT outid, biotest, biolang, bioteststatus, bioball100, bioball12, bioball, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.bioptregname
            AND regions_eo.eoareaname=znodata.bioptareaname AND regions_eo.eotername=znodata.biopttername
        WHERE biotest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO geo_test (student_id, test, lang, teststatus, ball100, ball12, ball, eo_id)
SELECT outid, geotest, geolang, geoteststatus, geoball100, geoball12, geoball, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.geoptregname
            AND regions_eo.eoareaname=znodata.geoptareaname AND regions_eo.eotername=znodata.geopttername
        WHERE geotest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO eng_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, eo_id)
SELECT outid, engtest, engteststatus, engball100, engball12, engball, engdpalevel, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.engptregname
            AND regions_eo.eoareaname=znodata.engptareaname AND regions_eo.eotername=znodata.engpttername
        WHERE engtest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO fra_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, eo_id)
SELECT outid, fratest, frateststatus, fraball100, fraball12, fraball, fradpalevel, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.fraptregname
            AND regions_eo.eoareaname=znodata.fraptareaname AND regions_eo.eotername=znodata.frapttername
        WHERE fratest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO deu_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, eo_id)
SELECT outid, deutest, deuteststatus, deuball100, deuball12, deuball, deudpalevel, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.deuptregname
            AND regions_eo.eoareaname=znodata.deuptareaname AND regions_eo.eotername=znodata.deupttername
        WHERE deutest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

INSERT INTO spa_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, eo_id)
SELECT outid, spatest, spateststatus, spaball100, spaball12, spaball, spadpalevel, id FROM
        (SELECT * FROM znodata
        LEFT JOIN regions_eo ON regions_eo.eoregname=znodata.deuptregname
            AND regions_eo.eoareaname=znodata.spaptareaname AND regions_eo.eotername=znodata.spapttername
        WHERE spatest!='NaN'
        ) AS s1
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');