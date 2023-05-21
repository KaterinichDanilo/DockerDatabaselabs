--DROP TABLE IF EXISTS regs, areas, ters, pt, eo, students, uml_test, hist_test,
-- ukr_test, math_test, mathst_test, spa_test, fra_test, deu_test, geo_test, bio_test, eng_test, chem_test, phys_test CASCADE;

CREATE TABLE regs(
        regname CHAR(250) PRIMARY KEY
);

CREATE TABLE areas(
        areaname  CHAR(250) PRIMARY KEY
);

CREATE TABLE ters(
        tername CHAR(250) PRIMARY KEY,
		tertypename CHAR(50)
);

CREATE TABLE eo(
        id bigserial PRIMARY KEY,
        eoname CHAR(300),
        eotypename CHAR(250),
        eoparent CHAR(300),
        regname CHAR(250),
		areaname CHAR(250),
		tername CHAR(250),
        FOREIGN KEY(regname)
	        REFERENCES regs(regname),
		FOREIGN KEY(areaname)
	        REFERENCES areas(areaname),
		FOREIGN KEY(tername)
	        REFERENCES ters(tername)
);

CREATE TABLE pt(
        name CHAR(300) PRIMARY KEY,
		regname CHAR(250),
		areaname CHAR(250),
		tername CHAR(250),
		FOREIGN KEY(regname)
	        REFERENCES regs(regname),
		FOREIGN KEY(areaname)
	        REFERENCES areas(areaname),
		FOREIGN KEY(tername)
	        REFERENCES ters(tername)
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
        regname CHAR(250),
		areaname CHAR(250),
		tername CHAR(250),
		FOREIGN KEY(eo_id)
	        REFERENCES eo(id),
        FOREIGN KEY(regname)
	        REFERENCES regs(regname),
		FOREIGN KEY(areaname)
	        REFERENCES areas(areaname),
		FOREIGN KEY(tername)
	        REFERENCES ters(tername)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
);

CREATE TABLE mathst_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        lang CHAR(20),
        teststatus CHAR(50),
        ball12 REAL,
        ball REAL,
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
);

CREATE TABLE fra_test(
        id bigserial PRIMARY KEY,
        student_id CHAR(50),
        test CHAR(50),
        teststatus CHAR(50),
        ball100 REAL,
        ball12 REAL,
        ball REAL,
        dpalevel CHAR(50),
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
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
        ptname CHAR(300),
        UNIQUE (student_id),
        FOREIGN KEY(ptname)
	        REFERENCES pt(name),
		FOREIGN KEY(student_id)
	        REFERENCES students(id)
);

DO $$
    BEGIN
        IF EXISTS
            ( SELECT 1
              FROM   information_schema.tables
              WHERE  table_schema = 'public'
              AND    table_name = 'znodata'
            )
        THEN
            INSERT INTO regs (regname) SELECT DISTINCT reg
				 FROM znodata CROSS JOIN LATERAL
				 (VALUES(umlptregname),(ukrptregname),(mathptregname), (mathstptregname), (histptregname), (spaptregname),
	(fraptregname), (deuptregname), (geoptregname), (bioptregname), (engptregname), (chemptregname), (physptregname), (eoregname), (regname)) as t(reg)
			WHERE reg IS NOT null AND reg!='NaN';

			INSERT INTO areas (areaname) SELECT DISTINCT area
				 FROM public.znodata CROSS JOIN LATERAL
				 (VALUES(umlptareaname),(ukrptareaname),(mathptareaname), (mathstptareaname), (histptareaname),
				  (spaptareaname), (fraptareaname), (deuptareaname), (geoptareaname), (bioptareaname),
				  (engptareaname), (chemptareaname), (physptareaname), (eoareaname), (areaname)) as t(area)
			WHERE area IS NOT null AND area!='NaN';

			INSERT INTO ters (tername) SELECT DISTINCT ter
				 FROM znodata CROSS JOIN LATERAL
				 (VALUES(umlpttername),(ukrpttername),(mathpttername), (mathstpttername), (histpttername),
				  (spapttername), (frapttername), (deupttername), (geopttername), (biopttername),
				  (engpttername), (chempttername), (physpttername), (eotername), (tername)) as t(ter)
				  WHERE ter IS NOT null AND ter!='NaN';
			UPDATE ters SET tertypename='селище міського типу' WHERE tername IN (SELECT tername FROM znodata WHERE tertypename='селище міського типу');
			UPDATE ters SET tertypename='селище, село' WHERE tername IN (SELECT tername FROM znodata WHERE tertypename='селище, село');
			UPDATE ters SET tertypename='місто' WHERE tername IN (SELECT tername FROM znodata WHERE tertypename='місто');

			INSERT INTO pt(name, regname, areaname, tername) SELECT DISTINCT umlptname, umlptregname, umlptareaname, umlpttername
				  FROM znodata
 				WHERE umltest!='NaN';

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT ukrptname, ukrptregname, ukrptareaname, ukrpttername FROM znodata
			 WHERE ukrtest!='NaN' AND ukrptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT ukrptname, ukrptregname, ukrptareaname, ukrpttername FROM znodata
			 WHERE ukrtest!='NaN' AND ukrptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT mathptname, mathptregname, mathptareaname, mathpttername FROM znodata
			WHERE mathtest!='NaN' AND mathptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT mathstptname, mathstptregname, mathstptareaname, mathstpttername FROM znodata
			 WHERE mathsttest!='NaN' AND mathstptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT mathstptname, mathstptregname, mathstptareaname, mathstpttername FROM znodata
			 WHERE mathsttest!='NaN' AND mathstptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT histptname,histptregname, histptareaname, histpttername FROM znodata
			 WHERE histtest!='NaN' AND histptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT engptname, engptregname, engptareaname, engpttername FROM znodata
			 WHERE engtest!='NaN' AND engptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT fraptname, fraptregname, fraptareaname, frapttername FROM znodata
			 WHERE fratest!='NaN' AND fraptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT spaptname, spaptregname, spaptareaname, spapttername FROM  znodata
			 WHERE spatest!='NaN' AND spaptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT deuptname, deuptregname, deuptareaname, deupttername FROM  znodata
			 WHERE deutest!='NaN' AND deuptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT physptname, physptregname, physptareaname, physpttername FROM znodata
			 WHERE phystest!='NaN' AND physptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT bioptname, bioptregname, bioptareaname, biopttername FROM znodata
			 WHERE biotest!='NaN' AND bioptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT geoptname, geoptregname, geoptareaname, geopttername FROM znodata
			 WHERE geotest!='NaN' AND geoptname NOT IN (SELECT name FROM pt);

			INSERT INTO pt(name, regname, areaname, tername)
			SELECT DISTINCT chemptname, chemptregname, chemptareaname, chempttername FROM znodata
			 WHERE chemtest!='NaN' AND chemptname NOT IN (SELECT name FROM pt);

			INSERT INTO eo (eoname, eotypename, eoparent, regname, areaname, tername)
			SELECT DISTINCT ON(eoname, eotypename, eoparent) eoname, eotypename, eoparent, eoregname, eoareaname, eotername FROM znodata
			WHERE eoname!='NaN';

			INSERT INTO students (id, year, birth, sextypename, classprofilename, classlangname, regtypename, regname, areaname, tername, eo_id)
			SELECT outid, year, birth, sextypename, classprofilename, classlangname, regtypename, znodata.regname, znodata.areaname, znodata.tername, eo.id AS eo_id FROM znodata
					LEFT JOIN eo ON eo.eoname=znodata.eoname AND eo.eoparent=znodata.eoparent AND eo.eotypename=znodata.eotypename;

			INSERT INTO uml_test (student_id, test, teststatus, ball100, ball12, ball, adaptscale, ptname)
			SELECT outid, umltest, umlteststatus, umlball100, umlball12, umlball, umladaptscale, umlptname
			FROM znodata
			WHERE umltest!='NaN';

			INSERT INTO ukr_test (student_id, test, subtest, teststatus, ball100, ball12, ball, adaptscale, ptname)
				SELECT outid, ukrtest, ukrsubtest, ukrteststatus, ukrball100, ukrball12, ukrball, ukradaptscale, ukrptname FROM znodata
			WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'znodata');

			INSERT INTO hist_test (student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
			SELECT outid, histtest, histlang, histteststatus, histball100, histball12, histball, histptname FROM znodata
			WHERE histtest!='NaN';

			INSERT INTO math_test (student_id, test, lang, teststatus, ball100, ball12, ball, dpalevel, ptname)
			SELECT outid, mathtest, mathlang, mathteststatus, mathball100, mathball12, mathball, mathdpalevel, mathptname FROM znodata
			WHERE mathtest!='NaN';

			INSERT INTO mathst_test (student_id, test, lang, teststatus, ball12, ball, ptname)
			SELECT outid, mathsttest, mathstlang, mathstteststatus, mathstball12, mathstball, mathstptname FROM znodata
			WHERE mathsttest!='NaN';

			INSERT INTO phys_test (student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
			SELECT outid, phystest, physlang, physteststatus, physball100, physball12, physball, physptname FROM znodata
			WHERE phystest!='NaN';

			INSERT INTO chem_test (student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
			SELECT outid, chemtest, chemlang, chemteststatus, chemball100, chemball12, chemball, chemptname FROM znodata
			WHERE chemtest!='NaN';

			INSERT INTO bio_test (student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
			SELECT outid, biotest, biolang, bioteststatus, bioball100, bioball12, bioball, bioptname FROM znodata
			WHERE biotest!='NaN';

			INSERT INTO geo_test (student_id, test, lang, teststatus, ball100, ball12, ball, ptname)
			SELECT outid, geotest, geolang, geoteststatus, geoball100, geoball12, geoball, geoptname FROM znodata
			WHERE geotest!='NaN';

			INSERT INTO eng_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, ptname)
			SELECT outid, engtest, engteststatus, engball100, engball12, engball, engdpalevel, engptname FROM znodata
			WHERE engtest!='NaN';

			INSERT INTO fra_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, ptname)
			SELECT outid, fratest, frateststatus, fraball100, fraball12, fraball, fradpalevel, fraptname FROM znodata
			WHERE fratest!='NaN';

			INSERT INTO deu_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, ptname)
			SELECT outid, deutest, deuteststatus, deuball100, deuball12, deuball, deudpalevel, deuptname FROM znodata
			WHERE deutest!='NaN';

			INSERT INTO spa_test (student_id, test, teststatus, ball100, ball12, ball, dpalevel, ptname)
			SELECT outid, spatest, spateststatus, spaball100, spaball12, spaball, spadpalevel, spaptname FROM znodata
			WHERE spatest!='NaN';

        END IF ;
    END
   $$ ;