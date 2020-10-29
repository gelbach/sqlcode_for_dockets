#primarytitle,filingdate,closeddate,natureofsuit,natureofsuitcode,noticeofremovalflag,
#dateentry,entrynumber,entrytext

import sqlite3
import os
import re
#import logging
import sys
sys.path.append('/data2/dockets/utilities')
from setupLoggerAndReturn import setupLoggerAndReturn

#THISFILE = __file__
THISFILE = "dummy"
mylogger = setupLoggerAndReturn(THISFILE + '.log')

def setupDB(filename):
    return sqlite3.connect(filename)

def setupCursor(db):
    return db.cursor()

def dropTables(logger,cursor):
    cursor.execute('''DROP TABLE IF EXISTS wlfilename''')
    cursor.execute('''DROP TABLE IF EXISTS caseheader''')
    cursor.execute('''DROP TABLE IF EXISTS leaddocket''')
    cursor.execute('''DROP TABLE IF EXISTS otherdocket''')
    cursor.execute('''DROP TABLE IF EXISTS party''')
    cursor.execute('''DROP TABLE IF EXISTS attorney''')
    cursor.execute('''DROP TABLE IF EXISTS entry''')
    cursor.execute('''DROP TABLE IF EXISTS fullcase''')
    cursor.execute('''DROP TABLE IF EXISTS unique_entry''')
    logger.info("Existing tables DROPped if they existed.\n")
    return 1

def createWlfilename(logger,cursor):
        def makeListOfAllFiles():
            searchAll = re.compile('.*xml$').search
            listOfAllFiles = [ l 
                               for l in os.listdir("/data2/dockets/") 
                               for m in (searchAll(l),) if m]
            return(listOfAllFiles)

	cursor.execute('''CREATE TABLE wlfilename (
	               id integer PRIMARY KEY AUTOINCREMENT, 
	               wlfilename text UNIQUE NOT NULL,
	               batch INTEGER
	          )''')

        listOfAllFiles = makeListOfAllFiles()
	search2005_2011 = re.compile('^.*(200(5|6|7|8|9)|2010|2011).*$').search
	search2005_2011_Remaining = re.compile('^.*Remaining20.*').search
	search2012_JanJune = re.compile('^.*2012JanuaryThroughJune.*').search
	search2012_JulSep = re.compile('^.*JulySept2012.*').search
	search2012_OctDec = re.compile('^.*OctDec2012.*').search
	search2013eoy = re.compile('^.*2013eoy.*').search
	search2013 = re.compile('^.*2013.*').search
	search2014_Dec = re.compile('^.*Dec2014.*').search
	search2014 = re.compile('^.*2014.*').search
		
	batch = dict()
	for file in listOfAllFiles:
            #print file
	    if search2005_2011_Remaining(file): #these came after initial 2005-2011 files
	        batch[file] = 2
	    elif search2005_2011(file): #these came first
	        batch[file] = 1
	    elif search2012_JanJune(file):
	        batch[file] = 3
	    elif search2012_JulSep(file):
	        batch[file] = 4
	    elif search2012_OctDec(file):
	        batch[file] = 5
	    elif search2013eoy(file): #these came after other 2013 files
	        batch[file] = 7
	    elif search2013(file): #these came before 2013eoy
	        batch[file] = 6
	    elif search2014_Dec(file): #these came after other 2014 files
	        batch[file] = 9
	    elif search2014(file): #these came before other 2014 files
	        batch[file] = 8

            #print "(batch[file],file)=(", batch[file],file, ")"
	    cursor.execute('''
	                 INSERT INTO wlfilename(batch,wlfilename) VALUES (?,?)
	              ''', (batch[file],file)
	    )
        try:
            assert len(batch.keys())==360
        except:
            crash
        logstring = "Verifying that len(batch.keys())=360: " + str(len(batch.keys())) + ".\n"
        logstring += "\tCREATEd TABLE wlfilename.\n" 
        logger.info(logstring)
        print "Verifying that len(batch.keys())=360:", len(batch.keys())
        return 1

def createFullcase(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE fullcase (
	               id integer UNIQUE PRIMARY KEY AUTOINCREMENT, 
	               fullcase_text text
	             )
	          '''
	         )
        logger.info("CREATEd TABLE fullcase.\n")
        return 1

def createCaseheader(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE caseheader (
	               id integer PRIMARY KEY AUTOINCREMENT,
	               court text,
	               docketnumber text, 
	               primarytitle text,
	               filingdate text,
	               closeddate text,
	               natureofsuit text,
	               natureofsuitcode integer,
	               fullcase_text text,
	               fullcase_id integer,
	               wlfilename_id integer NOT NULL,
                       classactionflag_case int,
                       mdlflag_case int,
	               CONSTRAINT fk_fullcase
	                 FOREIGN KEY(fullcase_id)
	                 REFERENCES fullcase(id)
	                 ON DELETE CASCADE,
	               CONSTRAINT fk_wlfilename
	                 FOREIGN KEY(wlfilename_id)
	                 REFERENCES wlfilename(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE caseheader.\n")
        return 1


def createLeaddocket(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE leaddocket (
	               leaddocketid integer NOT NULL ,
                       leaddocketmain text,
                       leaddocketsupplemental text,
                       accumulatedstring text,
	               fullcase_text text ,
	               caseheader_id integer NOT NULL,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE leaddocket.\n")
        return 1


def createOtherdocket(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE otherdocket (
	               otherdocketid integer NOT NULL ,
                       otherdocket text,
	               fullcase_text text ,
	               caseheader_id integer NOT NULL,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE otherdocket.\n")
        return 1


def createParty(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE party (
	               partyid integer NOT NULL ,
                       partyname text,
                       partytype text,
	               fullcase_text text ,
	               caseheader_id integer NOT NULL,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE party.\n")
        return 1


def createAttorney(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE attorney (
                       attorneyid integer NOT NULL,
                       attorneyname text,
                       attorneystatus text,
                       firm text,
                       firmcity text,
                       firmstate text,
                       partyid integer NOT NULL ,
                       caseheader_id integer NOT NULL ,
                       fullcase_text text ,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE,
	               CONSTRAINT fk_party
	                 FOREIGN KEY(partyid)
	                 REFERENCES party(partyid)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE party.\n")
        return 1

def createEntry(logger,cursor): 
	cursor.execute('''
	             CREATE TABLE entry (
	               id integer UNIQUE PRIMARY KEY AUTOINCREMENT, 
	               my_entrynumber integer NOT NULL,
	               entrynumber text,
	               dateentry text,
	               entrytext text,
	               entrytype text,
	               classactionflag text,
	               mdlflag text,
                       first50 text,
	               fullcase_text text ,
	               fullcase_id integer ,
	               caseheader_id integer NOT NULL,
	               CONSTRAINT fk_fullcase
	                 FOREIGN KEY(fullcase_id)
	                 REFERENCES fullcase(id)
	                 ON DELETE CASCADE,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE entry.\n")
        return 1

def createUnique_entry(logger,cursor): 
        #have to figure out what to do about my_entrynumber column--maybe just ditch?
	cursor.execute('''
	             CREATE TABLE unique_entry (
	               id integer UNIQUE PRIMARY KEY AUTOINCREMENT, 
	               my_entrynumber integer ,
	               entrynumber text,
	               dateentry text,
	               entrytext text,
	               fullcase_id integer ,
	               caseheader_id integer NOT NULL,
	               CONSTRAINT fk_fullcase
	                 FOREIGN KEY(fullcase_id)
	                 REFERENCES fullcase(id)
	                 ON DELETE CASCADE,
	               CONSTRAINT fk_caseheader
	                 FOREIGN KEY(caseheader_id)
	                 REFERENCES caseheader(id)
	                 ON DELETE CASCADE
	             )
	          '''
	         )
        logger.info("CREATEd TABLE unique_entry.\n")
        return 1

#mylogger = setupLogging()
dbfile = '/data2/dockets/sqlite/pydockets-v2.db'
try:
     os.remove(dbfile)
except:
     pass
db = setupDB(dbfile)
c  = setupCursor(db)

dropTables(mylogger,c)
createWlfilename(mylogger,c)
createFullcase(mylogger,c)
createCaseheader(mylogger,c)
createParty(mylogger,c)
createAttorney(mylogger,c)
createEntry(mylogger,c)
createOtherdocket(mylogger,c)
createLeaddocket(mylogger,c)
createUnique_entry(mylogger,c)

db.commit()
db.close()
os.chmod(dbfile,0664)
mylogger.info("Finished.\n")
