
from datetime import datetime as dt
import os
import traceback
import re
import csv
from lxml import etree as ET
import sqlite3

import sys
sys.path.append('/data2/dockets/utilities')

import logging

from xmlFiles import *
from CaseLevelFunctions import *
from GeneralFunctions import *
from docketsFileReader_new2 import *

import numpy as np
import pandas as pd

THISFILE = 'read-into-sqlite-tables-devt.py'

#logging function
def setupLogging():
     formatter = logging.Formatter('%(filename)s[%(funcName)s/%(lineno)d][%(levelname)s] at %(asctime)s:\n\t%(message)s')

     myLoggerFh = logging.FileHandler(THISFILE+'.log',mode='w')
     myLoggerFh.setFormatter(formatter)

     console = logging.StreamHandler()
     console.setFormatter(formatter)

     logger = logging.getLogger(THISFILE)#__name__)
     logger.setLevel(logging.DEBUG)
     logger.addHandler(myLoggerFh)
     logger.addHandler(console)
     logger.setLevel(logging.DEBUG)
     console.setLevel(logging.ERROR)

     logger.info('Starting ' + THISFILE + '.py.\n\n')
     return logger

mylogger = setupLogging()

class myQueryExecuter:
     'Class to execute queries against sqlite db, given cursor'
     """
     Will run against designated cursor, 
     logging an error if an Exception is raised on execution.
     """
     def __init__(self,cursor):
          self.cursor = cursor

     def execute(self,query):
          mylogger.info('About to execute query:\n\t {}'.format(query))
          try:
               self.query = query
               self.cursor.execute(query)
               mylogger.info('Execution succeeded.')
          except Exception as err:
               mylogger.info('Query Failed: %s\nError: %s' % (query, str(err)))          


######################
# DEFINE getCaseInfo #
######################

def getCaseInfo(object):
     """
     processDocketFunction function. 
     MUST RETURN A LIST!
     """
     docket = object.docket
     partyDataListOfNTs = object.partyDataList
     #print "partyDataListOfNTs={0}".format(partyDataListOfNTs)
     primaryTitle = getValue('primary.title',docket).replace('"','')
     docketNumber = getValueNested(['docket.number','docket.block'],docket)
     court        = getCourt(getValue('court',docket))
     filingDate   = getValueNested(['filing.date', 'filing.date.block'],docket)
     closedDate   = getDateClosed(docket)
     natureOfSuit = getNatureOfSuit(docket)
     natureOfSuitCode = getNatureOfSuitCode(docket)
     caseheaderList = [court,docketNumber,
                       primaryTitle,
                       filingDate,closedDate,
                       natureOfSuit,natureOfSuitCode
     ]
     dictOfEntriesInfo = object.docketEntriesDict
     return [caseheaderList,dictOfEntriesInfo,partyDataListOfNTs]

def insertIntoWlFileNameTable(wlfilename,db):
     cursor = db.cursor()
     query = '''
                         INSERT INTO wlfilename
                         (id,wlfilename)
                         VALUES (null,?)
                        '''#, (wlfilename,)
     try:
          cursor.execute(query, (wlfilename,))
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))
     return cursor.lastrowid

def getWlFileNameID(wlfilename,db):
     cursor = db.cursor()
     query = '''
                         SELECT id FROM wlfilename WHERE wlfilename = ?
                        '''
     try:
          cursor.execute(query, (wlfilename,))
          resultset = cursor.fetchall()
          assert len(resultset)==1 
          for eachtuple in resultset:
               assert len(eachtuple)==1
               wlfilename_id = eachtuple[0]
     except Exception as err:
          print('Query Failed: %s\nWith value: %s\nError: %s' % (query, wlfilename, str(err)))
     return wlfilename_id 

def insertIntoCaseheaderTableAndReturnId(caseheaderList,db,wlfilename_id):
     cursor = db.cursor()
     fullcase_text = '_'.join(caseheaderList[0:2])
     caseheaderList.extend([fullcase_text,wlfilename_id])
     tupleToInsert = tuple(caseheaderList)
     query = '''
                         INSERT INTO caseheader 
                         (id,court,docketnumber,primarytitle,filingdate,closeddate,
                          natureofsuit,natureofsuitcode,
                          fullcase_text, wlfilename_id
                         )
                         VALUES (null,?,?,?,?,?,?,?,?,?)
     '''
     try:
          cursor.execute(query, (tupleToInsert))
          db.commit()
     except Exception as err:
          print('caseheaderList is: ', caseheaderList)
          print('Query Failed: %s\nError: %s' % (query, str(err)))          
     return [cursor.lastrowid, fullcase_text]

def makeZipList(entryDict,fullcase_text,caseheader_id):
     myDENList = entryDict['myDENCounter']
     numberList = entryDict['number']
     dateList = entryDict['date']
     docketdescriptionList = entryDict['docketdescription']
     classactionflagList = entryDict['classactionflag']
     mdlflagList = entryDict['mdlflag']
     caseheader_idList = [caseheader_id]*len(myDENList)
     fullcase_textList = [fullcase_text]*len(myDENList)
     return zip(myDENList, numberList, dateList, docketdescriptionList,
                classactionflagList,mdlflagList,
                fullcase_textList, caseheader_idList)

def insertIntoEntryTable(entryDict,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     #print "entryDict={0}".format(entryDict)
     zipList = makeZipList(entryDict,caseheader_id,fullcase_text)
     #print "zipList={0}".format(zipList)
     query = '''
                INSERT INTO entry
                 (id, my_entrynumber, entrynumber, dateentry, entrytext, 
                      classactionflag, mdlflag,
                      caseheader_id,fullcase_text)
                 VALUES (null,?,?,?,?,?,?,?,?)
     '''
     try: 
          cursor.executemany(query, zipList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          

def insertIntoPartyTable(partyDataListOfNTs,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     queryParty = '''
                INSERT INTO party
                 (id, partycounter, partyname, partytype, partyterminated,
                      caseheader_id,fullcase_text)
                 VALUES (null,?,?,?,?,?,?)
     '''
     queryPartyList = list()
     queryAttorneyTEMP = '''
                INSERT INTO attorneyTEMP
                 (id, partycounter, attorneycounter, 
                      attorneyname, attorneystatus,
                      firmname, firmaddress, firmcity, firmstate, firmzip,
                      caseheader_id,fullcase_text)
                 VALUES (null,?,?,?,?,?,?,?,?,?,?,?)
     '''
     queryAttorneyTEMPList = list()
     for nt in partyDataListOfNTs:
          queryPartyList.append(
               tuple([
                    nt.partycounter,
                    nt.partyname, nt.partytype, nt.partyterminated,
                    caseheader_id, fullcase_text
               ])
          )
          #print "queryPartyList=\n\t{0}".format(queryPartyList)
          for attorney in nt.attorneyDataList:
               queryAttorneyTEMPList.append(
                    tuple([
                         nt.partycounter, 
                         attorney.attorneycounter, 
                         attorney.attorneyname, attorney.attorneystatus,
                         attorney.firmname, attorney.firmaddress, 
                         attorney.firmcity, attorney.firmstate, attorney.firmzip,
                         caseheader_id,fullcase_text
                    ])
               )
     try: 
          cursor.executemany(queryParty, queryPartyList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (queryParty, str(err)))          
     try:
          cursor.executemany(queryAttorneyTEMP, queryAttorneyTEMPList)
          db.commit()
          #need to execute attorney stuff here
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (queryAttorneyTEMP, str(err)))          


def updateCaseheaderSettingFlags(entryDict,db,caseheader_id):
     cursor = db.cursor()

     def makeFlags(entryDict,listOfFlagVarnames):
          dictOfMaxFlagValues = dict()
          for varname in listOfFlagVarnames:
               flagList = entryDict[varname]
               try:
                    dictOfMaxFlagValues[varname] = max(flagList)
               except:
                    dictOfMaxFlagValues[varname] = 0
          return dictOfMaxFlagValues
               
     dictOfMaxFlagValues = makeFlags(entryDict,['classactionflag','mdlflag'])
     #print "tuple={0}".format(
     #     tuple([dictOfMaxFlagValues['classactionflag'],
     #                       dictOfMaxFlagValues['mdlflag'],
     #                       caseheader_id]
     #      )
     #)
     tupleToInsert = tuple([dictOfMaxFlagValues['classactionflag'],
                            dictOfMaxFlagValues['mdlflag'],
                            caseheader_id]
     )
     query = '''
                         UPDATE caseheader 
                         SET anyclassactionflag=? , anymdlflag=? 
                         WHERE id = ?
     '''
     try:
          cursor.execute(query, (tupleToInsert))
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          


def convertNoneToEmptyString(var):
    if var is None:
         return ''
    else:
         return var

def insertIntoTables(dFRObject,db,wlfilename_id):
     def makeCasetextFromCaseheaderList(caseheaderList):
          return caseheaderList[0] + '_' + caseheaderList[1]

     outputList = dFRObject.output_list
     logging.info("Starting inserts at %s", dt.now())
     for docketOutput in outputList:
          caseheaderList = docketOutput[0]
          caseheaderList[0] = convertNoneToEmptyString(caseheaderList[0])
          #fullcase_id = getOrSetFullcaseId(caseheaderList,db)
          entryDict = docketOutput[1]
          #print "entryDict.keys()={0}".format(entryDict.keys())
          partyData = docketOutput[2]
          try:
               [caseheader_id,fullcase_text] = insertIntoCaseheaderTableAndReturnId(
                    caseheaderList,db,wlfilename_id)#,fullcase_id,wlfilename_id)
          except:
               mylogger.info("Bad docket for caseheaderList = %s", (caseheaderList))
          insertIntoEntryTable(entryDict,db,caseheader_id,fullcase_text)
          insertIntoPartyTable(partyData,db,caseheader_id,fullcase_text)
          updateCaseheaderSettingFlags(entryDict,db,caseheader_id)

################
# BOTTOM STUFF #
################

def printHeaderMaterial(listOfFiles):
     logging.info("List of files:\n\t%s" , listOfFiles)
     logging.info("Number of files=%s", len(listOfFiles))
     logging.info("Datetime at start is %s", dt.now())

def mainLoopFunction(listOfFiles,db):
     "reads in raw xml, extracts case-level variables, writes to ./csv/*.csv, and makes giant dataframe"
     myFirstLine="primaryTitle,court,docketNumber,filingDate,closedDate,natureOfSuit,natureOfSuitCode,wlFileName\n"
     counter = 0
     for file in listOfFiles:
          counter += 1
          #if counter==2:
          #    break
          logging.info("Starting %s", file)
          dFRObject = docketsFileReader(file, 
                                     processDocketFunction=getCaseInfo,
                                     nowrite=True,
                                     logger=mylogger,
                                     useDocketEntries=True,
                                     useBlockTypes = ['party']
                                )
          ##wlfilename_id = getWlFileNameID(os.path.basename(file),db)
          wlfilename_id = file
          insertIntoTables(dFRObject,db,wlfilename_id)
          logging.info("Done with file %s.", file)
     logging.info("Datetime at end is %s.\n\tDONE.", dt.now())


#do fullcase_id stuff
def executeClosingQueries(cursor):
     mq = myQueryExecuter(cursor)
     def populateUniqueEntry(db):#stopValue):
         maxIDTuple = cursor.execute('select max(id) from fullcase').fetchone()
         for idValue in range(1,maxIDTuple[0]):
              #if idValue>stopValue:
              #    break
              df = pd.read_sql_query('''
                                SELECT entrynumber, dateentry, entrytext, fullcase_id, caseheader_id
                                   FROM entry 
                                   WHERE fullcase_id=?
                                ''',
                                     db,params=(idValue,)
              )
              df.drop_duplicates(['entrynumber','dateentry','entrytext'],keep='first',inplace=True)
              df.to_sql('unique_entry',db,if_exists='append',index=False)

     def populateFullcase(mq):
          def createTempFullcase(mq):
               query = '''
                          CREATE TABLE temp_fullcase AS 
                          SELECT DISTINCT fullcase_text FROM caseheader
               '''
               mq.execute(query)
               return 1

          def insertIntoFullcaseFromTempFullcase(mq):
               query = '''
                          INSERT INTO fullcase 
                          SELECT rowid, fullcase_text from temp_fullcase
               '''
               mq.execute(query)
               #cursor.execute(query)
               return 1

          createTempFullcase(mq)
          insertIntoFullcaseFromTempFullcase(mq)
          return 1

     def createCaseheaderIndexes(mq):
          queryList = list()
          queryList.append('''CREATE INDEX idx_ch_natureofsuitcode on caseheader(natureofsuitcode)''')
          queryList.append('''CREATE INDEX idx_ch_fullcasetext ON caseheader(fullcase_text)''')
          queryList.append('''CREATE INDEX idx_natureofsuitcode on caseheader(natureofsuitcode)''')
          for query in queryList:
               mq.execute(query)
          return queryList

     def createFullcaseTextIndexes(mq):
          query = '''CREATE INDEX idx_fc_fullcasetext ON fullcase(fullcase_text)'''
          mq.execute(query)
          return query

     def updateCaseheaderSettingFullcaseId(mq):
          query = '''
                     UPDATE caseheader 
                          SET fullcase_id = (
                              SELECT id FROM fullcase 
                              WHERE fullcase_text = caseheader.fullcase_text
                          )
          '''
          mq.execute(query)
          return query

     def createEntryCaseheaderIDAndFullcaseIDIndexes(mq):
          queryList = list()
          queryList.append('''CREATE INDEX idx_entry_caseheader_id ON entry(caseheader_id)''')
          queryList.append('''CREATE INDEX idx_entry_fullcase_id ON entry(fullcase_id)''')
          for query in queryList:
               mq.execute(query)
          return queryList

     def updateEntrySettingFullcaseID(mq):
          query = '''UPDATE entry
                          SET fullcase_id = (
                              SELECT fullcase_id FROM caseheader
                              WHERE id = entry.caseheader_id
                          )
          '''
          mq.execute(query)
          return 1

     def dropTempFullcase(mq):
          query = '''DROP table temp_fullcase'''
          mq.execute(query)
          return 1

     def setAndIndexFirst50(mq):
          mq.execute(
               'UPDATE entry SET first50 = SUBSTR(entrytext, 1, 50)'
          )
          mq.execute(
               'CREATE INDEX idx_first50 ON entry(first50)'
          )
          return 1

     def createPartyIndex(mq):
          query = 'CREATE INDEX idx_party_fullcase_text on party(fullcase_text)'
          mq.execute(query)
          query = 'CREATE INDEX idx_party_partycounter on party(partycounter)'
          mq.execute(query)
          return 1

     def updatePartySettingFullcaseId(mq):
          query = '''
                     UPDATE party 
                          SET fullcase_id = (
                              SELECT id FROM fullcase 
                              WHERE fullcase_text = party.fullcase_text
                          )
          '''
          mq.execute(query)
          return 1

     def createAttorneyTEMPIndex(mq):
          query = 'CREATE INDEX idx_attorneyTEMP_fullcase_text on attorneyTEMP(fullcase_text)'
          mq.execute(query)
          return 1

     def updateAttorneyTEMPSettingFullcaseId(mq):
          query = '''
                     UPDATE attorneyTEMP
                          SET fullcase_id = (
                              SELECT id FROM fullcase 
                              WHERE fullcase_text = attorneyTEMP.fullcase_text
                          )
          '''
          mq.execute(query)
          return 1

     def createAttorneyTEMPIndexAfterSettingFullcaseId(mq):
          query = 'CREATE INDEX idx_attorneyTEMP_fullcase_id_partycounter on attorneyTEMP(fullcase_id, partycounter)'
          mq.execute(query)
          return 1

     def createAttorneySettingPartyId(mq):
          query1 = '''
                      CREATE TABLE dummyP AS SELECT 
                                id Pid, 
                                fullcase_id Pfullcase_id, 
                                partycounter Ppartycounter 
                      FROM party
          '''
          query2 = '''
                      CREATE TABLE attorney AS
                       SELECT * FROM attorneyTEMP
                        INNER JOIN dummyP ON 
                          attorneyTEMP.fullcase_id=dummyP.Pfullcase_id 
                          AND 
                          attorneyTEMP.partycounter=dummyP.Ppartycounter 
          '''
          query3 = '''
                      UPDATE attorney SET party_id = Pid
          '''
          mq.execute(query1)
          mq.execute(query2)
          mq.execute(query3)
          #query = '''
          #           UPDATE attorney 
          #                SET party_id = (
          #                    SELECT id FROM party 
          #                    WHERE
          #                          fullcase_id = attorney.fullcase_id and
          #                          partycounter = attorney.partycounter
          #                )
          #'''
          #mq.execute(query)
          return 1

     def createUniqueEntryIndex(mq):
          query = 'CREATE INDEX idx_fullcase_id on unique_entry(fullcase_id)'
          mq.execute(query)
          return 1

     def createAttorneyIndex(mq):
          query = 'CREATE INDEX idx_attorney_fullcase_text on attorney(fullcase_text)'
          mq.execute(query)
          return 1

     def createAttorneyIndexAfterSettingFullcaseId(mq):
          query = 'CREATE INDEX idx_attorney_fullcase_id_partycounter on attorney(fullcase_id, partycounter)'
          mq.execute(query)
          return 1

     populateFullcase(mq)
     createCaseheaderIndexes(mq)
     createFullcaseTextIndexes(mq)
     updateCaseheaderSettingFullcaseId(mq)
     createEntryCaseheaderIDAndFullcaseIDIndexes(mq)
     updateEntrySettingFullcaseID(mq)          

     createPartyIndex(mq)
     updatePartySettingFullcaseId(mq)

     createAttorneyTEMPIndex(mq)
     updateAttorneyTEMPSettingFullcaseId(mq)          
     createAttorneyTEMPIndexAfterSettingFullcaseId(mq);

     createAttorneySettingPartyId(mq)          
     createAttorneyIndex(mq)
     createAttorneyIndexAfterSettingFullcaseId(mq);
     dropTempFullcase(mq)

     populateUniqueEntry(cursor.connection)
     createUniqueEntryIndex(mq)

     setAndIndexFirst50(mq)
     db.commit()

   
#################################
##### MAIN PROCEDURAL BLOCK #####
#################################

searchAll = re.compile('.*xml$').search
dataDir = '/data2/dockets/preprocessed/'
listOfAllFiles = [ dataDir+l
                   for l in os.listdir("/data2/dockets/") 
                   for m in (searchAll(l),) if m]
#listOfAllFiles = [ 
#'dummy.xml' ,
#     '/data2/dockets/preprocessed/NFEDDIST2009Yale.Extracts+1.nxo_extracted_out.xml'
#                   '/data2/dockets/preprocessed/NFEDDIST2005Yale.Extracts+1.nxo_extracted_out.xml'
#]
#listOfAllFiles = ['/data2/dockets/preprocessed/NFEDDIST2006Yale.Extracts+8.nxo_extracted_out.xml']             #listOfAllFiles = listOfAllFiles[50:52]

print listOfAllFiles

logging.info("List of all files:", listOfAllFiles)
db = sqlite3.connect('/data2/dockets/sqlite/pydockets-devt.db')
c = db.cursor()
mainLoopFunction(listOfAllFiles,db)
executeClosingQueries(db.cursor())
db.close()
