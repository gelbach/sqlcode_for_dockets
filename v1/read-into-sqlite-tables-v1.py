
import numpy as np
import pandas as pd
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
from docketsFileReader_new import *
from myqueryexecuter import myQueryExecuter
from setupLoggerAndReturn import setupLoggerAndReturn

THISFILE = __file__
mylogger = setupLoggerAndReturn(THISFILE + '.log')

######################
# DEFINE getCaseInfo #
######################

def getCaseInfo(object):
     """
     processDocketFunction function. 
     MUST RETURN A LIST!
     """
     docket = object.docket
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
     return [caseheaderList,dictOfEntriesInfo]

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

def getCaseheaderList(docketOutput):
     return docketOutput[0]

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
     caseheader_idList = [caseheader_id]*len(myDENList)
     fullcase_textList = [fullcase_text]*len(myDENList)
     return zip(myDENList, numberList, dateList, docketdescriptionList,
                fullcase_textList, caseheader_idList)

def insertIntoEntryTable(entryDict,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     zipList = makeZipList(entryDict,caseheader_id,fullcase_text)
     query = '''
                INSERT INTO entry
                 (id, my_entrynumber, entrynumber, dateentry, entrytext, 
                      caseheader_id,fullcase_text)
                 VALUES (null,?,?,?,?,?,?)
     '''
     try: 
          cursor.executemany(query, zipList)
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

     """
     def getFullcaseId(casetext,cursor):
          query = '''
                         SELECT id FROM fullcase WHERE casetext = ?
                  '''
          try:
               cursor.execute(query, (casetext,))
          except Exception as err:
               print('Query Failed: %s\nError: %s' % (query, str(err)))
          resultset = cursor.fetchall()
          try:
               assert len(resultset)==1      #list of results
               assert len(resultset[0])==1   #tuple 
               fullcase_id = resultset[0][0] #number
               return fullcase_id 
          except:
               raise

     def getOrSetFullcaseId(caseheaderList,db):
          cursor = db.cursor()
          casetext = makeCasetextFromCaseheaderList(caseheaderList)
          try:
               fullcase_id = getFullcaseId(casetext,cursor)
          except:
               query = '''
                          INSERT INTO fullcase(casetext) VALUES (?)
                       '''
               try:
                    cursor.execute(query, (casetext,))
               except Exception as err:
                    print('Query Failed: %s\nError: %s' % (query, str(err)))
               fullcase_id = cursor.lastrowid
          return fullcase_id
     """

     outputList = dFRObject.output_list
     logging.info("Starting inserts at %s", dt.now())
     for docketOutput in outputList:
          caseheaderList = docketOutput[0]
          caseheaderList[0] = convertNoneToEmptyString(caseheaderList[0])
          #fullcase_id = getOrSetFullcaseId(caseheaderList,db)
          entryDict = docketOutput[1]
          try:
               [caseheader_id,fullcase_text] = insertIntoCaseheaderTableAndReturnId(
                    caseheaderList,db,wlfilename_id)#,fullcase_id,wlfilename_id)
               insertIntoEntryTable(entryDict,db,caseheader_id,fullcase_text)
          except:
               mylogger.info("Bad docket for caseheaderList = %s", (caseheaderList))

################
# BOTTOM STUFF #
################

def printHeaderMaterial(listOfFiles):
     mylogger.info("List of files:\n\t%s" , listOfFiles)
     mylogger.info("Number of files=%s", len(listOfFiles))
     mylogger.info("Datetime at start is %s", dt.now())

def mainLoopFunction(listOfFiles,db):
     "reads in raw xml, extracts case-level variables, writes to ./csv/*.csv, and makes giant dataframe"
     myFirstLine="primaryTitle,court,docketNumber,filingDate,closedDate,natureOfSuit,natureOfSuitCode,wlFileName\n"
     counter = 0
     for file in listOfFiles:
          counter += 1
          #if counter==2:
          #    break
          mylogger.info("Starting %s", file)
          dFRObject = docketsFileReader(file, 
                                     processDocketFunction=getCaseInfo,
                                     nowrite=True,
                                     logger=mylogger,
                                     useDocketEntries=True
                                )
          #wlfilename_id = insertIntoWlFileNameTable(os.path.basename(file),db)
          wlfilename_id = getWlFileNameID(os.path.basename(file),db)
          insertIntoTables(dFRObject,db,wlfilename_id)
          mylogger.info("Done with file %s.", file)
     mylogger.info("Datetime at end is %s.\n\tDONE.", dt.now())


#do fullcase_id stuff
def executeClosingQueries(cursor):
     mq = myQueryExecuter(cursor,mylogger)

     def populateUniqueEntry(db):#stopValue):
          cursor = db.cursor()
          maxIDTuple = cursor.execute('select max(id) from fullcase').fetchone()
          for idValue in range(1,maxIDTuple[0]):
               #if idValue>stopValue:
               #    break
               df = pd.read_sql_query('''
                                SELECT entrynumber, dateentry, entrytext, fullcase_id, caseheader_id
                                   FROM entry 
                                   WHERE fullcase_id=?
               ''',
                    db,params=(idValue,))
               df.drop_duplicates(['entrynumber','dateentry','entrytext'],keep='first',inplace=True)
               df.to_sql('unique_entry',db,if_exists='append',index=False)
          return 1

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

     def createFullcaseTextIndexes(mq):
          query1 = '''CREATE INDEX idx_ch_fullcasetext ON caseheader(fullcase_text)
          '''
          query2 = '''CREATE INDEX idx_fc_fullcasetext ON fullcase(fullcase_text)
          '''
          mq.execute(query1)
          mq.execute(query2)
          return 1

     def updateCaseheaderSettingFullcaseId(mq):
          query = '''
                     UPDATE caseheader 
                          SET fullcase_id = (
                              SELECT id FROM fullcase 
                              WHERE fullcase_text = caseheader.fullcase_text
                          )
          '''
          mq.execute(query)
          return 1

     def createEntryCaseheaderIDAndFullcaseIDIndexes(mq):
          query = '''CREATE INDEX idx_entry_caseheader_id ON entry(caseheader_id)
          '''
          mq.execute(query)
          query = '''CREATE INDEX idx_entry_fullcase_id ON entry(fullcase_id)
          '''
          mq.execute(query)
          return 1

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
               'UPDATE unique_entry SET first50 = SUBSTR(entrytext, 1, 50)'
          )
          mq.execute(
               'CREATE INDEX idx_first50 ON unique_entry(first50)'
          )
          return 1

     def createUniqueEntryIndex(mq):
          query = 'CREATE INDEX idx_fullcase_id on unique_entry(fullcase_id)'
          mq.execute(query)
          return 1

     populateFullcase(mq)
     createFullcaseTextIndexes(mq)
     updateCaseheaderSettingFullcaseId(mq)
     createEntryCaseheaderIDAndFullcaseIDIndexes(mq)
     updateEntrySettingFullcaseID(mq)          
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
#listOfAllFiles = [ #'dummy.xml' ,
#     '/data2/dockets/preprocessed/NFEDDIST2009Yale.Extracts+1.nxo_extracted_out.xml'
#                   '/data2/dockets/preprocessed/NFEDDIST2005Yale.Extracts+1.nxo_extracted_out.xml'
#              ]
#listOfAllFiles = ['/data2/dockets/preprocessed/NFEDDIST2006Yale.Extracts+8.nxo_extracted_out.xml']             #listOfAllFiles = listOfAllFiles[50:52]

#print listOfAllFiles

mylogger.info("List of all files: %" % listOfAllFiles)
db = sqlite3.connect('/data2/dockets/sqlite/pydockets-v1.db')
c = db.cursor()
mainLoopFunction(listOfAllFiles,db)
executeClosingQueries(db.cursor())
db.close()
