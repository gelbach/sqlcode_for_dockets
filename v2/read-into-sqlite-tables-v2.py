
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
sys.path.append('/home/gelbach/pydockets/python/hensler/github/Class-Actions-Project-with-DH/modules')
from henslerFunctions import CLASS_TEXT, MDL
from entryObject import entryObject

from xmlFiles import *
from CaseLevelFunctions import *
from GeneralFunctions import *
from docketsFileReader_new import *
from myqueryexecuter import myQueryExecuter
from setupLoggerAndReturn import setupLoggerAndReturn

#THISFILE = __file__
THISFILE = 'dummy'
mylogger = setupLoggerAndReturn(THISFILE + '.log')

dockettext_re = re.compile(r'(?P<docketmaintext>\d+:\d\d-[a-zA-Z]{2,3}-\d+)(?P<docketsupplementaltext>(-\w*){0,})')
def makeLeaddocketList(string):
    stringList = string.split(' ')
    accumulatedString = ''
    tupleList = list()
    id = 0
    for s in stringList:
        if dockettext_re.match(s) is not None:
            m = dockettext_re.match(s)
            id += 1
            mainText = m.group('docketmaintext')
            supplementalText = m.group('docketsupplementaltext')
            if supplementalText is None:
                supplementalText=''
            tupleList.append((id,mainText,supplementalText,accumulatedString))
            accumulatedString = ''  #reset
        else:
            accumulatedString = accumulatedString + s + ' '
    return tupleList

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
     caseheaderList = [court,
                       docketNumber,
                       primaryTitle,
                       filingDate,closedDate,
                       natureOfSuit,natureOfSuitCode
     ]
     dictOfEntriesInfo = object.docketEntriesDict
     listOfPartiesInfo = object.partiesDictList
     otherDocketsList = getOtherDockets(docket)
     leadDocketList = makeLeaddocketList(
          getValueNested(['lead.docket.number','lead.docket.block'],docket)
     )
     #return [caseheaderList,dictOfEntriesInfo,listOfPartiesInfo,otherDocketsList,leadDocketList]
     return {'caseheaderList':caseheaderList,
             'dictOfEntriesInfo':dictOfEntriesInfo,
             'listOfPartiesInfo':listOfPartiesInfo,
             'otherDocketsList':otherDocketsList,
             'leadDocketList':leadDocketList
        }

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
     if wlfilename == 'dummy.xml':
          return -9
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

def makeZipList(entryDict,caseheader_id,fullcase_text):
     def process_docketdescriptionList(docketdescriptionList):
         e = entryObject()
         ddl_pd_S = pd.Series(docketdescriptionList)
         classifyStringList = ddl_pd_S.apply(e.classifyString).tolist()
         classactionFlagList = ddl_pd_S.str.match(CLASS_TEXT).tolist()
         mdlFlagList = ddl_pd_S.str.match(MDL).tolist()
         return {'classifyStringList':classifyStringList,
                 'classactionFlagList':classactionFlagList,
                 'mdlFlagList':mdlFlagList
             }
     myDENList = entryDict['myDENCounter']
     numberList = entryDict['number']
     dateList = entryDict['date']
     docketdescriptionList = entryDict['docketdescription']
     p_dL_dict = process_docketdescriptionList(docketdescriptionList)
     classifyStringList = p_dL_dict['classifyStringList']
     classactionFlagList = p_dL_dict['classactionFlagList']
     mdlFlagList = p_dL_dict['mdlFlagList']

     caseheader_idList = [caseheader_id]*len(myDENList)
     fullcase_textList = [fullcase_text]*len(myDENList)
     zipList = zip(myDENList, numberList, dateList, docketdescriptionList,
                classifyStringList, classactionFlagList, mdlFlagList,
                caseheader_idList, fullcase_textList)
     flagDict = {'classactionflag_case':int(max(classactionFlagList)),
                 'mdlflag_case':int(max(mdlFlagList))
             }
     return {'zipList':zipList, 'flagDict':flagDict}

def insertIntoEntryTableAndReturnFlagDict(entryDict,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     makeZipListDict = makeZipList(entryDict,caseheader_id,fullcase_text)
     zipList = makeZipListDict['zipList']
     query = '''
                INSERT INTO entry
                 (id, my_entrynumber, entrynumber, dateentry, entrytext, 
                  entrytype, classactionflag, mdlflag,
                      caseheader_id,fullcase_text)
                 VALUES (null,?,?,?,?,?,?,?,?,?)
     '''
     try: 
          cursor.executemany(query, zipList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          
     return makeZipListDict['flagDict']

def updateCaseheaderTableSettingFlags(flagDict,db,caseheader_id):
    cursor = db.cursor()    
    try:
        cursor.execute("UPDATE caseheader SET classactionflag_case=?, mdlflag_case=? WHERE id = ?",
                       (flagDict['classactionflag_case'],flagDict['mdlflag_case'],caseheader_id)
        )
        db.commit()
    except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          

def insertIntoPartyAndAttorneyTables(partiesDictList,db,caseheader_id,fullcase_text):
     query_party = '''
     INSERT INTO party
     (partyid,partyname,partytype,caseheader_id,fullcase_text)
     VALUES (?,?,?,?,?)
     '''
     query_attorney = '''
     INSERT INTO attorney
     (attorneyid,attorneyname,attorneystatus,
     firm,firmcity,firmstate,
     partyid,caseheader_id,fullcase_text)
     VALUES (?,?,?,?,?,?,?,?,?)
     '''
     cursor = db.cursor()
     partyid = 0
     partyTupleList = list()
     attorneyTupleList = list()
     for party in partiesDictList:
          partyid += 1
          partyTupleList.append( (partyid, party['name'], party['type'], caseheader_id, fullcase_text) )
          if party['attorneyDictList'] is not None:
               attorneyid = 0
               for atty in party['attorneyDictList']:
                    attorneyid += 1
                    attorneyTupleList.append( (attorneyid,
                                               atty['attorneyname'],
                                               atty['attorneystatus'],
                                               atty['firm'],
                                               atty['firmcity'],
                                               atty['firmstate'],
                                               partyid, caseheader_id, fullcase_text)
                                         )
     try: 
          cursor.executemany(query_party, partyTupleList)
          cursor.executemany(query_attorney, attorneyTupleList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          


def insertIntoOtherdocketsTable(otherDocketsList,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     queryList = map(lambda n: n+(caseheader_id,fullcase_text),iter(otherDocketsList))
     query = '''
                INSERT INTO otherdocket
                 (otherdocketid, otherdocket, 
                      caseheader_id,fullcase_text)
                 VALUES (?,?,?,?)
     '''
     try: 
          cursor.executemany(query, queryList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          


def insertIntoLeaddocketsTable(leadDocketList,db,caseheader_id,fullcase_text):
     cursor = db.cursor()
     queryList = map(lambda n: n+(caseheader_id,fullcase_text),iter(leadDocketList))
     #print('queryList is ', queryList)
     query = '''
                INSERT INTO leaddocket
                 (leaddocketid, leaddocketmain, leaddocketsupplemental, accumulatedstring,
                      caseheader_id,fullcase_text)
                 VALUES (?,?,?,?,?,?)
     '''
     try: 
          cursor.executemany(query, queryList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          


def convertNoneToEmptyString(var):
    if var is None:
         return ''
    else:
         return var

def insertIntoTables(dFRObject,db,wlfilename_id):
     outputList = dFRObject.output_list
     logging.info("Starting inserts at %s", dt.now())
     for docketOutput in outputList:
          caseheaderList = docketOutput['caseheaderList']
          caseheaderList[0] = convertNoneToEmptyString(caseheaderList[0])
          #fullcase_id = getOrSetFullcaseId(caseheaderList,db)
          entryDict = docketOutput['dictOfEntriesInfo']
          partiesDictList = docketOutput['listOfPartiesInfo']
          otherDocketsList = docketOutput['otherDocketsList']
          leadDocketList = docketOutput['leadDocketList']          
          try:
               [caseheader_id,fullcase_text] = insertIntoCaseheaderTableAndReturnId(
                    caseheaderList,db,wlfilename_id)#,fullcase_id,wlfilename_id)
               flagDict = insertIntoEntryTableAndReturnFlagDict(entryDict,db,caseheader_id,fullcase_text) #returns case flag info
               updateCaseheaderTableSettingFlags(flagDict,db,caseheader_id)
               insertIntoPartyAndAttorneyTables(partiesDictList,db,caseheader_id,fullcase_text)
               insertIntoOtherdocketsTable(otherDocketsList,db,caseheader_id,fullcase_text)
               insertIntoLeaddocketsTable(leadDocketList,db,caseheader_id,fullcase_text)
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
     for filename in listOfFiles:
          counter += 1
          #if counter==2:
          #    break
          mylogger.info("Starting %s", filename)
          dFRObject = docketsFileReader(filename, 
                                     processDocketFunction=getCaseInfo,
                                     nowrite=True,
                                     logger=mylogger,
                                     useDocketEntries=True
                                )
          #wlfilename_id = insertIntoWlFileNameTable(os.path.basename(filename),db)
          wlfilename_id = getWlFileNameID(os.path.basename(filename),db)
          #wlfilename_id = 7
          insertIntoTables(dFRObject,db,wlfilename_id)
          mylogger.info("Done with filename %s.", filename)
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
listOfAllFiles = [ './dummy.xml' 
#     '/data2/dockets/preprocessed/NFEDDIST2009Yale.Extracts+1.nxo_extracted_out.xml'
#                   '/data2/dockets/preprocessed/NFEDDIST2005Yale.Extracts+1.nxo_extracted_out.xml'
              ]
#listOfAllFiles = ['/data2/dockets/preprocessed/NFEDDIST2006Yale.Extracts+8.nxo_extracted_out.xml']             
#listOfAllFiles = listOfAllFiles[50:52]

#print listOfAllFiles

mylogger.info("List of all files: {0}".format(listOfAllFiles))
db = sqlite3.connect('/data2/dockets/sqlite/pydockets-v2.db')
#c = db.cursor()
mainLoopFunction(listOfAllFiles,db)
#executeClosingQueries(db.cursor())
#db.close()

