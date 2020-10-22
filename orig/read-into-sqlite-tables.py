
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

import numpy as np
import pandas as pd

THISFILE = 'read-into-sqlite-tables.py'

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



######################
# DEFINE getCaseInfo #
######################

def getCaseInfo(object):
     """
     Sample processDocketFunction function. 
     MUST RETURN A LIST! First item will be written to csv file
     """
     docket = object.docket
     primaryTitle = getValue('primary.title',docket).replace('"','')
     court        = getCourt(getValue('court',docket))
     docketNumber = getValueNested(['docket.number','docket.block'],docket)
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

def getCaseheaderList(docketOutput):
     return docketOutput[0]

def insertIntoCaseheaderTableAndReturnId(caseheaderList,db,wlfilename_id):
     cursor = db.cursor()
     caseheaderList.extend([wlfilename_id])
     tupleToInsert = tuple(caseheaderList)
     query = '''
                         INSERT INTO caseheader 
                         (id,court,docketnumber,primarytitle,filingdate,closeddate,
                          natureofsuit,natureofsuitcode,
                          wlfilename_id
                         )
                         VALUES (null,?,?,?,?,?,?,?,?)
     '''
     try:
          cursor.execute(query, (tupleToInsert))
          db.commit()
     except Exception as err:
          print('caseheaderList is: ', caseheaderList)
          print('Query Failed: %s\nError: %s' % (query, str(err)))          
     return cursor.lastrowid

def makeZipList(entryDict,caseheader_id):
     myDENList = entryDict['myDENCounter']
     numberList = entryDict['number']
     dateList = entryDict['date']
     docketdescriptionList = entryDict['docketdescription']
     caseheader_idList = [caseheader_id]*len(myDENList)
     return zip(myDENList, numberList, dateList, docketdescriptionList,caseheader_idList)

def insertIntoEntryTable(entryDict,db,caseheader_id):
     cursor = db.cursor()
     zipList = makeZipList(entryDict,caseheader_id)
     query = '''
                INSERT INTO entry
                 (id, my_entrynumber, entrynumber, dateentry, entrytext, caseheader_id)
                 VALUES (null,?,?,?,?,?)
     '''
     try: 
          cursor.executemany(query, zipList)
          db.commit()
     except Exception as err:
          print('Query Failed: %s\nError: %s' % (query, str(err)))          

def insertIntoTables(dFRObject,db,wlfilename_id):
     outputList = dFRObject.output_list
     logging.info("Starting inserts at %s", dt.now())
     for docketOutput in outputList:
          caseheaderList = docketOutput[0]
          entryDict = docketOutput[1]
          caseheader_id = insertIntoCaseheaderTableAndReturnId(caseheaderList,db,wlfilename_id)
          insertIntoEntryTable(entryDict,db,caseheader_id)

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
                                     useDocketEntries=True
                                )
          wlfilename_id = insertIntoWlFileNameTable(os.path.basename(file),db)
          insertIntoTables(dFRObject,db,wlfilename_id)
          logging.info("Done with file %s.", file)
     logging.info("Datetime at end is %s.\n\tDONE.", dt.now())

   
#################################
##### MAIN PROCEDURAL BLOCK #####
#################################

searchAll = re.compile('.*xml$').search
listOfAllFiles = [ '/data2/dockets/'+l 
                   for l in os.listdir("/data2/dockets/") 
                   for m in (searchAll(l),) if m]
#listOfAllFiles = [ 'dummy.xml' ,
#                   '/data2/dockets/NFEDDIST2005Yale.Extracts+1.nxo_extracted_out.xml'
#              ]
#listOfAllFiles = ['/data2/dockets/NFEDDIST2006Yale.Extracts+8.nxo_extracted_out.xml']             #listOfAllFiles = listOfAllFiles[50:52]

logging.info("List of all files:", listOfAllFiles)
db = sqlite3.connect('/data2/dockets/sqlite/pydockets.db')
mainLoopFunction(listOfAllFiles,db)
db.close()


