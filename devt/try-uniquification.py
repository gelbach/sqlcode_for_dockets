
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
from setupLoggerAndReturn import setupLoggerAndReturn

from xmlFiles import *
from CaseLevelFunctions import *
from GeneralFunctions import *
from docketsFileReader_new import *

from myqueryexecuter import myQueryExecuter

mylogger = setupLoggerAndReturn('try-uniquification.py.log')
db = sqlite3.connect('/data2/dockets/sqlite/pydockets-devt.db')
c = db.cursor()
#mq = myQueryExecuter(c,mylogger)
#
##get list of fullcase_id values
#qFullcase = '''
#           SELECT id FROM fullcase;
#'''
#qUniqueEntry = '''
#                  SELECT entrynumber, dateentry, entrytext, 
#                         fullcase_id, caseheader_id
#                    FROM entry
#                    WHERE fullcase_id=?
#'''

maxIDTuple = c.execute('select max(id) from fullcase').fetchone()

#for highValue in rowCutList:
def myFunction(stopValue):
 for idValue in range(1,maxIDTuple[0]):
    if idValue>stopValue:
        break
    df = pd.read_sql_query('''
                           SELECT entrynumber, dateentry, entrytext, fullcase_id, caseheader_id
                              FROM entry 
                              WHERE fullcase_id=?
                           ''',
                           db,params=(idValue,))

    df.drop_duplicates(['entrynumber','dateentry','entrytext'],keep='first',inplace=True)
    df.to_sql('unique_entry',db,if_exists='append',index=False)
    
import time
start_time = time.time()
mylogger.info('Beginning at {}\n'.format(start_time))
stopValue = maxIDTuple[0]
#stopValue = 10
myFunction(stopValue)
end_time = time.time()
elapsed_time = end_time - start_time
mylogger.info('Finished at {}\n'.format(end_time))
mylogger.info('Elapsed time was {}'.format(elapsed_time))

