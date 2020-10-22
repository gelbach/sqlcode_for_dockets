
from orderedset import OrderedSet
from datetime import datetime as dt
import os
import traceback
import re
import csv
from lxml import etree as ET
import sqlite3
#import xml.etree.cElementTree as ET

import sys
sys.path.append('/data2/dockets/utilities')

from setupLoggerAndReturn import setupLoggerAndReturn

#import logging

from xmlFiles import *
from CaseLevelFunctions import *
from GeneralFunctions import *
from docketsFileReader_new import *

import numpy as np
import pandas as pd

def preProcessDocketFile(filename):
     "Preprocess docket file to skip non-party.block stuff"
     with open (filename,'r') as tfile:
          mystring = ''
          partyBlockYet = 0
          for line in tfile:
               if partyBlockYet==1:
                    mystring += line 
               if line == '<party.block>':
                    partyBlockYet = 1 
               elif line == '</party.block>':
                    partyBlockYet = 0 
     return mystring

mylogger = setupLoggerAndReturn('inspect-party-block.py.log')

################
# BOTTOM STUFF #
################

def printHeaderMaterial(listOfFiles):
     logging.info("List of files:\n\t%s" , listOfFiles)
     logging.info("Number of files=%s", len(listOfFiles))
     logging.info("Datetime at start is %s", dt.now())

searchAll = re.compile('.*xml$').search
dataDir = '/data2/dockets/'#preprocessed/'
listOfAllFiles = [ dataDir+l
                   for l in os.listdir("/data2/dockets/") 
                   for m in (searchAll(l),) if m]
#listOfAllFiles = ['/data2/dockets/preprocessed/NFEDDIST2006Yale.Extracts+8.nxo_extracted_out.xml']#             #listOfAllFiles = listOfAllFiles[50:52]

#print listOfAllFiles

partyBlockChildrenSet = OrderedSet([])
tagList = OrderedSet([])
printString = ''
for file in listOfAllFiles:
     printString = 'Starting ' + file + '\n'
     #mystring = '<dockets>' + preProcessDocketFile(file) + '</dockets>'
     #print mystring
     try:
          tree = ET.parse(file)
          root = tree.getroot()
          partyBlockList = root.iterfind('.//party.block')
          for block in partyBlockList:
               for child in block:
                    partyBlockChildrenSet.add(child.tag)
                    #now get list of all tags that are children of partyBlock
               for elem in block.iterdescendants():
                    if elem.tag is not None:
                         tagList.add(elem.tag)
          printString += ','.join(str(t) for t in sorted(list(partyBlockChildrenSet)))
          mylogger.info(printString)
     except:
          mylogger.info('\nBad parse for file ' + file + '\n\n')
mylogger.info(tagList)
