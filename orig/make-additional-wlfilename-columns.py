#primarytitle,filingdate,closeddate,natureofsuit,natureofsuitcode,noticeofremovalflag,
#dateentry,entrynumber,entrytext

import sqlite3
import os
import re

searchAll = re.compile('.*xml$').search
listOfAllFiles = [ l 
                   for l in os.listdir("/data2/dockets/") 
                   for m in (searchAll(l),) if m]

search2005_2011 = re.compile('^.*(200(5|6|7|8|9)|2010|2011).*$').search
search2005_2011_Remaining = re.compile('^.*Remaining20.*').search
search2012_JanJune = re.compile('^.*2012JanuaryThroughJune.*').search
search2012_JulSep = re.compile('^.*JulySept2012.*').search
search2012_OctDec = re.compile('^.*OctDec2012.*').search
search2013eoy = re.compile('^.*2013eoy.*').search
search2013 = re.compile('^.*2013.*').search
search2014_Dec = re.compile('^.*Dec2014.*').search
search2014 = re.compile('^.*2014.*').search

db = sqlite3.connect('/data2/dockets/sqlite/pydockets.db')
c = db.cursor()
#c.execute('''
#             ALTER TABLE wlfilename ADD COLUMN batch INTEGER 
#          '''
#      )

batch = dict()
for file in listOfAllFiles:
    #if not m:
    #    print file
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

    c.execute('''
                 UPDATE wlfilename SET batch=? WHERE wlfilename=?
              ''', (batch[file],file)
    )

#mylist=[]
#for key in batch.keys():
#    mylist.append(str(str(batch[key])+key))
#print '\n'.join(sorted(mylist))
print "Verifying that len(batch.keys())=360:", len(batch.keys())





db.commit()
db.close()

"""
NFEDDIST2014Yale.Extracts+10.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+11.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+12.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+13.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+14.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+15.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+16.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+17.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+18.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+19.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+1.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+20.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+21.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+22.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+2.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+3.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+4.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+5.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+6.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+7.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+8.nxo_extracted_out.xml
NFEDDIST2014Yale.Extracts+9.nxo_extracted_out.xml
NFEDDISTCV01Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV01Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV01Dec2014Part1Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV01Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV02Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV02Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV02Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV02Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+10.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+11.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+12.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+13.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+14.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+7.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+8.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part1Yale.Extracts+9.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV03Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+10.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+11.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+12.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+13.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+14.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+15.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+7.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+8.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part1Yale.Extracts+9.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV04Dec2014Part2Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV05Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV05Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV05Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV05Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part1Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV06Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part2Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV07Dec2014Part2Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV08Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV08Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV08Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part1Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+4.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+5.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+6.nxo_extracted_out.xml
NFEDDISTCV09Dec2014Part2Yale.Extracts+7.nxo_extracted_out.xml
NFEDDISTCV10Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV10Dec2014Part1Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV10Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV10Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV11Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV11Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV11Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
NFEDDISTCV11Dec2014Part2Yale.Extracts+3.nxo_extracted_out.xml
NFEDDISTCV12Dec2014Part1Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV12Dec2014Part2Yale.Extracts+1.nxo_extracted_out.xml
NFEDDISTCV12Dec2014Part2Yale.Extracts+2.nxo_extracted_out.xml
"""
