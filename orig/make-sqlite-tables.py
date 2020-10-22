#primarytitle,filingdate,closeddate,natureofsuit,natureofsuitcode,noticeofremovalflag,
#dateentry,entrynumber,entrytext

import sqlite3
import os

db = sqlite3.connect('/data2/dockets/sqlite/pydockets.db')
c = db.cursor()

c.execute('''DROP TABLE IF EXISTS wlfilename''')
c.execute('''DROP TABLE IF EXISTS caseheader''')
c.execute('''DROP TABLE IF EXISTS entry''')

c.execute('''CREATE TABLE wlfilename (
               id integer PRIMARY KEY AUTOINCREMENT, 
               wlfilename text UNIQUE NOT NULL)''')

c.execute('''
             CREATE TABLE caseheader (
               id integer PRIMARY KEY AUTOINCREMENT,
               court text,
               docketnumber text, 
               primarytitle text,
               filingdate text,
               closeddate text,
               natureofsuit text,
               natureofsuitcode integer,
               wlfilename_id integer NOT NULL,
               CONSTRAINT fk_wlfilename
                 FOREIGN KEY(wlfilename_id)
                 REFERENCES wlfilename(id)
                 ON DELETE CASCADE
             )
          '''
         )

c.execute('''
             CREATE TABLE entry (
               id integer UNIQUE PRIMARY KEY AUTOINCREMENT, 
               my_entrynumber integer NOT NULL,
               entrynumber text,
               dateentry text,
               entrytext text,
               caseheader_id integer NOT NULL,
               CONSTRAINT fk_caseheader
                 FOREIGN KEY(caseheader_id)
                 REFERENCES caseheader(id)
                 ON DELETE CASCADE
             )
          '''
         )

db.commit()
db.close()

