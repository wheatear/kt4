#!/usr/bin/env python
"""emergency provisioning ps control model"""

import sys
import os
import time
import multiprocessing
import signal
import cx_Oracle as orcl
import psorder
from config import *

class Loader(multiprocessing.Process):
    def __init__(self,cfg,qusplite):
        multiprocessing.Process.__init__(self)
        self.cfg = cfg
        # self.conn = cfg.conn
        self.quPs = qusplite
        self.exit = None
        self.sleepTime = 1
        self.maxRowNum = 5

    def run(self):
        self.cfg.logWriter.pid = '%s %s' % (os.getpid(),self.name)
        self.cfg.getDb()
        # self.conn = self.cfg.db
        # self.conn.autocommit = 1
        signal.signal(signal.SIGTERM,self.sigHandler)
        # curQry = self.cfg.db.cursor()
        sql = 'select to_char(ps_id),bill_id,sub_bill_id,ps_param,ps_status,ps_net_code,fail_log,ps_service_code from EMPS_PROVISION where ps_status=0 and rownum<%d order by create_date,ps_id' % self.maxRowNum
        # curUpd = self.conn.cursor()
        # print cur,cur2
        # curQry.prepare(sql)
        # curUpd.prepare('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2')
        i = 0
        j = 0
        while not self.exit:
            i += 1
            # if i > 99:
            #     self.cfg.logWriter.writeLog('INFO', 'cursor close and reconn')
            #     curQry.close()
            #     # curUpd.close()
            #     curQry = self.cfg.db.cursor()
            #     # curUpd = self.conn.cursor()
            #
            #     # curUpd.prepare('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2')
            #     # curQry.prepare(sql)
            #     i = 0
            #     j += 1
            curQry = self.cfg.db.cursor()
            self.cfg.logWriter.writeLog('INFO','loop %d, execute sql: %s' % (i+j*100,sql))
            self.cfg.logWriter.writeLog('DEBUG', 'psQueue size: %d' % ( self.quPs.qsize()))
            # try:
            curQry.execute(sql)

            # except orcl.DatabaseError as exc:
            #     error, = exc.args
            #     self.cfg.logWriter.writeLog('ERROR', "Oracle-Error-Code: %d" %  error.code)
            #     self.cfg.logWriter.writeLog('ERROR', "Oracle-Error-Message:%s " %  error.message)
            #     self.cfg.logWriter.writeLog('ERROR', "Oracle-Error-offset:%d " % error.offset)
            #     self.cfg.logWriter.writeLog('ERROR', "Oracle-Error-context:%s " % error.context)
            #     self.cfg.logWriter.writeLog('ERROR', "Oracle-Error-isrecoverable:%s " % error.isrecoverable)
            #     self.cfg.logWriter.writeLog('ERROR','cursor: %s' %  curQry)
            #     self.cfg.logWriter.writeLog('ERROR','cache size: %d' %  curQry.connection.stmtcachesize)
            #     self.cfg.logWriter.writeLog('ERROR', 'cur description: %s' % curQry.description)
            #     self.cfg.logWriter.writeLog('ERROR', 'cur rowcount: %d' % curQry.rowcount)
            #     curQry.close()
            #     # self.conn.close()
            #     # self.cfg.getDb()
            #     # self.conn = self.cfg.db
            #     curQry = self.conn.cursor()
            #     # curQry.prepare(sql)
            #     time.sleep(self.sleepTime)
            #     continue
            rows = curQry.fetchall()
            self.cfg.logWriter.writeLog('INFO', 'load %d rows' % curQry.rowcount)
            # if rows is None:
            #     msg = 'load 0 rows. sleep 10'
            #     self.cfg.logWriter.writeLog('INFO', msg)
            #     time.sleep(self.sleepTime)
            #     continue
            # rowCount = len(rows)
            # msg = 'loading %d rows.' % rowCount
            # self.cfg.logWriter.writeLog('INFO', msg)
            for porder in rows:
                # print porder
                self.cfg.logWriter.writeLog('INFO', porder)
                ps = psorder.PsOrder(*porder)
                ps.psStatus = 1
                msg = 'load ps:%s, psQueue size: %d' % (ps.psId, self.quPs.qsize())
                self.cfg.logWriter.writeLog('DEBUG', msg)
                # msg = '%s : %d, %s' % (curUpd.statement, ps.psStatus, ps.psId)
                self.cfg.logWriter.writeLog('DEBUG', msg)
                curQry.execute('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2', (ps.psStatus, ps.psId))
                curQry.connection.commit()
                self.quPs.put(ps)
            # rows = None
            self.cfg.logWriter.writeLog('INFO', 'sleep %d' % self.sleepTime)
            # time.sleep(self.sleepTime)
            curQry.close()
        self.quPs.put(None)

    def sigHandler(self,signum,fram):
        msg = '%s received signal: %d' % (self.name,signum)
        self.cfg.logWriter.writeLog('INFO', msg)
        self.exit = True
        # self.quPs.put(None)

