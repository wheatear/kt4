#!/usr/bin/env python
'''emergency provisioning ps control model'''

import sys
import os
import time
import multiprocessing
import cx_Oracle as orcl
import psorder
from config import *

class Spliter(multiprocessing.Process):
    def __init__(self, cfg, qups,quhssnsn1,quhssnsn2,quhssnsn3,quhssnsn4,quhsshw5,quhsshw6,quhsshw7,quhsshw8):
        multiprocessing.Process.__init__(self)
        self.cfg = cfg
        # self.conn = cfg.conn
        self.quPs = qups
        self.quHssnsn1 = quhssnsn1
        self.quHssnsn2 = quhssnsn2
        self.quHssnsn3 = quhssnsn3
        self.quHssnsn4 = quhssnsn4
        self.quHsshw5 = quhsshw5
        self.quHsshw6 = quhsshw6
        self.quHsshw7 = quhsshw7
        self.quHsshw8 = quhsshw8
        self.regionTable = 'ps_net_number_area'
        self.dServiceKey = {'SUSPEND=3;':'SUSPEND=3;', 'EPS=1;':'EPS=1;', 'EPS=2;STATUS=GRANTED;':'EPS=2;', 'GPRS=1;':'GPRS=1;'}
        # self.exit = None

    def run(self):
        self.cfg.logWriter.pid = '%s %s' % (os.getpid(),self.name)
        self.cfg.getDb()
        self.conn = self.cfg.db
        curQry = self.conn.cursor()
        curUpdSvc = self.conn.cursor()
        curUpdFail = self.conn.cursor()
        sql = 'select ps_net_code from %s where :1 between start_number and end_number' % self.regionTable
        curQry.prepare(sql)
        curUpdSvc.prepare('update  EMPS_PROVISION set ps_status=:1,ps_net_code=:2,ps_service_code=:3 where ps_id=:4')
        curUpdFail.prepare('update EMPS_PROVISION set ps_status=:1,fail_reason=:2 where ps_id=:3')
        while True:
            msg = 'psQueue size: %d' % (self.quPs.qsize())
            self.cfg.logWriter.writeLog('DEBUG', msg)
            try:
                ps = self.quPs.get()
            except KeyboardInterrupt, e:
                msg = 'psQueue been KeyboardInterrupt, spliter process exit: %s' %  e
                self.cfg.logWriter.writeLog('INFO', msg)
                break
            if ps is None:
                self.cfg.logWriter.writeLog('INFO', 'spliter exit.')
                break
            ps.psNetCode = self.mapNetCode(curQry, curUpdFail, ps)
            if ps.psNetCode is None:
                self.moveHis(self.conn,ps)
                continue
            ps.psServiceCode = self.mapServiceCode(curUpdFail, ps)
            if ps.psServiceCode is None:
                self.moveHis(self.conn,ps)
                continue
            if ps.psNetCode == 'HSSNSN1':
                self.quHssnsn1.put(ps)
            elif ps.psNetCode == 'HSSNSN2':
                self.quHssnsn2.put(ps)
            elif ps.psNetCode == 'HSSNSN3':
                self.quHssnsn3.put(ps)
            elif ps.psNetCode == 'HSSNSN4':
                self.quHssnsn4.put(ps)
            elif ps.psNetCode == 'HSSHW5':
                self.quHsshw5.put(ps)
            elif ps.psNetCode == 'HSSHW6':
                self.quHsshw6.put(ps)
            elif ps.psNetCode == 'HSSHW7':
                self.quHsshw7.put(ps)
            elif ps.psNetCode == 'HSSHW8':
                self.quHsshw8.put(ps)

            msg = 'splite success: %s %s %s %s' % (ps.psStatus, ps.psNetCode, ps.psServiceCode, ps.psId)
            self.cfg.logWriter.writeLog('DEBUG', msg)
            curUpdSvc.execute(None, (ps.psStatus, ps.psNetCode, ps.psServiceCode, ps.psId))
            curUpdSvc.connection.commit()
            msg = 'hssQueue size: HSSNSN1:%d, HSSNSN2:%d, HSSNSN3:%d, HSSNSN4:%d, HSSHW5:%d, HSSHW6:%d, HSSHW7:%d, HSSHW8:%d' % (self.quHssnsn1.qsize(),self.quHssnsn2.qsize(),self.quHssnsn3.qsize(),self.quHssnsn4.qsize(),self.quHsshw5.qsize(),self.quHsshw6.qsize(),self.quHsshw7.qsize(),self.quHsshw8.qsize())
            self.cfg.logWriter.writeLog('DEBUG', msg)
            # self.cfg.logWriter.writeLog('INFO', 'sleep 10')
            # time.sleep(10)

    def moveHis(self,conn,ps):
        msg = 'move to his table: %s' % ps.psId
        self.cfg.logWriter.writeLog('DEBUG', msg)
        cur = conn.cursor()
        cur.execute("insert into EMPS_PROVISION_HIS select * from EMPS_PROVISION where ps_id=:1", (ps.psId,))
        cur.execute("delete from EMPS_PROVISION where ps_id=:1", (ps.psId,))
        conn.commit()
        cur.close()

    def updateServiceCode(self,conn,ps):
        cur = conn.cursor()


    def mapNetCode(self, cur, curUpdFail, ps):
        msg = '%s; %s' % (cur.statement, ps.billId)
        self.cfg.logWriter.writeLog('DEBUG', msg)
        cur.execute(None,(ps.billId,))
        row = cur.fetchone()
        if row is None:
            ps.psStatus = -1
            ps.failReason = "can't find ps_net_code"
            msg = '%s; %s' % (curUpdFail.statement, ps.psId)
            self.cfg.logWriter.writeLog('DEBUG', msg)
            curUpdFail.execute(None,(-1,ps.failReason, ps.psId))
            curUpdFail.connection.commit()
            return None
        psNetCode = row[0]
        psNetCode = psNetCode[:-1]
        ps.psNetCode = psNetCode
        ps.ps_status = 2
        msg = 'get billid: %s ps_net_code: %s' % (ps.billId,psNetCode)
        self.cfg.logWriter.writeLog('DEBUG', msg)
        return psNetCode

    def mapServiceCode(self, curUpdFail, ps):
        serviceCode = None
        for (sKey,sValue) in self.dServiceKey.items():
            if ps.psParam.find(sKey) > -1:
                serviceCode = sValue
        if serviceCode is None:
            ps.psStatus = -1
            ps.failReason = "can't find ps_service_code"
            msg = '%s; %s' % (curUpdFail.statement, ps.psId)
            self.cfg.logWriter.writeLog('DEBUG', msg)
            curUpdFail.execute(None, (-1, ps.failReason, ps.psId))
            curUpdFail.connection.commit()
            return None
        ps.ps_status = 3
        ps.psServiceCode = serviceCode
        msg = 'get psid: %s psparam: %s  ps_service_code: %s' % (ps.psId, ps.psParam, serviceCode)
        self.cfg.logWriter.writeLog('DEBUG', msg)
        return serviceCode

