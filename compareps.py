#!/usr/bin/env python
"""send record to kt
"""
import sys
import os
import time
import random
import copy
import cx_Oracle as orcl

from sendkt import *


class ComparePs(PsOrder):
    def __init(self):
        super(self.__class__,self).__init__()

class comparePsCreator(PsCreator):
    def __init__(self, orderFile, ktDs):
        self.startTime = time.time()
        self.endTime = None
        ktds = ktDs[0]
        self.oktDs = ktDs[1]
        super(self.__class__,self).__init__(orderFile, ktds)
        self.compareTask = None
        self.ps3Suc = 0
        self.ps4Suc = 0
        self.ps3Fail = 0
        self.ps4Fail = 0
        self.ps3Undone = 0
        self.ps4Undoen = 0
        self.passNum = 0
        self.failNum = 0

    def toOperate(self):
        self.log.writeLog('INFO', 'get ps')
        self.orderDs = self.openFile(self.orderFile, 'r')
        ps4 = self.getNextPs()
        while ps4:
            if not ps4:
                continue
            # ps.setKtDs(self.ktDs)
            ps3 = copy.deepcopy(ps4)
            ps4.getSubInfo(self.ktDs)
            ps3.getSubInfo(self.oktDs)
            ps3.sendTablePre = 'ps_provision'
            self.aPs.append([ps3, ps4])
            if ps4.regionCode:
                ps4.sendKt(self.ktDs)
                ps3.sendKt(self.oktDs)
            ps4 = self.getNextPs()
        self.closeFile(self.orderDs)
        self.log.writeLog('INFO', 'sended %d ps to kt' % self.psNum)
        self.unDone = self.psNum
        self.ps3Undone = self.psNum
        self.ps4Undone = self.psNum

        while (self.unDone):
            sleepTime = self.getSleepTime(self.unDone)
            self.endTime = time.time()
            spt = self.endTime - self.startTime
            self.log.writeLog('INFO', 'kt3 all: %d, sucess: %d, fail: %d, undone: %d; time: %d' % (self.psNum, self.ps3Suc, self.ps3Fail, self.ps3Undone, spt))
            self.log.writeLog('INFO', 'kt4 all: %d, sucess: %d, fail: %d, undone: %d; time: %d' % (self.psNum, self.ps4Suc, self.ps4Fail, self.ps4Undone, spt))
            self.log.writeLog('INFO', 'sleep: %d ...' % (sleepTime))
            time.sleep(sleepTime)
            self.unDone = self.qrySts(self.aPs)
            self.unDone += self.qryTask(self.aPs)
        self.endTime = time.time()
        spt = self.endTime - self.startTime
        self.log.writeLog('INFO', 'kt3 all: %d, sucess: %d, fail: %d, undone: %d; time: %d' % (
        self.psNum, self.ps3Suc, self.ps3Fail, self.ps3Undone, spt))
        self.log.writeLog('INFO', 'kt4 all: %d, sucess: %d, fail: %d, undone: %d; time: %d' % (
        self.psNum, self.ps4Suc, self.ps4Fail, self.ps4Undone, spt))
        self.comparePs(self.aPs)
        self.outDs = self.openFile(self.outName, 'w')
        self.writeRelt(self.aPs)
        self.closeFile(self.outDs)
        self.log.writeLog('INFO', 'write result completed')

    def writeRelt(self, aPses):
        self.outDs.write('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
            'num', 'case_name', 'psid_v3', 'psid_v4', 'billid', 'imsi', 'pass/fail', 'case_comment',
            'psStatus_v3', 'failReason_v3', 'psStatus_v4', 'failReason_v4'))
        caseNum = len(aPses)
        ps3Suc = 0
        ps4Suc = 0
        for pspaire in aPses:
            ps3 = pspaire[0]
            ps4 = pspaire[1]
            comparision = pspaire[2]
            try:
                self.outDs.write('%d,%s,%d,%d,%s,%s,%s,%s,%s,"%s",%s,"%s"\n' % (
                    ps4.psNo, ps4.psOrderName, ps3.psId, ps4.psId, ps4.billId, ps4.subBillId,comparision, ps4.comment, ps3.psStatus, ps3.failReason, ps4.psStatus, ps4.failReason))
            except IOError, e:
                self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outName, e))
        self.endTime = time.time()
        spt = self.endTime - self.startTime
        self.outDs.write('\n')
        self.outDs.write('kt3 all: %d, sucess: %d, fail: %d, undone: %d.\n' % (
            self.psNum, self.ps3Suc, self.ps3Fail, self.ps3Undone))
        self.outDs.write('kt4 all: %d, sucess: %d, fail: %d, undone: %d.\n' % (
            self.psNum, self.ps4Suc, self.ps4Fail, self.ps4Undone))
        self.outDs.write('pass: %d, fail: %d, time: %d.\n' % (self.passNum, self.failNum, spt))
        self.log.writeLog('INFO', 'pass: %d, fail: %d, time: %d.' % (self.passNum, self.failNum, spt))
        for pspaire in self.failPs:
            ps3 = pspaire[0]
            ps4 = pspaire[1]
            comparision = pspaire[2]
            self.log.writeLog('INFO','%d,%s,%d,%d,%s,%s,%s,%s,%s,"%s",%s,"%s"\n' % (
                    ps4.psNo, ps4.psOrderName, ps3.psId, ps4.psId, ps4.billId, ps4.subBillId,comparision, ps4.comment, ps3.psStatus, ps3.failReason, ps4.psStatus, ps4.failReason))
            

    def qrySts(self, aPses):
        unDone = 0
        for pspaire in aPses:
            ps3 = pspaire[0]
            ps4 = pspaire[1]
            if ps3.psStatus != 0 and ps4.psStatus != 0:
                continue
            if ps3.psStatus == 0:
                if not ps3.getSts(self.oktDs, self.month):
                    unDone += 1
                else:
                    self.ps3Undone -= 1
                    if ps3.psStatus == 9:
                        self.ps3Suc += 1
                    else:
                        self.ps3Fail += 1
            if ps4.psStatus == 0:
                if not ps4.getSts(self.ktDs, self.month):
                    unDone += 1
                else:
                    self.ps4Undone -= 1
                    if ps4.psStatus == 9:
                        self.ps4Suc += 1
                    else:
                        self.ps4Fail += 1
        return unDone

    def qryTask(self, aPses):
        unDone = 0
        for pspaire in aPses:
            ps3 = pspaire[0]
            ps4 = pspaire[1]
            ps3Tnum = len(ps3.aTask)
            ps4Tnum = len(ps4.aTask)
            if ps3Tnum > 0 and ps4Tnum > 0:
                continue
            if ps3Tnum == 0:
                if not ps3.getTasks(self.oktDs, self.month):
                    unDone += 1
            if ps4Tnum == 0:
                if not ps4.getTasks(self.ktDs, self.month):
                    unDone += 1
        return unDone

    def comparePs(self, aPses):
        self.failPs = []
        for pspaire in aPses:
            ps3 = pspaire[0]
            ps4 = pspaire[1]
            comparision = ps3.compare(ps4)
            pspaire.append(comparision)
            if comparision > 0:
                self.failNum += 1
                self.failPs.append(pspaire)
            else:
                self.passNum += 1


class compareMain(Main):
    def startProc(self):
        myPsCreator = comparePsCreator(self.cmdName, self.ktDs)
        myPsCreator.toOperate()

    def getConf(self):
        cfg = CmpaConf(self.cfgFile)
        cfg.setLogFile(self.logFile)
        log = Log(cfg)
        #		self.log = log
        log.writeLog('DEBUG', vars(cfg))
        self.ktDs = cfg.getDs()


class CmpaConf(Conf):
    def __init__(self, cfgFile):
        super(self.__class__, self).__init__(cfgFile)
        self.ocon = None
        fCfg = open(cfgFile, 'r')
        exec fCfg
        fCfg.close()
        if odbusr: self.odbusr = odbusr
        if opawd: self.opawd = opawd
        if osid: self.osid = osid
        if ohost: self.ohost = ohost
        if oport: self.oport = oport

    def getDs(self):
        super(self.__class__, self).getDs()
        if self.ocon: return (self.con,self.ocon)
        try:
            dsn = orcl.makedsn(self.ohost, self.oport, self.osid)
            dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            self.ocon = orcl.connect(self.odbusr, self.opawd, dsn)
            # log.writeLog('INFO', '%s %s' % (self.ohost, self.osid))
        # cursor = con.cursor()
        except Exception, e:
            print 'could not connec to oracle(%s:%s/%s %s), %s' % (self.ohost, self.oport, self.osid,self.odbusr, e)
            exit()
        return (self.con, self.ocon)



# main here
if __name__ == '__main__':
    'main'
    main = compareMain()
    main.getConf()
    log = Log(None)
    log.writeLog('INFO', '%s %s' % (main.baseName, 'Start process...'))
    main.startProc()

    log.writeLog('INFO', '%s %s' % (main.baseName, ' process completed.'))

    log.close()