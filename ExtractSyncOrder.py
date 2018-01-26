#!/usr/bin/env python

"""ps parse tool"""

import sys
import os
import time
import datetime
import getopt
import random
import re
import signal
import logging
# import multiprocessing
import cx_Oracle as orcl
from socket import *


class KtClient(object):
    def __init__(self):
        # self.cfgFile = cfgfile
        self.ktName = None
        self.ktType = None
        self.dbUser = None
        self.dbPwd = None
        self.dbHost = None
        self.dbPort = None
        self.dbSid = None
        self.orderTablePre = 'i_provision'
        self.syncServer = None
        self.sockPort = None

        self.conn = None
        self.tcpClt = None
        self.month = time.strftime("%Y%m", time.localtime())

        self.dCursors = {}

    def connDb(self):
        if self.conn: return self.conn
        try:
            connstr = '%s/%s@%s/%s' % (self.dbUser, self.dbPwd, self.dbHost, self.dbSid)
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
        except Exception, e:
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.dbHost, self.dbUser, self.dbSid, e)
            exit()
        return self.conn

    def getCur(self, sql):
        if sql in self.dCursors: return self.dCursors[sql]
        cur = self.prepareSql(sql)
        self.dCursors[sql] = cur
        return cur

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError, e:
            logging.error('prepare sql err: %s\n%s', e, sql)
            return None
        return cur

    def closeAllCur(self):
        logging.info('close all opened cursor.')
        for curName in self.dCursors:
            cur = self.dCursors[curName]
            logging.info('close cur: %s' % cur.statement)
            cur.close()

    def executeCur(self, cur, params=None):
        logging.info('execute cur %s', cur.statement)
        try:
            if params is None:
                cur.execute(None)
            else:
                cur.execute(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s\n%s\n%s ', e, cur.statement, cur.bindvars)
            return e
        return cur

    def executemanyCur(self, cur, params):
        # logging.info('execute cur %s', cur.statement)
        # logging.info('params %s', params)
        try:
            cur.executemany(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s\n%s\n%s ', e, cur.statement, cur.bindvars)
            return e
        return cur


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.aClient = []

    def loadLogLevel(self):
        try:
            fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            print('Can not open configuration file %s: %s' % (self.cfgFile, e))
            exit(2)
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            if line[:8] == 'LOGLEVEL':
                param = line.split(' = ', 1)
                logLevel = 'logging.%s' % param[1]
                self.logLevel = eval(logLevel)
                break
        fCfg.close()

    def loadClient(self):
        # super(self.__class__, self).__init__()
        # for cli in self.aClient:
        #     cfgFile = cli.
        try:
            fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit()
        clientSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                clientSection = 0
                if client is not None: self.aClient.append(client)
                client = None
                continue
            if line == '#provisioning client conf':
                clientSection = 1
                client = KtClient()
                continue
            if clientSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if param[0] == 'prvnName':
                client.ktName = param[1]
            elif param[0] == 'dbusr':
                client.dbUser = param[1]
            elif param[0] == 'type':
                client.ktType = param[1]
            elif param[0] == 'dbpwd':
                client.dbPwd = param[1]
            elif param[0] == 'dbhost':
                client.dbHost = param[1]
            elif param[0] == 'dbport':
                client.dbPort = param[1]
            elif param[0] == 'dbsid':
                client.dbSid = param[1]
            elif param[0] == 'table':
                client.orderTablePre = param[1]
            elif param[0] == 'server':
                client.syncServer = param[1]
            elif param[0] == 'sockPort':
                client.sockPort = param[1]
        fCfg.close()
        logging.info('load %d clients.', len(self.aClient))
        return self.aClient


class KtCase(object):
    """provisioning synchronous order case"""
    sqlSaveSyncCase = 'insert into %s(log_name,case_num,ps_service_type,action_id,sync_param,ps_id,bill_id,sub_bill_id,create_date,region_code)' \
          ' values(:log_name,:case_num,:ps_service_type,:action_id,:sync_param,:ps_id,:bill_id,:sub_bill_id,:create_date,:region_code)'
    sqlCreateTable = """create table %s
        (
          log_name         VARCHAR2(100),
          case_num        number(10),
          ps_service_type  VARCHAR2(100),
          action_id       NUMBER(4),
          sync_param        VARCHAR2(4000),
          ps_id           VARCHAR2(64),
          bill_id         VARCHAR2(64),
          sub_bill_id     VARCHAR2(64),
          create_date     DATE,
          region_code     VARCHAR2(6),
          notes           VARCHAR2(2000)
        )
        """
    def __init__(self, logName,caseNum, psServiceType, actionId, psParam, billId, subBillId):
        self.logName = logName
        self.psCaseNum = caseNum
        self.psId = None
        self.psServiceType = psServiceType
        self.billId = billId
        self.subBillId = subBillId
        self.actionId = actionId
        self.psParam = psParam
        self.createDate = datetime.datetime.now()
        self.regionCode = 'null'
        self.comment = None
        self.client = None
        # self.curPs = None
        self.psTable = ''

    def createResultTable(self):
        createTableSql = KtCase.sqlCreateTable % self.psTable
        logging.info('create table %s' % createTableSql)
        # logging.info(createIndexSql)
        # cur = self.client.getCur(createTableSql)
        cur = self.client.conn.cursor()
        try:
            # self.client.executeCur(cur, createTableSql)
            cur.execute(createTableSql)
            # cur.execute(createIndexSql)
        except orcl.DatabaseError, e:
            logging.error('can not execute Sql: %s : %s', cur.statement, e)
        cur.close()

    def savePs(self, psTable):
        # if self.curPs is None:
        sql = KtCase.sqlSaveSyncCase % self.psTable
        cur = self.client.getCur(sql)
        psValues = {'log_name': self.logName, 'case_num': self.psCaseNum, 'ps_service_type': self.psServiceType, 'action_id': self.actionId,
                    'sync_param': self.psParam, 'ps_id':self.psId, 'bill_id':self.billId, 'sub_bill_id':self.subBillId,
                    'create_date':self.createDate, 'region_code':self.regionCode}
        logging.debug(cur.statement)
        logging.debug(psValues)
        insrt = self.client.executeCur(cur, psValues)
        if insrt.__str__().find('ORA-00942') > -1:
            logging.info('no result table, create the result table: %s', self.psTable)
            self.createResultTable()
            insrt = self.client.executeCur(cur, psValues)
        cur.connection.commit()


class KtCaseGrp(KtCase):
    def __init__(self, maxNum=1000):
        self.maxNum = maxNum
        self.aCase = []

    def savePs(self, psTable):
        sql = KtCase.sqlSaveSyncCase % psTable
        cur = self.client.getCur(sql)
        aPsValues = []
        i = 0
        for case in self.aCase:
            psValue = {'log_name': case.logName, 'case_num': case.psCaseNum, 'ps_service_type': case.psServiceType, 'action_id': case.actionId,
                    'sync_param': case.psParam, 'ps_id':case.psId, 'bill_id':case.billId, 'sub_bill_id':case.subBillId,
                    'create_date':case.createDate, 'region_code':case.regionCode}
            aPsValues.append(psValue)
            i += 1
        logging.debug(cur.statement)
        logging.info('save %d ps', i)
        # logging.debug(aPsValues)
        insrt = self.client.executemanyCur(cur, aPsValues)
        if insrt.__str__().find('ORA-00942') > -1:
            logging.info('no result table, create the result table: %s', self.psTable)
            self.createResultTable()
            insrt = self.client.executemanyCur(cur, aPsValues)
        cur.connection.commit()
        return i


class ExtractOrder(object):
    def __init__(self, syncLog, client, resultDs):
        self.syncLog = syncLog
        self.client = client
        self.resultDs = resultDs
        self.fSyncPs = None
        self.psKey = 'PS_PARAM='
        self.psNum = 0
        self.isGz = 0
        self.maxPsNum = 10000

    def openSyncLog(self):
        if self.fSyncPs is not None: return self.fSyncPs
        try:
            if self.syncLog[len(self.syncLog)-3:] == '.gz':
                logging.info('gunzip file %s.' % self.syncLog)
                self.isGz = 1
                os.system('gunzip %s' % self.syncLog)
                self.syncLog = self.syncLog[:len(self.syncLog)-3]
            logging.info('open file %s.' % self.syncLog)
            self.fSyncPs = open(self.syncLog, 'r')
        except IOError, e:
            logging.fatal( 'Can not open synchrous log file %s: %s' , self.syncLog, e)
            logging.fatal('exit.')
            exit()
        return self.syncLog

    def closeCaseFile(self):
        if self.fSyncPs:
            logging.info('close synchrous log file %s.' % self.syncLog)
            self.fSyncPs.close()
        if self.isGz == 1:
            logging.info('gzip file %s.' % self.syncLog)
            os.system('gzip %s' % self.syncLog)

    def getNextPs(self):
        billId = ''
        subBillId = ''
        actionId = ''
        psServiceType = ''
        psId = 0
        psParam = ''
        for line in self.fSyncPs:
            line = line.strip()
            if len(line) == 0: continue
            if not line.startswith(self.psKey):
                continue
            # logging.debug(line)
            psParam = line.split('=',1)[1]

            aPsFieldes = psParam.split(';')
            for field in aPsFieldes:
                field = field.strip()
                m = re.match(r'TRADE_ID=(.+)', field)
                if m:
                    psId = m.group(1)
                    # aPsFieldes.remove(field)
                    continue
                m = re.match(r'ACTION_ID=(\d+)', field)
                if m:
                    actionId = m.group(1)
                    # aPsFieldes.remove(field)
                    continue
                m = re.match(r'PS_SERVICE_TYPE=(.+)', field)
                if m:
                    psServiceType = m.group(1)
                    # aPsFieldes.remove(field)
                    continue
                m = re.match(r'MSISDN1?=(.+)', field)
                if m:
                    billId = m.group(1)
                    # aPsFieldes.remove(field)
                    continue
                m = re.match(r'IMSI1?=(.+)', field)
                if m:
                    subBillId = m.group(1)
                    # aPsFieldes.remove(field)
                    continue
            # psParam = ';'.join(aPsFieldes)
            self.psNum += 1
            logName = self.syncLog
            case_num = self.psNum
            case = KtCase(logName, case_num, psServiceType, actionId, psParam, billId, subBillId)
            case.psId = psId
            case.client = self.client
            case.psTable = self.resultDs
            return case
        return None

    def getNextPsGrp(self):
        caseGrp = KtCaseGrp(self.maxPsNum)
        caseGrp.psTable = self.resultDs
        caseGrp.client = self.client
        # for i in range(self.maxPsNum):
        case = self.getNextPs()
        i = 0
        while case is not None:
            caseGrp.aCase.append(case)
            i += 1
            if i > self.maxPsNum:
                return caseGrp
            case = self.getNextPs()
        if len(caseGrp.aCase) > 0:
            return caseGrp
        else:
            return None


class listDir(object):
    def __init__(self, path, client, resultOut):
        self.searchPath = path
        self.client = client
        self.resultDsName = resultOut
        self.pathKey = r'sync.+SYNC'
        self.syncFiles = []
        self.aCases = []

    def listfile(self):
        # files = os.listdir(self.searchPath)
        psCount = 0
        for file in os.listdir(self.searchPath):
            logging.info('extract from file %s', file)
            if re.match(self.pathKey, file):
                logging.info('file %s matched %s', file, self.pathKey)
                fileName = '%s/%s' % (self.searchPath, file)
                extracter = ExtractOrder(fileName, self.client, self.resultDsName)
                extracter.openSyncLog()
                case = extracter.getNextPsGrp()
                while case is not None:
                    # self.aCases.append(case)
                    psCount += case.savePs(self.resultDsName)
                    case = extracter.getNextPsGrp()
                extracter.closeCaseFile()
        logging.info('extracted %d sync case.', psCount)


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.cfgFile = '%s.cfg' % self.Name
        # self.logFile = 'psparse.log'
        self.client = None
        self.caseDs = None
        # self.resultOut = 'PS_SYNC_ORDER'
        self.resultOut = 'PS_SYNC_CASE_test'

    def checkArgv(self):
        if self.argc < 2:
            print 'need log dir.'
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        self.caseDs = sys.argv[1]
        if self.argc == 3:
            self.resultOut = sys.argv[2]
        self.logFile = '%s%s' % (self.caseDs, '.log')
        # self.outFile = '%s%s' % (self.caseDs, '.csv')

    def buildKtClient(self):
        logging.info('build kt client.')
        self.aKtClient = self.cfg.loadClient()
        for cli in self.aKtClient:
            cli.connDb()
            self.client = cli

    def usage(self):
        print "Usage: %s logdir" % self.baseName
        print "example:   %s %s" % (self.baseName,'synclog1709')
        exit(1)

    def start(self):
        self.cfg = Conf(self.cfgFile)
        self.cfg.loadLogLevel()
        self.logLevel = self.cfg.logLevel

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        self.buildKtClient()
        # 'zg.ps_provision_his_100_201710'  (self.client, self.caseDs, self.resultOut)
        lister = listDir( self.caseDs, self.client, self.resultOut)
        lister.listfile()
        self.client.closeAllCur()


# main here
if __name__ == '__main__':
    main = Main()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
