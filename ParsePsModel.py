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
import multiprocessing
import cx_Oracle as orcl
from socket import *


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


class PsParser(object):
    def __init__(self,ktclient, pstable, resulttable):
        self.db = ktclient
        self.psTable = pstable
        self.resultTable = resulttable
        self.dPs = {}
        self.dComnPara = {}
        self.dDynamicPara = {}
        self.dEdsmpPara = {}
        self.dSyncPara = {}
        self.dPsType = {}

    def makeParaModel(self):
        self.dComnPara = {
            r'DIMPU=\d+;': 'DIMPU=@DIMPU@;',
            r'OPERATETIME=\d+;': 'OPERATETIME=@OPERATETIME@;',
            r'TARGET_1=\d+;': 'TARGET_1=@TARGET_1@;',
            r'C_NUMBER_1=\d+;': 'C_NUMBER_1=@C_NUMBER_1@;',
            r'IMSI2=\d+;': 'IMSI2=@IMSI2@;',
            r'OPRTIME=\d+;': 'OPRTIME=@OPRTIME@;',
            r'KIPARAMETER=[^;]+;': 'KIPARAMETER=@KIPARAMETER@;',
            r'KIVALUE=[^;]+;': 'KIVALUE=@KIVALUE@;',
            r'TRANSACTIONID=\d+;': 'TRANSACTIONID=@TRANSACTIONID@;',
            r'EFFTIME=\d+;': 'EFFTIME=@EFFTIME@;',
            r'EXPIRETIME=\d+;': 'EXPIRETIME=@EXPIRETIME@;',
            r'DESTUSER_ID=\d+;': 'DESTUSER_ID=@DESTUSER_ID@;',
            r'FEEUSER_ID=\d+;': 'FEEUSER_ID=@FEEUSER_ID@;',
            r'NAME=\d+;': 'NAME=@NAME@;',
            r'SHORTNUMBEROLD=\d+;': 'SHORTNUMBEROLD=@SHORTNUMBEROLD@;',
            r'SHORTNUMBER=\d+;': 'SHORTNUMBER=@SHORTNUMBER@;',
            r'MSISDN1=\d+;': 'MSISDN1=@MSISDN1@;',
            r'MSISDN2=\d+;': 'MSISDN2=@MSISDN2@;',
            r'BUSINESSGROUPID=\d+;': 'BUSINESSGROUPID=@BUSINESSGROUPID@;',
            r'BUSINESSGROUPID=\d+;': 'BUSINESSGROUPID=@BUSINESSGROUPID@;',
            r'PASSWORD=[^;]+;': 'PASSWORD=@PASSWORD@;',
            r'IMSI1=\d+;': 'IMSI1=@IMSI1@;',
            r'PASSWORD=\d+;': 'PASSWORD=@PASSWORD@;',
            r'C_NUMBER_1=tel:\d+;': 'C_NUMBER_1=tel:@C_NUMBER_1@;',
            r'CHARGEACCOUNT=\d+;': 'CHARGEACCOUNT=@CHARGEACCOUNT@;',
            r'SEQUENCE=\d+;': 'SEQUENCE=@SEQUENCE@;',
            r'ACNTSTOP=[-\d]+;': 'ACNTSTOP=@ACNTSTOP@;',
            r'CARDPIN=\d+;': 'CARDPIN=@CARDPIN@;',
            r'SEQNO=\d+;': 'SEQNO=@SEQNO@;',
            r'RELEASEUCBLACK_LIST=[\d&]+;': 'RELEASEUCBLACK_LIST=@RELEASEUCBLACK_LIST@;',
            r'NUMLIST=\d+;': 'NUMLIST=@NUMLIST@;',
            r'GRPID=\d+;': 'GRPID=@GRPID@;',
            r'OPEN_TIME=[- :\d]+;': 'OPEN_TIME=@OPEN_TIME@;',
            r'HSSPW=[^;]+;': 'HSSPW=@HSSPW@;',
            r'DIMPU=tel:\d+;': 'DIMPU=tel:@DIMPU@;',
            r'DIMPU=tel:\+\d+;': 'DIMPU=tel:+@DIMPU+@;',
            r'TARGET_1=tel:\d+;': 'TARGET_1=tel:@TARGET_1@;',
            r'TARGET_1=tel:\+\d+;': 'TARGET_1=tel:+@TARGET_1+@;',
            r'FWD_NUM=\d+;': 'FWD_NUM=@FWD_NUM@;',
            r'CFU_CFNUMBERS=\d+;': 'CFU_CFNUMBERS=@CFU_CFNUMBERS@;',
            r'USERLONGNUMBER=\d+;': 'USERLONGNUMBER=@USERLONGNUMBER@;',
            r'EXTENSIONNUMBER=\d+;': 'EXTENSIONNUMBER=@EXTENSIONNUMBER@;',
            r'USR_NAME=[^;]+;': 'USR_NAME=@USR_NAME@;',
            r'USR_PWD=\d+;': 'USR_PWD=@USR_PWD@;',
            r'PDP_ADDRESS=[\.\d]+;': 'PDP_ADDRESS=@PDP_ADDRESS@;',
            r'POS_ID=[^;]+;': 'POS_ID=@POS_ID@:',
            r'POS_PORT=[^;]+;': 'POS_PORT=@POS_PORT@;',
            r'NAME=[^;]+': 'NAME=@NAME@',
            r'DEFAULTCENBGNUMBER=\d+;': 'DEFAULTCENBGNUMBER=@DEFAULTCENBGNUMBER@;',
            r'PARA_NUM=\d+;': 'PARA_NUM=@PARA_NUM@;',
            r'ASSOCIATIONNUM_1=\d+;': 'ASSOCIATIONNUM_1=@ASSOCIATIONNUM_1@;',
            r'ASSOCIATIONNUM_2=\d+;': 'ASSOCIATIONNUM_2=@ASSOCIATIONNUM_2@;',
            r'CENASSISTNUM=\d+;': 'CENASSISTNUM=@CENASSISTNUM@;',
            r'CFB_CFNUMBERS=\d+;': 'CFB_CFNUMBERS=@CFB_CFNUMBERS@;',
            r'DEFAULTCENBGNUMBER=\d+;': 'DEFAULTCENBGNUMBER=@DEFAULTCENBGNUMBER@;',
            r'IPV4ADD=[\.\d]+;': 'IPV4ADD=@IPV4ADD@;',
        }
        self.dDynamicPara = {
            r'PARA_NAME_(\d+)=[^;]+;': 'PARA_NAME_P%s=@PARA_NAME_V%s@;',
            r'PARA_VALUE_(\d+)=[^;]+;': 'PARA_VALUE_P%s=@PARA_VALUE_V%s@;',
        }
        # r'SUBSCODE=\d+;': 'SUBSCODE=@SUBSCODE@;',
        self.dEdsmpPara = {
            r'TRADENO=\d+;': 'TRADENO=@TRADENO@;',
            r'ISSUETIME=\d+;': 'ISSUETIME=@ISSUETIME@;',
            r'PHONENO=\d+;': 'PHONENO=@PHONENO@;',
            r'EMPNAME=[^;]+;': 'EMPNAME=@EMPNAME@;',
            r'ENTCODE=\d+;': 'ENTCODE=@ENTCODE@;',
            r'ENTNAME=[^;]+;': 'ENTNAME=@ENTNAME@;',
            r'EMAIL=[^;]+;': 'EMAIL=@EMAIL@;',
            r'STARTDATE=\d+;': 'STARTDATE=@STARTDATE@;',
            r'USERNAME=[^;]+;': 'USERNAME=@USERNAME@;',
            r'CPHONE=\d+;': 'CPHONE=@CPHONE@;',
            r'ADDR=[^;]+;': 'ADDR=@ADDR@;',
            r'CPERSON=[^;]+;': 'CPERSON=@CPERSON@;',
            r'USERNUM=\d+;': 'USERNUM=@USERNUM@;',
            r'EMPNUM=\d+;': 'EMPNUM=@EMPNUM@;',
            r'LOGINCODE=[^;]+;': 'LOGINCODE=@LOGINCODE@;',
            r'BILLINGITEM=\d+;': 'BILLINGITEM=@BILLINGITEM@;',
            r'PARA_NUM=\d+;': 'PARA_NUM=@PARA_NUM@;',
        }
        self.dSyncPara = {
            r'TRADE_ID=[^;]+;': 'TRADE_ID=@TRADE_ID@;',
            r'MSISDN=\d+;': 'MSISDN=@MSISDN@;',
            r'MSISDN=\d+\r;': 'MSISDN=@MSISDN@\r;',
            r'IMSI=\d+\r;': 'IMSI=@IMSI@\r;',
            r'MSISDN1=\d+;': 'MSISDN1=@MSISDN1@;',
            r'IMSI=\d+;': 'IMSI=@IMSI@;',
            r'IMSI1=\d+;': 'IMSI1=@IMSI1@;',
            r'CALLPHONE=\d+;': 'CALLPHONE=@CALLPHONE@;',
            r'SEQUENCE=[^;]+;': 'SEQUENCE=@SEQUENCE@;',
            r'GRPID=\d+;': 'GRPID=@GRPID@;',
            r'NUMLIST=\d+;': 'NUMLIST=@NUMLIST@;',
            r'DEPT=[^;]+;': 'DEPT=@DEPT@;',
            r'CARDPIN=\d+;': 'CARDPIN=@CARDPIN@;',
            r'FLAGS=\d+;': 'FLAGS=@FLAGS@;',
            r'NOTABLE=[^;]+;': 'NOTABLE=@NOTABLE@;',
            r'MAXOUTNUM=\d+;': 'MAXOUTNUM=@MAXOUTNUM@;',
            r'NAME=\d+;': 'NAME=@NAME@;',
            r'GRPNAME=[^;]+;': 'GRPNAME=@GRPNAME@;',
            r'ISDNNO=\d+;': 'ISDNNO=@ISDNNO@;',
            r'BILL_ID=\d+;': 'BILL_ID=@BILL_ID@;',
            r'SUB_BILL_ID=\d+;': 'SUB_BILL_ID=@SUB_BILL_ID@;',
            r'KIPARAMETER=[^;]+;': 'KIPARAMETER=@KIPARAMETER@;',
            r'INFILE=[^;]+;': 'INFILE=@INFILE@;',
            r'ISDNLIST=\d+;': 'ISDNLIST=@ISDNLIST@;',
            r'NUMLIST=[^;]+;': 'NUMLIST=@NUMLIST@;',
            r'OUTLIST=\d+;': 'OUTLIST=@OUTLIST@;',
        }

    # r'PARA_NAME_\d+=[^;]+;': 'PARA_NAME_1=@PARA_NAME_1@;'
    def parseComnPara(self, psParam, psServiceType):
        # logging.debug('parse common param: %s', psParam)
        # psPa = unicode(psParam, 'gbk')
        for varKey in self.dComnPara:
            # rexp = unicode(varKey,'gbk')
            # rexp = varKey
            if re.search(varKey, psParam):
                # varValue = unicode(self.dComnPara[rexp],'gbk')
                varValue = self.dComnPara[varKey]
                psParam = re.sub(varKey, varValue, psParam)
        # psParam = psPa.encode('gbk')
        return psParam

    def setPsType(self, varKey, psServiceType):
        if psServiceType in self.dPsType[varKey]:
            self.dPsType[varKey][psServiceType] +=1
        else:
            dType = {psServiceType:1}
            self.dPsType[varKey] = dType

    def parseSyncPara(self, psParam, psServiceType):
        # logging.debug('parse common param: %s', psParam)
        # psPa = unicode(psParam, 'gbk')
        for varKey in self.dSyncPara:
            # rexp = unicode(varKey,'gbk')
            # rexp = varKey
            if re.search(varKey, psParam):
                # varValue = unicode(self.dComnPara[rexp],'gbk')
                varValue = self.dSyncPara[varKey]
                psParam = re.sub(varKey, varValue, psParam)
        # psParam = psPa.encode('gbk')
        return psParam

    def parseEdsmpPara(self, psParam, psServiceType):
        # logging.debug('parse common param: %s', psParam)
        for varKey in self.dEdsmpPara:
            if re.search(varKey, psParam):
                varValue = self.dEdsmpPara[varKey]
                psParam = re.sub(varKey, varValue, psParam)
        return psParam

    def parseDynamicPara(self, psParam, psServiceType):
        # logging.debug('parse dynamic param: %s', psParam)
        for varKey in self.dDynamicPara:
            m = re.search(varKey, psParam)
            while m:
                # logging.debug('searched:%s %s %s', varKey, psParam, m.group())
                searched = m.group()
                varValue = self.dDynamicPara[varKey] % (m.group(1), m.group(1))
                # psParam = re.sub(searched, varValue, psParam)
                psParam = psParam.replace(searched,varValue, 1)
                m = re.search(varKey, psParam)
        return psParam

    def parseVgopPara(self, psParam, psServiceType):
        p1 = self.parseComnPara(psParam)
        p2 = self.parseDynamicPara((p1))
        return p2

    def parsePs(self):
        # sql = 'select ps_service_type,action_id,ps_param,ps_id,bill_id,sub_bill_id,region_code,create_date from %s' % self.psTable
        sql = 'select ps_service_type,action_id,sync_param,ps_id,bill_id,sub_bill_id,log_name,create_date from %s' % self.psTable
        cur = self.db.conn.cursor()
        cur.execute(sql)
        cur.arraysize = 10000
        logging.debug('timeer %f fetch rows', time.time())
        rows = cur.fetchmany()
        rownum = len(rows)
        total = rownum
        logging.debug('timeer %f fetch %d rows, total %d, get %d keys', time.time(),rownum, total, len(self.dPs))
        while rownum > 0:
            for row in rows:
                psKey = ''
                actionId = row[1]
                psParam = row[2]
                psServiceType = row[0]
                # logging.debug('parse %s %s', actionId, psParam)
                if actionId == 1002:
                    psKey = 'action_id=1002;'
                elif actionId == 1001:
                    p1 = self.parseComnPara(psParam, psServiceType)
                    psKey = self.parseDynamicPara(p1, psServiceType)
                elif actionId == 3000 or actionId == 3001:
                    p1 = self.parseDynamicPara(psParam, psServiceType)
                    psKey = self.parseEdsmpPara(p1, psServiceType)
                else:
                    p1 = self.parseComnPara(psParam, psServiceType)
                    psKey = self.parseSyncPara(p1, psServiceType)

                if psKey in self.dPs:
                    self.dPs[psKey][8] += 1
                else:
                    self.dPs[psKey] = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],1]
            logging.debug('timeer %f fetch %d rows, total %d, get %d keys', time.time(), rownum, total, len(self.dPs))

            rows = cur.fetchmany()
            rownum = len(rows)
            total += rownum

        logging.debug('timeer %f fetch %d rows, total %d, get %d keys', time.time(),rownum, total, len(self.dPs))
        logging.debug('timeer %f get %d ps keys', time.time(), len(self.dPs))
        cur.close()

    def checkResultTable(self):
        haveTable = 1
        sql = 'select 1 from %s' % self.resultTable
        cur = self.db.conn.cursor()
        try:
            cur.execute(sql)
        except orcl.DatabaseError, e:
            haveTable = 0
        if haveTable == 0:
            self.createResultTable()

    def createResultTable(self):
        create_table = """ 
                      create table %s
                       (
                         ps_table        VARCHAR2(100) not null,
          ps_model          VARCHAR2(4000) not null,
          ps_model_name   VARCHAR2(2000),
          ps_service_code VARCHAR2(200),
          ps_service_type VARCHAR2(100) not null,
          action_id       NUMBER(4) not null,
          ps_param        VARCHAR2(4000) not null,
          ps_count             NUMBER(15),
          ps_id           NUMBER(15),
          bill_id         VARCHAR2(64),
          sub_bill_id    VARCHAR2(64),
          CREATE_DATE    DATE,
          REGION_CODE    VARCHAR2(6),
          notes           VARCHAR2(2000)
                       )
                                       """
        createTableSql = create_table % self.resultTable
        cur = self.db.conn.cursor()
        try:
            cur.execute(createTableSql)
        except orcl.DatabaseError, e:
            logging.error('can not prepare caseSql: %s : %s', cur.statement, e)

    def saveResult(self):
        sql = 'insert into %s(ps_table,ps_model,ps_service_type,action_id,ps_param,ps_count,ps_id,bill_id,sub_bill_id,create_date,region_code) values(:ps_table,:ps_model,:ps_service_type,:action_id,:ps_param,:ps_count,:ps_id,:bill_id,:sub_bill_id,:create_date,:region_code)' % self.resultTable
        cur = self.db.conn.cursor()
        cur.prepare(sql)
        i = 0
        total = 0
        aValues = []
        logging.debug('timeer %f save %d rows, total %d', time.time(), i, total)
        logging.debug('sql:%s', sql)
        for psKey in self.dPs:
            i += 1
            psInfo = self.dPs[psKey]
            # # for async order
            # dPsValue = {'ps_table':self.psTable, 'ps_model':psKey, 'ps_service_type':psInfo[0], 'action_id':psInfo[1], 'ps_param':psInfo[2], 'ps_id':psInfo[3], 'bill_id':psInfo[4], 'sub_bill_id':psInfo[5], 'create_date':psInfo[7], 'region_code':psInfo[6], 'ps_count':psInfo[8]}
            # for sync order
            log_name = psInfo[6]
            psInfo[6] = None
            dPsValue = {'ps_table':log_name, 'ps_model':psKey, 'ps_service_type':psInfo[0], 'action_id':psInfo[1], 'ps_param':psInfo[2], 'ps_id':psInfo[3], 'bill_id':psInfo[4], 'sub_bill_id':psInfo[5], 'create_date':psInfo[7], 'region_code':psInfo[6], 'ps_count':psInfo[8]}

            aValues.append(dPsValue)
            if i == 1000:
                try:
                    cur.executemany(None,aValues)
                    cur.connection.commit()
                except orcl.DatabaseError, e:
                    logging.fatal('Can not execute sql: %s', e)
                    logging.fatal('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
                total += i
                logging.debug('timeer %f save %d rows, total %d', time.time(), i, total)
                aValues = []
                i = 0
        try:
            cur.executemany(None, aValues)
            cur.connection.commit()
        except orcl.DatabaseError, e:
            logging.fatal('Can not execute sql: %s', e)
            logging.fatal('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
        cur.close()
        total += i
        logging.debug('timeer %f save %d rows, total %d', time.time(), i, total)
        logging.info('var model statistic:')
        for key in self.dPsType:
            for pt in self.dPsType[key]:
                logging.info('%s %s %d', key, pt, self.dPsType[key][pt])



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

        self.dAsyncSendCur = {}
        self.dAsyncStatusCur = {}
        self.dAsyncTaskCur = {}

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


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.cfgFile = '%s.cfg' % self.Name
        # self.logFile = 'psparse.log'
        self.client = None
        self.caseDs = None
        # self.resultOut = 'PS_MODEL_SUMMARY'
        self.resultOut = 'PS_SYNCMODEL_SUMMARY'

    def checkArgv(self):
        if self.argc < 2:
            print 'need pstable.'
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
        print "Usage: %s pstable" % self.baseName
        print "example:   %s %s" % (self.baseName,'zg.ps_provision_his_100_201710')
        exit(1)

    def start(self):
        self.cfg = Conf(self.cfgFile)
        self.cfg.loadLogLevel()
        self.logLevel = self.cfg.logLevel

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        self.buildKtClient()
        # 'zg.ps_provision_his_100_201710'
        parser = PsParser(self.client, self.caseDs, self.resultOut)
        parser.makeParaModel()
        parser.parsePs()
        parser.checkResultTable()
        parser.saveResult()

# main here
if __name__ == '__main__':
    main = Main()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
