#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""add number area to table ps_number_area and ps_scp_area tool"""

import sys
import os
import shutil
import time
import datetime
import copy
# import multiprocessing
import Queue
import signal
import getopt
import cx_Oracle as orcl
import socket
import glob
# from multiprocessing.managers import BaseManager
# import pexpect
# import pexpect.pxssh
import base64
import logging
import re
# import sqlite3
# import threading
# from config import *

# import config.addnumberarea_cfg


class DbConn(object):
    dConn = {}

    # def __new__(cls, *args, **kw):
    #     if not hasattr(cls, '_instance'):
    #         old = super(DbConn, cls)
    #         cls._instance = old.__new__(cls, *args, **kw)
    #     return cls._instance

    def __init__(self, connId, dbInfo):
        self.connId = connId
        self.dbInfo = dbInfo
        self.dCur = {}
        self.conn = None
        if connId in DbConn.dConn:
            self.conn = DbConn.dConn[connId]
        else:
            self.connectServer()
        # self.connectServer()

    def connectServer(self):
        if self.conn: return self.conn
        connstr = '%s/%s@%s/%s' % (
        self.dbInfo['DBUSR'], self.dbInfo['DBPWD'], self.dbInfo['DBHOST'], self.dbInfo['DBSID'])
        try:
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
            DbConn.dConn[self.connId] = self.conn
        except Exception, e:
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['DBHOST'],
                          self.cfg.dbinfo['DBUSR'], self.cfg.dbinfo['DBSID'], e)
            exit()
        return self.conn

    def prepareSql(self, sql):
        logging.debug('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError, e:
            logging.error('prepare sql err: %s', sql)
            return None
        return cur

    def executemanyCur(self, cur, params):
        logging.debug('execute cur %s : %s', cur.statement, params)
        try:
            cur.executemany(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return cur

    def fetchmany(self, cur):
        logging.debug('fetch %d rows from %s', cur.arraysize, cur.statement)
        try:
            rows = cur.fetchmany()
        except orcl.DatabaseError, e:
            logging.error('fetch sql err %s:%s ', e, cur.statement)
            return None
        return rows

    def fetchone(self, cur):
        logging.debug('fethone from %s', cur.statement)
        try:
            row = cur.fetchone()
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return row

    def fetchall(self, cur):
        logging.debug('feth all from %s', cur.statement)
        try:
            rows = cur.fetchall()
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return rows

    def executeCur(self, cur, params=None):
        logging.debug('execute cur %s : %s', cur.statement, params)
        try:
            if params is None:
                cur.execute(None)
            else:
                cur.execute(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return cur

    def close(self):
        logging.info('close conn %s', self.connId)
        conn = DbConn.dConn.pop(self.connId)
        conn.close()

    def closeAll(self):
        logging.info('close all conn')
        for cId in DbConn.dConn.keys():
            logging.debug('close conn %s', cId)
            conn = DbConn.dConn.pop(cId)
            conn.close()


class NumberArea(object):
    sqlNetNumb = 'insert into ps_net_number_area'
    def __init__(self):
        self.startNumber = None
        self.endNumber = None
        self.psServiceType = 'HLR'
        self.groupCode = '0'
        self.regionCode = None
        self.status = 1
        self.doneDate = datetime.datetime.now()
        self.doneCode = 0
        self.psNetCode = None
        self.numberType = 1
        self.smpRegionCode = '000'
        self.remark = None
        self.scpSegment = None
        self.scpType = 1
        self.describe = None
        self.state = 1
        self.line = None
        self.dTable = {}
        self.makeParam()
        self.dTable = {'ps_net_number_area': self.dNetNumb,
                       'ps_scp_number_area': self.dScpSegm}
        self.dSql = {}
        self.makeInsertSql()

    def makeParam(self):
        self.dNetNumb = {
            'START_NUMBER': self.startNumber,
            'END_NUMBER': self.endNumber,
            'PS_SERVICE_TYPE': self.psServiceType,
            'GROUP_CODE': self.groupCode,
            'REGION_CODE': self.regionCode,
            'STATUS': self.status,
            'DONE_DATE': self.doneDate,
            'DONE_CODE':self.doneCode,
            'PS_NET_CODE': self.psNetCode,
            'NUMBER_TYPE': self.numberType,
            'SMP_REGION_CODE': self.smpRegionCode,
            'REMARK': self.remark,
        }
        self.dTable['ps_net_number_area'] = self.dNetNumb
        self.dScpSegm = {
            'START_NUMBER': self.startNumber,
            'END_NUMBER': self.endNumber,
            'PS_NET_CODE': self.psNetCode,
            'SCP_SEGMENT': self.scpSegment,
            'SCP_TYPE': self.scpType,
            'DESCRIBE': self.describe,
            'STATE': self.state
        }
        self.dTable['ps_scp_number_area'] = self.dScpSegm

    def makeInsertSql(self):
        for table in self.dTable:
            dTableModel = self.dTable[table]
            sqlfield = ''
            sqlValue = ''
            for field in dTableModel:
                if not sqlfield:
                    sqlfield = field
                else:
                    sqlfield = '%s,%s' % (sqlfield, field)
                if not sqlValue:
                    sqlValue = ':%s' % field
                else:
                    sqlValue = '%s,:%s' % (sqlValue, field)
            sqlInsert = 'insert into %s(%s) values(%s)' % (table, sqlfield, sqlValue)
            self.dSql[table] = sqlInsert
            # logging.info(self.dSql)

    def insertNumb(self):
        self.makeInsertSql()
        if not self.insertTable('ps_scp_number_area'):
            main.conn.conn.rollback()
            logging.error('save %d - %d failure', self.startNumber, self.endNumber)
            return False
        if not self.insertTable('ps_net_number_area'):
            main.conn.conn.rollback()
            logging.err ('save %d - %d failure', self.startNumber, self.endNumber)
            return False
        main.conn.conn.commit()
        logging.info('save %d - %d success', self.startNumber, self.endNumber)
        return True

    def insertTable(self, tableName):
        # logging.info('insert table %s', tableName)
        self.makeParam()
        sql = self.dSql[tableName]
        dParam = self.dTable[tableName]
        # logging.debug(sql)
        logging.debug(dParam)
        cur = main.conn.prepareSql(sql)
        if not main.conn.executeCur(cur, dParam):
            # cur.connection.commit()
            cur.connection.rollback()
            return False
        return True


class CsvBuilder(object):
    dNumbFields = {u'手机号段起始': 'startNumber',
                   u'手机号段终止': 'endNumber',
                   u'号段': 'numberSegment',
                   u'归属HLR/HSS': 'psNetCode',
                   u'VPMN SCP': 'scpSegment',
                   }
    sqlHss = 'select distinct ps_net_code,region_code from ps_net_number_area'
    sqlScp = 'select distinct scp_code,scp_segment from PS_SCP_SEGMENT'
    def __init__(self, file):
        self.numbFile = file
        self.outFile = main.outFile
        self.fNumb = None
        self.fOut = None
        self.dFieldIndex = {}
        self.type = None  # ordinary  virtual
        self.numbArea = None
        self.dScp = {}
        self.dHss = {}
        self.succNum = 0
        self.failNum = 0

    def openNumFile(self):
        if self.fNumb:
            return self.fNumb
        logging.info('open number band file %s', self.numbFile)
        backFile = os.path.join(main.dirBack, self.numbFile)
        workFile = os.path.join(main.dirWork, self.numbFile)
        inFile = os.path.join(main.dirIn, self.numbFile)
        if not os.path.exists(inFile):
            logging.error('no file %s', inFile)
            exit(-1)
        self.workFile = workFile
        shutil.copy(inFile, backFile)
        os.rename(inFile, workFile)
        self.fNumb = main.openFile(workFile, 'r')
        return self.fNumb

    def openOutFile(self):
        if self.fOut:
            return self.fOut
        logging.info('open result file %s', self.outFile)
        # self.outFile = os.path.join(main.dirOut,self.numbFile)
        self.fOut = main.openFile(self.outFile, 'w')
        return self.fOut

    def closeNumFile(self):
        if self.fNumb:
            logging.info('close number band file %s', self.numbFile)
            self.fNumb.close()
            os.remove(self.workFile)

    def closeOutFile(self):
        if self.fOut:
            logging.info('close result file %s', self.numbFile)
            self.fOut.write('total: success: %d;  failure: %d%s' % (self.succNum, self.failNum, os.linesep))
            self.fOut.close()

    def writeResult(self, resp='success'):
        if resp == 'success':
            self.succNum += 1
        elif resp == 'failure':
            self.failNum += 1
        else:
            logging.error('number result code error: %s', resp)
        self.fOut.write('%s,%s%s' % (self.numbArea.line, resp, os.linesep))

    def loadScp(self):
        logging.info('loading scp config')
        cur = main.conn.prepareSql(self.sqlScp)
        main.conn.executeCur(cur)
        aScp = main.conn.fetchall(cur)
        cur.close()
        for scp in aScp:
            self.dScp[scp[0]] = scp[1]
        logging.info('scp segment: %s', self.dScp)

    def loadHssRegion(self):
        logging.info('loading hss region_code config')
        cur = main.conn.prepareSql(self.sqlHss)
        main.conn.executeCur(cur)
        aHss = main.conn.fetchall(cur)
        cur.close()
        for hss in aHss:
            self.dHss[hss[0]] = hss[1]
        logging.info(self.dHss)

    def loadNumbField(self):
        self.openNumFile()
        self.openOutFile()
        fildName = self.fNumb.readline()
        self.fOut.write(fildName)
        fildName = fildName.strip()
        if len(fildName) < 1:
            self.loadNumbField()
        logging.info('number segment file fields: %s', fildName)
        # fildName = fildName.upper()
        aFildName = fildName.split(',')
        for i,field in enumerate(aFildName):
            field = field.decode('gbk')
            if field in self.dNumbFields:
                if field == u"手机号段起始":
                    self.type = 'virtual'
                elif field == u"号段":
                    self.type = 'ordinary'
                self.dFieldIndex[self.dNumbFields[field]] = i
        logging.info(self.dNumbFields)
        logging.info(self.dFieldIndex)
        if self.type == 'ordinary' and len(self.dFieldIndex) < 3:
            logging.error('no enough information in number file: %s(%s)', self.numbFile,fildName)
            self.closeOutFile()
            exit(-1)
        if self.type == 'virtual' and len(self.dFieldIndex) < 4:
            logging.error('no enough information in number file: %s(%s)', self.numbFile,fildName)
            self.closeOutFile()
            exit(-1)
        # return aFildName

    def loadNumber(self):
        numbInfo = self.fNumb.readline()
        self.numbArea = NumberArea()
        self.numbArea.line = numbInfo.rstrip()
        if not numbInfo:
            return 'end'
        numbInfo = numbInfo.strip()
        if len(numbInfo) < 1:
            self.loadNumber()
        logging.info('load number: %s', numbInfo)
        aNumbInfo = numbInfo.split(',')
        if len(aNumbInfo) < 3:
            logging.error('no enough infomation in %s', numbInfo)
            # self.writeResult('failure')
            return None
        for field in self.dFieldIndex:
            indx = self.dFieldIndex[field]
            val = aNumbInfo[indx].strip()
            if len(val) < 1:
                logging.error('%s error: %s', field, numbInfo)
                # self.writeResult('failure')
                return None
            aNumbInfo[indx] = val

        if self.type == 'ordinary':
            logging.info('loading ordinary number segment')
            indx = self.dFieldIndex['numberSegment']
            val = aNumbInfo[indx]
            if not self.parseOrdinary(val):
                logging.error('ordinary number error: %s', val)
                # self.writeResult('failure')
                self.numbArea = None
                return None
        elif self.type == 'virtual':
            logging.info('loading virtual operator number segment')
            indxStart = self.dFieldIndex['startNumber']
            valStart = aNumbInfo[indxStart]
            indxEnd = self.dFieldIndex['endNumber']
            valEnd = aNumbInfo[indxEnd]
            if not self.parseVirtual(valStart, valEnd):
                logging.error('virtual operator number error: %s - %s', valStart, valEnd)
                # self.writeResult('failure')
                self.numbArea = None
                return None

        logging.info('loading hss code')
        indx = self.dFieldIndex['psNetCode']
        val = aNumbInfo[indx]
        if not self.parseHss(val):
            logging.error('hss code error: %s', val)
            # self.writeResult('failure')
            self.numbArea = None
            return None

        logging.info('loading scp segment')
        indx = self.dFieldIndex['scpSegment']
        val = aNumbInfo[indx]
        if not self.parseScp(val):
            logging.error('scp code error: %s', val)
            # self.writeResult('failure')
            self.numbArea = None
            return None
        logging.info('number: %d %d %s %s %s', self.numbArea.startNumber, self.numbArea.endNumber, self.numbArea.psNetCode, self.numbArea.regionCode, self.numbArea.scpSegment)
        # self.writeResult('success')
        return self.numbArea

    def parseOrdinary(self, val):
        logging.debug('parsing ordinary number')
        val = val.replace(' ', '')
        aNumb = val.split('-')
        logging.debug(aNumb)
        if len(aNumb) != 2:
            logging.warn('number error: %s', val)
            return False
        for num in aNumb:
            m = re.match('\d{11}$', num)
            if m is None:
                logging.warn('number error: %s', num)
                return False
        self.numbArea.startNumber = int(aNumb[0])
        self.numbArea.endNumber = int(aNumb[1])
        return self.numbArea.startNumber

    def parseVirtual(self, valStart, valEnd):
        logging.debug('parsing virtual operator number')
        m = re.match('\d{11}$', valStart)
        if m is None:
            logging.warn('number error: %s', valStart)
            return False
        m = re.match('\d{11}$', valEnd)
        if m is None:
            logging.warn('number error: %s', valEnd)
            return False
        self.numbArea.startNumber = int(valStart)
        self.numbArea.endNumber = int(valEnd)
        return self.numbArea.startNumber

    def parseHss(self, val):
        logging.debug('parsing hss')
        hssCode = val.split()[0]
        hssNo = hssCode[-1]
        if hssNo in ('1','2','3','4'):
            hssCode = hssCode.replace('0','NSN')
        elif hssNo in ('5','6','7','8'):
            hssCode = hssCode.replace('0','HW')
        else:
            logging.error('hsscode error: %s', val)
            return False
        if hssCode in self.dHss:
            self.numbArea.psNetCode = hssCode
            self.numbArea.regionCode = self.dHss[hssCode]
        else:
            logging.error('hsscode error: %s', val)
            return False
        return self.numbArea.psNetCode

    def parseScp(self, val):
        logging.debug('parsing scp')
        if val in self.dScp:
            segment = self.dScp[val]
            self.numbArea.scpSegment = segment
        else:
            logging.error('scpcode error: %s', val)
            return False
        return self.numbArea.scpSegment

    def checkNumber(self):
        pass

    def checkScp(self):
        pass


class Director(object):
    def __init__(self, builder):
        self.builder = builder

    def start(self):
        self.builder.loadNumbField()
        self.builder.loadScp()
        self.builder.loadHssRegion()
        while True:
            numberArea = self.builder.loadNumber()
            if not numberArea:
                logging.error('load number error')
                self.builder.writeResult('failure')
                continue
            elif numberArea == 'end':
                logging.info('load number complete.')
                break
            logging.info('save number')
            if numberArea.insertNumb():
                self.builder.writeResult('success')
            else:
                self.builder.writeResult('failure')
        self.builder.closeOutFile()
        logging.info('file: %s;  success: %d;  failure: %d', self.builder.numbFile, self.builder.succNum, self.builder.failNum)
        # logging.info('%s completed.', main.appNameBody)


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.conn = None
        self.writeConn = None
        self.inFile = None
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())

    def checkArgv(self):
        self.dirBin, self.appName = os.path.split(self.Name)
        self.appNameBody, self.appNameExt = os.path.splitext(self.appName)

        if self.argc < 2:
            self.usage()
        argvs = sys.argv[1:]
        if len(argvs) > 0:
            self.inFile = os.path.basename(argvs[0])

    def parseWorkEnv(self):
        if self.dirBin=='' or self.dirBin=='.':
            self.dirBin = '.'
            self.dirApp = '..'
        else:
            dirApp, dirBinName = os.path.split(self.dirBin)
            if dirApp=='':
                self.dirApp = '.'
            else:
                self.dirApp = dirApp
        self.dirLog = os.path.join(self.dirApp, 'log')
        self.dirCfg = os.path.join(self.dirApp, 'config')
        # self.dirCfg = self.dirBin
        self.dirBack = os.path.join(self.dirApp, 'back')
        self.dirIn = os.path.join(self.dirApp, 'input')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirOut = os.path.join(self.dirApp, 'output')
        self.dirWork = os.path.join(self.dirApp, 'work')
        self.dirTpl = os.path.join(self.dirApp, 'template')

        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s.out' % (self.inFile)
        tplName = '*.tpl'
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.inFile = os.path.join(self.dirIn, self.inFile)
        self.outFile = os.path.join(self.dirOut, outName)
        # self.tplFile = os.path.join(self.dirTpl, self.cmdTpl)
        # self.logPre = os.path.join(self.dirLog, logPre)


    def usage(self):
        print "Usage: %s numberfile" % self.appName
        print('numberfile : number area file, csv file')
        print(u'file format 1(普通新号段归属HSS和SCP分配表):'.encode('gbk'))
        print(u'品牌,号段,IMSI,数量（万）,归属HLR/HSS,VPMN SCP'.encode('gbk'))
        print(u'动感地带,19800900000-19800999999,4600710090XXXXX-4600710099XXXXX,10,HSS04 FE041,SCP06'.encode('gbk'))
        print(u'file format 2(转售商新号段归属HSS和SCP分配表):'.encode('gbk'))
        print(u'归属HLR/HSS,VPMN SCP,手机号段起始,手机号段终止,对应IMSI,数量（万）,号码品牌,公司'.encode('gbk'))
        print(u'HSS04 FE041,SCP19,16500010000,16500019999,4600730001XXXXX,1,虚拟运营商,北京国美'.encode('gbk'))
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError, e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def connectServer(self):
        if self.conn is not None: return self.conn
        self.conn = DbConn('main', self.appCfg.DBINFO)
        self.conn.connectServer()
        return self.conn

    # def getConn(self, connId):
    #     conn = DbConn(connId, self.cfg.dbinfo)
    #     return conn

    def makeFactory(self):
        if self.facType == 't':
            return self.makeTableFactory()
        elif self.facType == 'f':
            return self.makeFileFactory()

    def makeTableFactory(self):
        self.netType = 'KtPs'
        self.netCode = 'kt4'
        logging.info('net type: %s  net code: %s', self.netType, self.netCode)
        fac = TableFac(self)
        return fac

    def makeFileFactory(self):
        if not self.fCmd:
            self.fCmd = self.openFile(self.tplFile, 'r')
            logging.info('cmd template file: %s', self.tplFile)
            if not self.fCmd:
                logging.fatal('can not open command file %s. exit.', self.tplFile)
                exit(2)

        for line in self.fCmd:
            if line[:8] == '#NETTYPE':
                aType = line.split()
                self.netType = aType[2]
            if line[:8] == '#NETCODE':
                aCode = line.split()
                self.netCode = aCode[2]
            if self.netType and self.netCode:
                logging.info('net type: %s  net code: %s', self.netType, self.netCode)
                break
        logging.info('net type: %s  net code: %s', self.netType, self.netCode)
        if self.netType is None:
            logging.fatal('no find net type,exit.')
            exit(3)
        # facName = '%sFac' % self.netType
        # fac_meta = getattr(self.appNameBody, facName)
        # fac = fac_meta(self)
        facName = '%sFFac' % self.netType
        fac = createInstance(self.appNameBody, facName, self)
        # fac = FileFac(self)
        return fac

    def start(self):
        self.checkArgv()
        cfgName = '%s_cfg' % self.appNameBody
        self.appCfg = __import__(cfgName)
        self.parseWorkEnv()

        # self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.appCfg.LOGLEVEL, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...' % self.appName)
        logging.info('infile: %s' % self.inFile)

        self.connectServer()
        # factory = self.makeFactory()
        # print('respfile: %s' % factory.respFullName)
        # builder = Builder(self)
        builder = CsvBuilder(self.inFile)
        director = Director(builder)
        director.start()


def createInstance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj

# main here
if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s completed.', main.appName)
