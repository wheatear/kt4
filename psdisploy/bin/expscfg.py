#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""export ps config from kt db"""
######################################################################
## Filename:      distribute.py
##
## Version:       1.0
## Author:        wangxintian <wangxt5@asiainfo.com>
## Created at:
##
## Description:   export ps config from kt db
## 备注:
##
######################################################################

import sys
import os
import time
import datetime
import getopt
import re
import signal
import logging
import cx_Oracle as orcl
import socket
# import multiprocessing
import sqlite3
import pexpect


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.aClient = []
        self.fCfg = None
        self.dbinfo = {}
        self.expConf = {}

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
        return self.logLevel

    def openCfg(self):
        if self.fCfg:
            self.fCfg.seek(0)
            return self.fCfg
        try:
            self.fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit(2)
        return self.fCfg

    def closeCfg(self):
        if self.fCfg: self.fCfg.close()

    def loadEnv(self):
        # super(self.__class__, self).__init__()
        # for cli in self.aClient:
        #     cfgFile = cli.
        try:
            fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit(2)
        envSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line == '#running envirment conf':
                if clientSection == 1:
                    clientSection = 0
                    if client is not None: self.aClient.append(client)
                    client = None

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

    def loadDbinfo(self):
        rows = self.openCfg()
        dbSection = 0
        client = None
        dbRows = []
        for i, line in enumerate(rows):
            line = line.strip()
            if len(line) == 0:
                dbSection = 1
                continue
            if line == '#DBCONF':
                dbSection = 1
                continue
            if dbSection < 1:
                continue
            logging.debug(line)
            dbRows.append(i)
            param = line.split(' = ', 1)
            if len(param) > 1:
                self.dbinfo[param[0]] = param[1]
            else:
                self.dbinfo[param[0]] = None
        # self.removeUsed(dbRows)
        self.dbinfo['connstr'] = '%s/%s@%s/%s' % (self.dbinfo['dbusr'], self.dbinfo['dbpwd'], self.dbinfo['dbhost'], self.dbinfo['dbsid'])
        logging.info('load dbinfo, %s %s %s', self.dbinfo['dbusr'], self.dbinfo['dbhost'], self.dbinfo['dbsid'])
        return self.dbinfo

    def loadCfgTable(self):
        fl = self.openCfg()
        expSection = 0
        cfgTable = None
        for i, line in enumerate(fl):
            line = line.strip()
            if len(line) == 0:
                expSection = 0
                continue
            if line == '#EXPCONF':
                expSection = 1
                continue
            if expSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if len(param) > 1:
                self.expConf[param[0]] = param[1]
            else:
                self.expConf[param[0]] = None
        # self.removeUsed(dbRows)
        # logging.info('load expconf, %s %s %s', self.dbinfo['dbusr'], self.dbinfo['dbhost'], self.dbinfo['dbsid'])
        return self.expConf


class DbConn(object):
    def __init__(self, dbInfo):
        self.dbInfo = dbInfo
        self.conn = None
        # self.connectServer()

    def connectServer(self):
        if self.conn: return self.conn
        connstr = '%s/%s@%s/%s' % (self.dbInfo['dbusr'], self.dbInfo['dbpwd'], self.dbInfo['dbhost'], self.dbInfo['dbsid'])
        try:
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
        except Exception, e:
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['dbhost'], self.cfg.dbinfo['dbusr'], self.cfg.dbinfo['dbsid'], e)
            exit()
        return self.conn

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError, e:
            logging.error('prepare sql err: %s', sql)
            return None
        return cur

    def executemanyCur(self, cur, params):
        logging.info('execute cur %s : %s', cur.statement, params)
        try:
            cur.executemany(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return cur

    def executeCur(self, cur, params=None):
        logging.info('execute cur %s', cur.statement)
        try:
            if params is None:
                cur.execute(None)
            else:
                cur.execute(None, params)
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
        logging.debug('fethone from %s', cur.statement)
        try:
            rows = cur.fetchall()
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return rows


class ReHost(object):
    def __init__(self, hostName, hostIp):
        self.hostName = hostName
        self.hostIp = hostIp
        self.dUser = {}

    def setUser(self, user, passwd, prompt):
        self.dUser[user] = (passwd, prompt)


class DisCmd(object):
    def __init__(self, host, user, file, remoteDir=None):
        self.host = host
        self.user = user
        self.file = file
        self.remoteDir = remoteDir


class CfgExport(object):
    sqlCfgVer = "select version_id,pub_describe from PS_V_CFG_CUR"
    def __init__(self, main):
        self.main = main
        self.conn = main.conn
        self.file = main.outFile
        self.expTalbes = None
        self.dbInfo = None
        self.cfgVersion = None
        self.pubDesc = None

        self.remoteDir = main.remoteDir
        self.outFile = main.outFile
        # self.aHost = []
        self.dHosts = {}
        self.aHostStatus = []
        self.aDistStatus = []
        self.succ = []
        self.fail = []
        self.fResult = None

    def prepareExp(self):

        cur = self.conn.prepareSql(self.sqlCfgVer)
        self.conn.executeCur(cur, None),
        versInfo = self.conn.fetchone(cur)
        self.cfgVersion = versInfo[0]
        self.pubDesc = versInfo[1]
        self.expTalbes = self.main.cfg.loadCfgTable()['cfgtable']
        self.file = '%s_%s_%s.dmp' % (self.file, self.cfgVersion, self.main.nowtime)
        self.dbInfo = '%s/%s@%s' % (self.conn.dbInfo['dbusr'], self.conn.dbInfo['dbpwd'], self.conn.dbInfo['tns'])
        logging.info('export ps_cfg version: %s.', self.cfgVersion)
        logging.info('pub_desc: %s', self.pubDesc)
        logging.info('exp tables: %s', self.expTalbes)
        logging.info('export file: %s', self.file)
        print('export ps_cfg version: %s.' % self.cfgVersion)
        print('pub_desc: %s' % self.pubDesc)
        print('export file: %s' % self.file)
        print('exp tables: %s' % self.expTalbes)

    def export(self):
        self.prepareExp()
        cmd = 'exp %s tables=%s file=%s' % (self.dbInfo, self.expTalbes, self.file)
        # print(cmd)
        (outStr, exitSts) = pexpect.run(cmd, timeout=300, withexitstatus=1)
        # print(exitSts)
        print(outStr)
        logging.info(outStr)


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.conn = None
        self.souFile = None
        self.user = None
        self.remoteDir = None

    def parseWorkEnv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        appNameBody, appNameExt = os.path.splitext(appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt

        self.dirApp = os.path.dirname(self.dirBin)
        if self.dirBin == '' or self.dirBin == '.':
            self.dirBin = '.'
            self.dirApp = '..'
        else:
            dirApp = os.path.dirname(self.dirBin)
            if dirApp == '':
                self.dirApp = '.'
            else:
                self.dirApp = dirApp
        # print('dirApp: %s  dirBin: %s' % (self.dirApp, dirBin))
        self.dirLog = os.path.join(self.dirApp, 'log')
        self.dirCfg = os.path.join(self.dirApp, 'config')
        self.dirTpl = os.path.join(self.dirApp, 'template')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirInput = os.path.join(self.dirApp, 'input')
        self.dirOutput = os.path.join(self.dirApp, 'output')

        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.hostName = socket.gethostname()
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        outFilePre = '%s_%s' % ('pscfg', self.hostName)

        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)
        self.outFile = os.path.join(self.dirOutput, outFilePre)

    def checkArgv(self):
        appName = os.path.basename(self.Name)
        self.appName = appName
        # if self.argc < 3:
        #     self.usage()
        # self.souFile = sys.argv[1]
        # self.user = sys.argv[2]
        # if self.argc > 2:
        #     self.remoteDir = sys.argv[3]
        # else:
        #     self.remoteDir = self.souFile

    def usage(self):
        print "Usage: %s" % self.appName
        # print "example:   %s %s %s" % (self.appName,'somefiles ktrun', 'remotedir')
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
        self.conn = DbConn(self.cfg.dbinfo)
        self.conn.connectServer()
        return self.conn

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()
        # logLevel = 'logging.%s' % 'DEBUG'
        # self.logLevel = eval(logLevel)
        # self.logLevel = logging.DEBUG

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)
        print('outfile: %s' % self.outFile)
        logging.info('outfile: %s', self.outFile)

        self.cfg.loadDbinfo()
        self.connectServer()
        exporter = CfgExport(main)
        exporter.export()


if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.appName)
