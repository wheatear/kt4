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
import expscfg


class CfgImport(expscfg.CfgExport):
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

    def start(self):
        self.backCfg()
        self.truncCfg()
        self.importCfg()


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.conn = None
        self.souFile = None

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
        self.conn = expscfg.DbConn(self.cfg.dbinfo)
        self.conn.connectServer()
        return self.conn

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        self.cfg = expscfg.Conf(self.cfgFile)
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
        importer = CfgImport(main)
        importer.start()


if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.appName)
