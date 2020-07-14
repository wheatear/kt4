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
        super(self.__class__, self).__init__(main)
        self.inFile = main.inFile
        # self.outFile = main.backFile
        self.lastVersion = None
        self.lastDesc = None

    def getCurVersion(self):
        self.lastVersion = self.curVersion
        self.lastDesc = self.curDesc
        super(self.__class__, self).getCurVersion()

    def backCfg(self):
        logging.info('backup cur ps cfg')
        # print('backup cur ps cfg')
        super(self.__class__, self).export()

    def truncCfg(self):
        logging.info('truncate old ps_cfg %s', self.expTalbes)
        aTables = self.expTalbes.split(',')
        sqlTpl = 'truncate table %s'
        for tab in aTables:
            sql = sqlTpl % tab
            logging.info(sql)
            print(sql)
            cur = self.conn.executeSql(sql)
            cur.close()

    def importCfg(self):
        cmd = r"imp \'%s\' file=%s full=y ignore=y" % (self.dbInfo, self.inFile)
        print(cmd)
        logging.info(cmd)
        (outStr, exitSts) = pexpect.run(cmd, timeout=300, withexitstatus=1)
        # print(exitSts)
        print(outStr)
        logging.info(outStr)

    def start(self):
        self.prepareExp()
        logging.info('before import current ps_cfg version: %s', self.curVersion)
        logging.info('desc : %s', self.curDesc)
        print('backup current ps_cfg: %s', self.outFile)
        self.backCfg()
        print('truncat old ps_cfg')
        self.truncCfg()
        print('import new ps_cfg from file: %s' % self.inFile)
        self.importCfg()
        self.prepareExp()
        logging.info('after import last ps_cfg version: %s', self.lastVersion)
        logging.info('desc : %s', self.lastDesc)
        logging.info('after import current ps_cfg version: %s', self.curVersion)
        logging.info('desc : %s', self.curDesc)
        print('after import last ps_cfg version: %s' % self.lastVersion)
        print('desc : %s' % self.lastDesc)
        print('after import current ps_cfg version: %s' % self.curVersion)
        print('desc : %s' % self.curDesc)


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.conn = None
        self.inFile = None

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
        self.dirBackup = os.path.join(self.dirApp, 'backup')

        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.hostName = socket.gethostname()
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        backFilePre = '%s_%s' % ('pscfg', self.hostName)

        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)
        self.inFile = os.path.join(self.dirInput, self.inFile)
        self.outFile = os.path.join(self.dirBackup, backFilePre)

    def checkArgv(self):
        appName = os.path.basename(self.Name)
        self.appName = appName
        if self.argc < 2:
            self.usage()
        self.inFile = sys.argv[1]
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
        print('backfile: %s' % self.outFile)
        logging.info('backfile: %s', self.outFile)

        self.cfg.loadDbinfo()
        self.connectServer()
        importer = CfgImport(main)
        importer.start()


if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.appName)
