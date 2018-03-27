#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""netcheck.py:  check network connection"""
######################################################################
## Filename:      netcheck.py
##
## Version:       2.1
## Author:        wangxintian <wangxt5@asiainfo.com>
## Created at:
##
## Description:
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
import socket


class RemoteHost(object):
    def __init__(self, ip, port, hostname=None):
        self.ip = ip
        self.port = int(port)
        self.hostName = hostname
        self.addr = (ip, self.port)
        self.status = None

    def connect(self):
        self.status = 'connected'
        clt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            clt.connect(self.addr)
        except Exception, e:
            print('host %s:%d fail: %s' % (self.ip, self.port, e))
            logging.warn('connect %s:%d fail: %s' , self.ip, self.port, e)
            self.status = 'fail: %s' % e
        clt.close()
        logging.info('host %s:%d  %s', self.ip, self.port, self.status)
        return self.status

    def getStatus(self):
        msg = '%s:%d %s %s' % (self.ip, self.port, self.hostName, self.status)
        return msg


class NetChecker(object):
    def __init__(self, main, netFile):
        self.main = main
        self.netFile = netFile
        self.aHost = []
        self.aCheckStatus = []
        self.fResult = None
        self.hostName = socket.gethostname()
        self.hostIp = socket.gethostbyname(self.hostName)

    def checkRemoteHost(self):
        fNet = self.main.openFile(self.netFile, 'r')
        for line in fNet:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aHostInfo = line.split()
            if len(aHostInfo) > 3:
                hPre = aHostInfo[:2]
                hPre.append(' '.join(aHostInfo[2:]))
                aHostInfo = hPre
            reHost = RemoteHost(*aHostInfo)
            self.aHost.append(reHost)
            reHost.connect()
            hostStatus = reHost.getStatus()
            checkStatus = '%s - %s' % (self.hostIp, hostStatus)
            self.aCheckStatus.append(checkStatus)
            self.writeStatus(checkStatus)
        fNet.close()
        self.fResult.close()

    def getCheckStatus(self):
        return self.aCheckStatus

    def writeStatus(self, status):
        if not self.fResult or self.fResult.closed:
            self.fResult = self.main.openFile(self.main.outFile, 'w')
        self.fResult.write('%s%s' % (status, os.linesep))


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.dsIn = None

    def parseWorkEnv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        # print('0 bin: %s   appName: %s    name: %s' % (dirBin, appName, self.Name))
        appNameBody, appNameExt = os.path.splitext(appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt

        self.dirApp = None
        if dirBin == '' or dirBin == '.':
            dirBin = '.'
            dirApp = '..'
            self.dirBin = dirBin
            self.dirApp = dirApp
        else:
            dirApp, dirBinName = os.path.split(dirBin)
            # print('dirapp: %s' % dirApp)
            if dirApp == '':
                dirApp = '.'
                self.dirBin = dirBin
                self.dirApp = dirApp
            else:
                self.dirApp = dirApp
        # print('dirApp: %s  dirBin: %s' % (self.dirApp, dirBin))
        self.dirLog = os.path.join(self.dirApp, 'log')
        self.dirCfg = os.path.join(self.dirApp, 'config')
        self.dirTpl = os.path.join(self.dirApp, 'template')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirInput = os.path.join(self.dirApp, 'input')
        self.dirOutput = os.path.join(self.dirApp, 'output')

        # self.today = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.today = time.strftime("%Y%m%d", time.localtime())
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        outFileName = '%s_%s.rsp' % (os.path.basename(self.dsIn), self.today)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)
        self.outFile = os.path.join(self.dirOutput, outFileName)


    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.appName = appName
        if self.argc < 2:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        self.dsIn = sys.argv[1]

    def usage(self):
        print "Usage: %s networkfile" % self.appName
        print "example:   %s %s" % (self.appName,'networkfile')
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError, e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        # self.cfg = Conf(self.cfgFile)
        # self.logLevel = self.cfg.loadLogLevel()
        # logLevel = 'logging.%s' % 'DEBUG'
        # self.logLevel = eval(logLevel)
        self.logLevel = logging.DEBUG
        # self.logLevel = logging.INFO
        # print('loglevel: %d  logFile: %s' % (self.logLevel, self.logFile))
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)
        print('respfile: %s' % self.outFile)
        logging.info('outfile: %s', self.outFile)

        checker = NetChecker(self, self.dsIn)
        checker.checkRemoteHost()




if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)