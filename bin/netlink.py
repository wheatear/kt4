#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""sendreq.py"""
######################################################################
## Filename:      sendreq.py
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


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.fCfg = None
        self.dNet = {}

    def loadLogLevel(self):
        fCfg = self.openCfg()
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
        if self.fCfg and not self.fCfg.closed: return self.fCfg
        try:
            self.fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit(2)
        return self.fCfg

    def closeCfg(self):
        if self.fCfg: self.fCfg.close()

    def loadNet(self):
        fCfg = self.openCfg()
        netSection = 0
        net = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                if net is not None:
                    netType = net['NETTYPE']
                    if netType not in self.dNet:
                        self.dNet[netType] = [net]
                    else:
                        self.dNet[netType].append(net)
                net = None
                netSection = 0
                continue
            if line[:8] == '#NETTYPE':
                if net is not None:
                    netType = net['NETTYPE']
                    if netType not in self.dNet:
                        self.dNet[netType] = [net]
                    else:
                        self.dNet[netType].append(net)
                net = None

                netSection = 1
                net = {}
                line = line[1:]
            if netSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if len(param) > 1:
                net[param[0]] = param[1]
            else:
                net[param[0]] = None

        self.closeCfg()
        logging.info('load %d net.', len(self.dNet))
        return self.dNet

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


class RemoteHost(object):
    def __init__(self, ip, port, hostname=None):
        self.ip = ip
        self.port = port
        self.hostName = hostname
        self.addr = (ip, port)
        self.status = None

    def connect(self):
        self.status = 'connected'
        clt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            clt.connect(self.addr)
        except Exception, e:
            print('host %s:%d fail: %s' % (self.ip, self.port, e))
            self.status = 'fail: %s' % e
        clt.close()
        return self.connected

    def getStatus(self):
        msg = '%s:%d %s' % (self.ip, self.port, self.status)
        return msg


class NetLinker(object):
    def __init__(self, main, netFile):
        self.main = main
        self.netFile = netFile
        self.aHost = []
        self.aHostStatus = []
        self.fResult = None

    def linkRemoteHost(self):
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
            self.aHostStatus.append(reHost.getStatus())
            self.writeStatus(reHost)
        fNet.close()
        self.fResult.close()

    def getLinkStatus(self):
        return self.aHostStatus

    def writeStatus(self, reHost):
        if not self.fResult or self.fResult.closed:
            self.fResult = self.main.openFile(self.main.outFile)
        self.fResult.write('%s%s' % (reHost.getStatus, os.linesep))


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
        logging.info('outfile: %s', self.outFile)

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

    def makeFactory(self):
        if not self.fCmd:
            self.fCmd = self.openFile(self.cmdFile, 'r')
            logging.info('cmd file: %s', self.cmdFile)
            if not self.fCmd:
                logging.fatal('can not open command file %s. exit.', self.cmdFile)
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
        fac = FileFac(self)
        return fac

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        # self.cfg = Conf(self.cfgFile)
        # self.logLevel = self.cfg.loadLogLevel()
        logLevel = 'logging.%s' % 'DEBUG'
        self.logLevel = eval(logLevel)

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)
        print('respfile: %s' % self.outFile)

        self.cfg.loadNet()
        factory = self.makeFactory()

        director = Director(factory)
        director.start()


if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)