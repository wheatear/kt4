#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""distribute files or dirs to all hostes of a system"""
######################################################################
## Filename:      distribute.py
##
## Version:       1.0
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


class Distribute(object):
    def __init__(self, main):
        self.main = main
        self.aHost = []

    def spreadFile(self,remotedir,files):
        sucfile = []
        failfile = []
        # logging.info('put file %s to %s', files ,self.aHost)
        for h in self.aHost:
            if h[0] == self.localIp:
                continue
            logging.info('put to host: %s',h)
            logging.info('put files: %s', files)
            putfile = self.putFile(files, h[2], h[3], h[0], remotedir)
            if putfile:
                sucfile.append((h[0], files))
                logging.debug('success spread to %s file: %s', h[0], files)
            else:
                failfile.append((h[0], files))
                logging.debug('fail spread to %s file: %s', h[0], files)
        logging.info('success spread %d files: %s', len(sucfile), sucfile)
        logging.info('fail spread %d files: %s', len(failfile), failfile)

    def putFile(self,lfile,ruser,rpasswd,rhost,rpath):
        cmdUser = 'nms'
        cmdPwd = 'ailk,123'
        cmd = 'scp -pr %s %s@%s:%s' % (lfile,cmdUser,rhost,rpath)
        logging.info( cmd)
        try:
            pscp = pexpect.spawn(cmd)
            # flog = open('../log/spreadlog.log', 'w')
            pscp.logfile = sys.stdout
            index = pscp.expect(['assword:', pexpect.EOF])
            if index == 0:
                # pscp.sendline(base64.decodestring(rpasswd))
                pscp.sendline(cmdPwd)
            elif index == 1:
                return self.parseResp(pscp)
            # pscp.sendline('Tst,1234')
            fileBase = os.path.basename(lfile)
            index = pscp.expect(['assword:', pexpect.EOF])
            i = 0
            if index == 0:
                logging.warn('passward error for hotst: %s',rhost)
                return False
            elif index == 1:
                return self.parseResp(pscp)

            # elif index == 1:
            #     logging.info('put file %s to %s error:%s',lfile,rhost,pscp.match.group())
            #     return False
            # elif index == 2:
            #     logging.info('put file %s to %s error:%s',lfile,rhost,pscp.match.group())
            #     return False
        except (pexpect.TIMEOUT,pexpect.EOF),e:
            logging.info(pscp.buffer)
            logging.info('scp file %s:%s failed. %s' ,rhost,lfile,e)
            return False
        return True

    def parseResp(self,pscp):
        i = 0
        resp = pscp.before
        reg = re.compile(' \r\n\w(.+)')
        errres = re.findall(reg, resp)
        errnum = len(errres)
        if errnum > 0:
            for erf in errres:
                logging.info(erf)
            logging.info('%d files error', errnum)
            resp = re.sub(reg, '', resp)
        fileret = pscp.before.split('\r\n\r')
        for frt in fileret:
            frt = frt.strip()
            if frt == '':
                continue
            i += 1
            ret = frt.split('\r')
            if len(ret) > 1:
                logging.info(ret[1])
            else:
                logging.info(ret[0])
        logging.info('%d files ok', i)
        # logging.info(pscp.match.group())
        return True

    def getAllHost(self):
        try:
            fCfg = open(self.cfgfile,'r')
        except Exception,e:
            logging.error('can not open host cfg file %s: %s' ,self.cfgfile,e)
            exit(-1)
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aHostInfo = line.split(' ')
            self.aHost.append(aHostInfo)
        fCfg.close()

    def getLocalIp(self):
        self.hostname = socket.gethostname()
        logging.info('local host: %s' ,self.hostname)
        self.localIp = socket.gethostbyname(self.hostname)
        return self.localIp


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.dsFile = None
        self.remoteDir = None

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
        self.hostName = socket.gethostname()
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        outFileName = '%s_%s_%s.rsp' % (os.path.basename(self.appNameBody), self.hostName, self.today)

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
        self.dsFile = sys.argv[1]
        if self.argc == 3:
            self.remoteDir = sys.argc[2]

    def usage(self):
        print "Usage: %s somefiles remotedir" % self.appName
        print "example:   %s %s %s" % (self.appName,'somefiles', 'remotedir')
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
        print('outfile: %s' % self.outFile)

        self.cfg.loadNet()
        factory = self.makeFactory()

        director = Director(factory)
        director.start()


if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)