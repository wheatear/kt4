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
import multiprocessing
import sqlite3
import pexpect


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


class Distribute(object):
    def __init__(self, main):
        self.main = main
        self.dbFile = main.dbFile
        self.file = main.souFile
        self.user = main.user
        self.remoteDir = main.remoteDir
        self.outFile = main.outFile
        # self.aHost = []
        self.dHosts = {}
        self.aHostStatus = []
        self.aDistStatus = []
        self.succ = []
        self.fail = []
        self.fResult = None

    def makeAllHosts(self):
        conn = sqlite3.connect(self.dbFile)
        cursor = conn.cursor()
        cursor.execute('SELECT hostname,hostip FROM kthosts')
        rows = cursor.fetchall()
        for row in rows:
            host = ReHost(*row)
            self.dHosts[row[0]] = host
        # cursor.close()
        userSql = 'select hostname,user,passwd,prompt from hostuser'
        cursor.execute(userSql)
        rows = cursor.fetchall()
        for row in rows:
            hostName = row[0]
            user = row[1]
            passwd = row[2]
            prompt = row[3]
            self.dHosts[hostName].setUser(user, passwd, prompt)
        cursor.close()
        conn.close()
        return self.dHosts

    def spreadFile(self):
        self.makeAllHosts()
        # pool = multiprocessing.Pool(self.processes)
        # self.aHostStatus = pool.map(self.checkRemoteHost, self.aHost)
        # pool.close()
        # pool.join()
        for hostName in self.dHosts:
            host = self.dHosts[hostName]
            status = self.scpFile(host)
            result = None
            if status:
                result = '%s %s %s succ' % (hostName, self.file, self.remoteDir)
                self.succ.append(result)
            else:
                result = '%s %s %s fail' % (hostName, self.file, self.remoteDir)
                self.fail.append(result)
            self.aHostStatus.append(result)
        # self.getCheckStatus()
        self.writeStatus()

    def writeStatus(self):
        if not self.fResult or self.fResult.closed:
            self.fResult = self.main.openFile(self.main.outFile, 'a')
        for status in self.aHostStatus:
            self.fResult.write('%s%s' % (status, os.linesep))
        self.fResult.close()

    def scpFile(self, host):
        # lfile, ruser, rpasswd, rhost, rpath
        cmdUser = self.user
        cmdPwd = host.dUser[cmdUser][0]
        cmd = 'scp -pr %s %s@%s:%s' % (self.file,cmdUser,host.hostIp,self.remoteDir)
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
            fileBase = os.path.basename(self.file)
            index = pscp.expect(['assword:', pexpect.EOF])
            i = 0
            if index == 0:
                logging.warn('passward error for hotst: %s',host.hostIp)
                return False
            elif index == 1:
                return self.parseResp(pscp)
        except (pexpect.TIMEOUT,pexpect.EOF),e:
            logging.error(pscp.buffer)
            logging.info('scp file %s:%s failed. %s' ,host.hostIp,self.file,e)
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

    def getLocalIp(self):
        self.hostname = socket.gethostname()
        logging.info('local host: %s' ,self.hostname)
        self.localIp = socket.gethostbyname(self.hostname)
        return self.localIp


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.db = 'kthosts.db'
        self.souFile = None
        self.user = None
        self.remoteDir = None

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        appNameBody, appNameExt = os.path.splitext(self.appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt
        if self.argc < 3:
            self.usage()
        self.souFile = sys.argv[1]
        self.user = sys.argv[2]
        if self.argc > 2:
            self.remoteDir = sys.argv[3]
        else:
            self.remoteDir = self.souFile

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
        self.dbFile = os.path.join(self.dirBin, self.db)

    def usage(self):
        print "Usage: %s somefiles user remotedir" % self.appName
        print "example:   %s %s %s" % (self.appName,'somefiles ktrun', 'remotedir')
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

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)
        print('outfile: %s' % self.outFile)
        logging.info('outfile: %s', self.outFile)

        distributer = Distribute(main)
        distributer.spreadFile()


if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)