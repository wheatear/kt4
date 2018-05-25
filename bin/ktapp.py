#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""kt appc.sh tool"""

import sys
import os
import time
import multiprocessing
import Queue
import signal
import getopt
# import cx_Oracle as orcl
import socket
# import hostdirs
from multiprocessing.managers import BaseManager
import pexpect
import pexpect.pxssh
import base64
import logging
import re
import sqlite3
import threading
# from config import *


class ReHost(object):
    def __init__(self, hostName, hostIp, port):
        self.hostName = hostName
        self.hostIp = hostIp
        self.port = port


class ReCmd(object):
    def __init__(self):
        self.cmd = r'appControl -c %s:%s'
        self.prompt = r'\(ac console\)# '
        self.aCmds = []

    def addCmd(self, cmdStr):
        self.aCmds.append(cmdStr)


class RemoteSh(threading.Thread):
    def __init__(self, reCmd, reHost, logPre):
        threading.Thread.__init__(self)
        self.reCmd = reCmd
        self.host = reHost
        self.reCmd.cmd = 'appControl -c %s:%s' % (reHost.hostIp, str(reHost.port))
        self.logPre = logPre

    def run(self):
        logging.info('remote shell of host %s running in pid:%d %s', self.host.hostName, os.getpid(), self.name)
        appc = pexpect.spawn(self.reCmd.cmd)
        print(self.reCmd.cmd)
        flog1 = open('%s_%s.log1' % (self.logPre, self.host.hostName), 'a')
        flog2 = open('%s_%s.log2' % (self.logPre, self.host.hostName), 'a')
        flog1.write('%s %s starting%s' % (time.strftime("%Y%m%d%H%M%S", time.localtime()), self.host.hostName, os.linesep))
        flog1.flush()
        appc.logfile = flog2
        i = appc.expect([self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])
        if i > 0:
            return
        flog1.write(appc.before)
        flog1.write(appc.match.group())
        flog1.write(appc.buffer)
        logging.info('connected to host: %s %s', self.host.hostName, self.host.hostIp)

        cmdcontinue = 0
        # prcPattern = r'( ?\d)\t(app_\w+)\|(\w+)\|(\w+)\r\n'
        prcPattern = r'( ?\d{1,2})\t(app_\w+)\|(\w+)\|(\w+)\r\n'
        prcs = []
        for cmd in self.reCmd.aCmds:
            logging.info('exec: %s', cmd)
            # time.sleep(1)
            appc.sendline(cmd)
            # time.sleep(1)
            if cmd == 'query': prcs = []
            i = appc.expect([prcPattern, self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])
            flog1.write('match %d' % i)
            flog1.write(appc.before)
            if i <2:
                flog1.write(appc.match.group())
            flog1.write(appc.buffer)
            while i == 0:
                appPrc = appc.match.group()
                print(appPrc)
                prcs.append(appPrc)
                flog1.write(appPrc)
                i = appc.expect([prcPattern, self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])
            if i > 1:
                return

            # logging.info('exec: %s', appc.before)
        appc.sendline('exit')
        i = appc.expect(['GoodBye\!\!', pexpect.TIMEOUT, pexpect.EOF])
        flog1.write('%s %s end%s' % (time.strftime("%Y%m%d%H%M%S", time.localtime()), self.host.hostName, os.linesep))
        flog1.close()
        flog2.close()
        # flog.write(prcs)

    def doSu(self, clt, suCmd, pwd, auto_prompt_reset=True):
        clt.sendline(suCmd)
        i = clt.expect([u'密码：', 'Password:',pexpect.TIMEOUT,pexpect.EOF])
        if i==0 or i==1:
            clt.sendline(pwd)
            i = clt.expect(["su: 鉴定故障", r"[#$]", pexpect.TIMEOUT])
        else:
            clt.close()
            # raise pexpect.ExceptionPxssh('unexpected su response ')
            return False
        if i==1:
            pass
        else:
            clt.close()
            raise pexpect.ExceptionPxssh('unexpected login response')
        if auto_prompt_reset:
            if not clt.set_unique_prompt():
                clt.close()
                raise pexpect.ExceptionPxssh('could not set shell prompt '
                                     '(received: %r, expected: %r).' % (
                                         clt.before, clt.PROMPT,))
        return True

    def suExit(self, clt):
        clt.sendline('exit')
        clt.prompt()

class ReShFac(object):
    def __init__(self, main):
        self.main = main
        self.group = main.group
        self.hosts = main.hosts
        # self.dest = dest

    def loadCmd(self):
        logging.info('create cmd ')
        cmd = ReCmd()
        cmd.addCmd('query')
        return cmd

    def makeReSh(self, host, cmd):
        logging.info('create remote appc of %s', host.hostName)
        reSh = RemoteSh(cmd, host, self.main.logPre)
        return reSh

    def loadHosts(self):
        dHosts = {}
        dHosts['skt1'] = ReHost('skt1', '10.4.72.66', '15200')
        dHosts['skt2'] = ReHost('skt2', '10.4.72.67', '15200')
        dHosts['skt3'] = ReHost('skt3', '10.4.72.68', '15200')
        dHosts['skt4'] = ReHost('skt4', '10.4.72.69', '15200')
        dHosts['skt5'] = ReHost('skt5', '10.4.72.70', '15200')
        dHosts['skt6'] = ReHost('skt6', '10.4.72.71', '15200')
        dHosts['skt7'] = ReHost('skt7', '10.4.72.72', '15200')
        dHosts['skt8'] = ReHost('skt8', '10.4.72.73', '15200')
        dHosts['skt9'] = ReHost('skt9', '10.4.72.74', '15200')
        dHosts['skt10'] = ReHost('skt10', '10.4.72.75', '15200')
        return dHosts

    def startAll(self):
        logging.info('all host to connect: %s' , self.aHosts)
        # aHosts = self.aHosts
        # pool = multiprocessing.Pool(processes=10)
        for h in self.aHosts:
            # h.append(self.localIp)
            if h[1] == self.localIp:
                continue
            logging.info('run client %s@%s(%s)' , h[2], h[0], h[1])
            self.runClient(*h)
            # pool.apply_async(self.runClient,h)
        # pool.close()
        # pool.join()

    def getLocalIp(self):
        self.hostname = socket.gethostname()
        logging.info('local host: %s' ,self.hostname)
        self.localIp = socket.gethostbyname(self.hostname)
        return self.localIp
    def getHostIp(self):
        self.hostName = socket.gethostname()
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            self.hostIp = ip
        finally:
            s.close()
        return ip


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.dParam['BILL_ID'], order.getStatus()))

    def start(self):
        appcmd = self.factory.loadCmd()
        # localIp = self.factory.getLocalIp()
        dHosts = self.factory.loadHosts()
        localHost = socket.gethostname()

        i = 0
        aReSh = []
        for hostName in dHosts:
            # if hostName == localHost:
            #     continue
            i += 1
            logging.debug('timeer %f host %s', time.time(), hostName)
            host = dHosts[hostName]
            reSh = self.factory.makeReSh(host, appcmd)
            aReSh.append(reSh)
            reSh.start()
        logging.info('start %d remotesh.', i)

        for reSh in aReSh:
            reSh.join()
            logging.info('host %s cmd completed.', reSh.host.hostName)
        logging.info('all %d remotesh completed.', i)


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)

    def parseWorkEnv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        # print('0 bin: %s   appName: %s    name: %s' % (dirBin, appName, self.Name))
        appNameBody, appNameExt = os.path.splitext(appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt

        if dirBin=='' or dirBin=='.':
            dirBin = '.'
            dirApp = '..'
            self.dirBin = dirBin
            self.dirApp = dirApp
        else:
            dirApp, dirBinName = os.path.split(dirBin)
            if dirApp=='':
                dirApp = '.'
                self.dirBin = dirBin
                self.dirApp = dirApp
            else:
                self.dirApp = dirApp

        self.dirLog = os.path.join(self.dirApp, 'log')
        self.dirCfg = os.path.join(self.dirApp, 'config')
        self.dirTpl = os.path.join(self.dirApp, 'template')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirOut = os.path.join(self.dirApp, 'output')

        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s_%s' % (self.appNameBody, self.nowtime)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logPre)
        self.outFile = os.path.join(self.dirOut, outName)

    def checkArgv(self):
        if self.argc < 2:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]

        self.group = []
        self.hosts = []
        try:
            opts, arvs = getopt.getopt(argvs, "rsdqh:")
        except getopt.GetoptError, e:
            print 'get opt error:%s. %s' % (argvs, e)
            # self.usage()
        self.mode = 'query'
        # for opt, arg in opts:
        #     if opt == '-r':
        #         self.mode = 'restart'
        #     elif opt == '-s':
        #         self.mode = 'start'
        #     elif opt == '-d':
        #         self.mode = 'shutdown'
        #     elif opt == '-q':
        #         self.mode = 'query'

    def usage(self):
        print "Usage: %s [-r] [-s] [-d] [-q]" % self.baseName
        print "example:   %s %s" % (self.baseName,' -g ktgroup mkdir.sh')
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
        self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        factory = ReShFac(self)
        # remoteShell.loger = loger
        director = Director(factory)
        director.start()

        # remoteShell.join()


# main here
if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.baseName)
