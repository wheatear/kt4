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
import cx_Oracle as orcl
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


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.aClient = []
        self.fCfg = None
        self.dbinfo = {}

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
        if self.fCfg: return self.fCfg
        try:
            self.fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit(2)
        return self.fCfg

    def closeCfg(self):
        if self.fCfg: self.fCfg.close()

    def loadClient(self):
        # super(self.__class__, self).__init__()
        # for cli in self.aClient:
        #     cfgFile = cli.
        try:
            fCfg = open(self.cfgFile, 'r')
        except IOError, e:
            logging.fatal('can not open configue file %s', self.cfgFile)
            logging.fatal('exit.')
            exit(2)
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
                if clientSection == 1:
                    clientSection = 0
                    if client is not None: self.aClient.append(client)
                    client = None

                clientSection = 1
                client = Centrex()
                continue
            if clientSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if param[0] == 'server':
                client.serverIp = param[1]
            elif param[0] == 'sockPort':
                client.port = param[1]
            elif param[0] == 'GLOBAL_USER':
                client.user = param[1]
            elif param[0] == 'GLOBAL_PASSWD':
                client.passwd = param[1]
            elif param[0] == 'GLOBAL_RTSNAME':
                client.rtsname = param[1]
            elif param[0] == 'GLOBAL_URL':
                client.url = param[1]
        fCfg.close()
        logging.info('load %d clients.', len(self.aClient))
        return self.aClient

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

class ReHost(object):
    def __init__(self, hostName, hostIp, port, timeOut):
        self.hostName = hostName
        self.hostIp = hostIp
        self.port = port
        self.timeOut = timeOut


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
    sqlHost = "select machine_name,rpc_ip,rpc_port,time_out from rpc_register where app_name='appControl' and state=1 order by machine_name"

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
        cur = self.main.conn.cursor()
        cur.execute(self.sqlHost)
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            dHosts[row[0]] = ReHost(*row)
        # dHosts['skt2'] = ReHost('skt2', '10.4.72.67', '15200')
        # dHosts['skt3'] = ReHost('skt3', '10.4.72.68', '15200')
        # dHosts['skt4'] = ReHost('skt4', '10.4.72.69', '15200')
        # dHosts['skt5'] = ReHost('skt5', '10.4.72.70', '15200')
        # dHosts['skt6'] = ReHost('skt6', '10.4.72.71', '15200')
        # dHosts['skt7'] = ReHost('skt7', '10.4.72.72', '15200')
        # dHosts['skt8'] = ReHost('skt8', '10.4.72.73', '15200')
        # dHosts['skt9'] = ReHost('skt9', '10.4.72.74', '15200')
        # dHosts['skt10'] = ReHost('skt10', '10.4.72.75', '15200')
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


class DbConn(object):
    def __init__(self, dbInfo):
        self.dbInfo = dbInfo
        self.conn = None
        # self.connectServer()

    def connectServer(self):
        if self.conn: return self.conn
        # if self.remoteServer: return self.remoteServer
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
        self.conn = None

    def parseWorkEnv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
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

    def connectServer(self):
        if self.conn is not None: return self.conn
        self.conn = DbConn(self.cfg.dbinfo)
        self.conn.connectServer()
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

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        self.cfg = Conf(main, self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()
        self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        self.cfg.loadDbinfo()
        self.connectServer()
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
