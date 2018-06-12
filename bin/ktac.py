#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""kt appc.sh tool"""

import sys
import os
import time
import copy
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

class KtHost(object):
    def __init__(self, hostName, hostIp, port, timeOut):
        self.hostName = hostName
        self.hostIp = hostIp
        self.port = port
        self.timeOut = timeOut

    def __str__(self):
        str = '%s %s %s %s' % (self.hostName, self.hostIp, self.port, self.timeOut)
        return str


class AcCmd(object):
    def __init__(self):
        self.cmd = r'appControl -c %s:%s'
        self.prompt = r'\(ac console\)# '
        self.prcPattern = r'(( ?\d{1,2})\t(app_\w+)\|(\w+)\|(\w+))\r\n'
        self.aCmds = []
        # self.hosts = []

    def addCmd(self, cmdStr):
        self.aCmds.append(cmdStr)

    def __str__(self):
        str = '%s\n' % self.cmd
        for cmd in self.aCmds:
            str = '%s%s\n' % (str,cmd)
        return str


class AcConsole(threading.Thread):
    def __init__(self, reCmd, objType, reHost, aHostProcess, aProcess, logPre):
        threading.Thread.__init__(self)
        self.reCmd = reCmd
        self.objType = objType
        self.host = reHost
        # self.reCmd.cmd = 'appControl -c %s:%s' % (reHost.hostIp, str(reHost.port))
        self.aProcess = aProcess
        self.aHostProcess = aHostProcess
        self.dDoneProcess = {}
        self.logPre = logPre
        self.queryNum = 10

    def run(self):
        logging.info('remote shell of host %s running in pid:%d %s', self.host.hostName, os.getpid(), self.name)
        self.reCmd.cmd = 'appControl -c %s:%s' % (self.host.hostIp, str(self.host.port))
        timeOut = self.host.timeOut / 1000
        print(self.reCmd.cmd)
        appc = pexpect.spawn(self.reCmd.cmd, timeout=timeOut)
        # print(self.reCmd.cmd)
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
        prcPattern = r'(( ?\d{1,2})\t(app_\w+)\|(\w+)\|(\w+))\r\n'
        prcs = []
        dQryProcess = self.queryProcess(appc)
        self.markProcStatus(dQryProcess)
        for cmd in self.reCmd.aCmds:
            if cmd[:5] == 'query':
                continue
            logging.info('exec: %s', cmd)
            # time.sleep(1)
            # print('send cmd: %s' % cmd)
            aCmdProcess = self.makeCmdProcess(cmd)
            for cmdProc in aCmdProcess:
                print('(%s)%s' % (self.host.hostName, cmdProc))
                appc.sendline(cmdProc)
                i = appc.expect([self.reCmd.prompt, r'RESULT:FALSE:', pexpect.TIMEOUT, pexpect.EOF])
                iResend = 0
                while i == 1:
                    iResend += 1
                    if iResend > 5:
                        break
                    time.sleep(60)
                    appc.sendline(cmdProc)
                    i = appc.expect([self.reCmd.prompt, r'RESULT:FALSE:', pexpect.TIMEOUT, pexpect.EOF])
            # print('check process after %s:' % cmd)
            time.sleep(60)
            dDoneProcess = self.checkResult(cmd, appc)

            self.markProcStatus(dDoneProcess)
            # time.sleep(1)
            # logging.info('exec: %s', appc.before)
        appc.sendline('exit')
        i = appc.expect(['GoodBye\!\!', pexpect.TIMEOUT, pexpect.EOF])
        flog1.write('%s %s end%s' % (time.strftime("%Y%m%d%H%M%S", time.localtime()), self.host.hostName, os.linesep))
        flog1.close()
        flog2.close()
        # flog.write(prcs)

    def sendCmds(self, acs, aCmd):
        for cmdProc in aCmd:
            print('(%s)%s' % (self.host.hostName, cmdProc))
            acs.sendline(cmdProc)
            i = acs.expect([self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])

    def makeCmdProcess(self, cmd):
        aCmdProc = []
        if cmd == 'query':
            return [cmd]
        if self.objType == 'h':
            cmdProc = '%sall' % cmd
            aCmdProc.append(cmdProc)
        else:
            for prc in self.aProcess:
                cmdProc = '%s %s' % (cmd, prc[2])
                aCmdProc.append(cmdProc)
        return aCmdProc

    def checkResult(self, cmd, acs):
        if cmd[:5] == 'start':
            return self.checkStart(acs)
        elif cmd[:5] == 'shutd':
            return self.checkDown(acs)

    def checkStart(self, acs):
        aBaseProc = []
        if self.objType == 'h':
            aBaseProc = self.aHostProcess
        else:
            aBaseProc = self.aProcess
        dCheckProc = {}
        for i in range(self.queryNum):
            dCheckProc = self.queryProcess(acs)
            self.printDicInfo(dCheckProc)
            unRun = 0
            for proc in aBaseProc:
                prcName = proc[1]
                if prcName not in dCheckProc:
                    unRun = 1
                    break
            if unRun == 1:
                time.sleep(60)
                continue
            else:
                break
        return dCheckProc

    def checkDown(self, acs):
        dCheckProc = {}
        for i in range(self.queryNum):
            dCheckProc = self.queryProcess(acs)
            if self.objType == 'h':
                if len(dCheckProc) == 0:
                    return dCheckProc
                else:
                    time.sleep(60)
                    continue
            for proc in self.aProcess:
                prcName = proc[1]
                # prcName = prcAcName.split('|')[2]
                prcIsRun = 0
                if prcName in dCheckProc:
                    prcIsRun = 1
                    break
            if prcIsRun == 1:
                time.sleep(60)
                continue
            else:
                break
        return dCheckProc

    def queryProcess(self, acs):
        # print('query')
        print('(%s)%s' % (self.host.hostName, 'query'))
        acs.sendline('query')
        # aHostProc = self.aHostProcess
        dQryProcess = {}
        i = acs.expect([self.reCmd.prcPattern, self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])
        while i == 0:
            appPrc = acs.match.group(1)
            procIndx = acs.match.group(2)
            procApp = acs.match.group(3)
            procType = acs.match.group(4)
            procName = acs.match.group(5)
            dQryProcess[procName] = [procIndx, procApp, procType]
            # print(appPrc)
            i = acs.expect([self.reCmd.prcPattern, self.reCmd.prompt, pexpect.TIMEOUT, pexpect.EOF])
        return dQryProcess

    def markProcStatus(self, dProc):
        dMarked = {}
        if not dProc:
            for proc in self.aHostProcess:
                proc.append('0')
            return
        for proc in self.aHostProcess:
            procName = proc[1]
            if procName in dProc:
                qryProc = dProc[procName]
                if procName == proc[2]:
                    procAcName = '%s|%s|%s' % (qryProc[1], qryProc[2], procName)
                    proc[2] = procAcName
                proc.append(qryProc[0])
                dMarked[procName] = qryProc
            else:
                proc.append('0')
        for pName in dProc:
            if pName in dMarked:
                continue
            else:
                procInfo = [self.host.hostName,pName,pName,None,dProc[pName][0]]
                self.aHostProcess.append(procInfo)

    def suExit(self, clt):
        clt.sendline('exit')
        clt.prompt()

    def printDicInfo(self, dict):
        for k in dict:
            logging.info('%s %s' % (k,dict[k]))

class AcBuilder(object):
    sqlHost = "select machine_name,rpc_ip,rpc_port,time_out from rpc_register where app_name='appControl' and state=1 order by machine_name"
    sqlNet = "select process_name,ip,net_code from ps_proxy_route where state=1"
    # sqlProcess = "select process_name,ip,net_code from ps_proxy_route where state=1"
    sqlProcess = "select m.machine_name,m.process_name,m.process_name,r.net_code,m.sort from sys_machine_process m, ps_proxy_route r where m.state=1 and m.process_name=r.process_name(+) order by m.machine_name,m.sort"
    def __init__(self, main):
        self.main = main
        self.group = main.group
        self.arvHosts = main.hosts
        self.conn = self.main.conn
        self.dHosts = {}
        self.dProcess = {}
        self.dAllProcess = {}
        self.acCmd = None
        self.aAcCons = []
        # self.dest = dest

    def buildCmd(self):
        logging.info('create cmd ')
        main = self.main
        cmd = AcCmd()
        if main.cmd == 'r':
            cmd.addCmd('shutdown')
            cmd.addCmd('startup')
        elif main.cmd == 's':
            cmd.addCmd('startup')
        elif main.cmd == 'd':
            cmd.addCmd('shutdown')
        elif main.cmd == 'q':
            cmd.addCmd('query')

        # cmd.addCmd('query')
        self.acCmd = cmd
        return cmd

    def buildAcConsole(self):
        logging.info('create remote appc ')
        for hostName in self.dProcess:
            logging.info(hostName)
            aProcess = self.dProcess[hostName]
            aHostProcess = self.dAllProcess[hostName]
            # hostName = self.dProcess[process][0]
            host = self.dHosts[hostName]
            cmd = copy.deepcopy(self.acCmd)
            # cmdProc = self.makeCmdProcess(cmd, aProcess)
            logging.info(cmd)
            acCons = AcConsole(cmd, main.objType, host, aHostProcess, aProcess, self.main.logPre)
            self.aAcCons.append(acCons)
        return self.aAcCons

    def printProcess(self, dProcess):
        sProcessStatus = ''
        for host in sorted(dProcess):
            aProcess = dProcess[host]
            strHostProc = '%s process %d:' % (host, len(aProcess))
            sProcessStatus = '%s%s%s' % (sProcessStatus, os.linesep, strHostProc)
            # print(strHostProc)
            for proc in aProcess:
                n = len(proc) - 1
                str = '%s\t' * n
                procStatus = proc[4:]
                procStatus.extend(proc[1:4])
                out = str % tuple(procStatus)
                # aProcessStatus.append(out)
                sProcessStatus = '%s%s%s' % (sProcessStatus, os.linesep, out)
                # print(out)
        return sProcessStatus

    def printAllHost(self):
        for hostName in self.dHosts:
            host = self.dHosts[hostName]
            logging.info('%s %s %s %s', host.hostName, host.hostIp, host.port, host.timeOut)

    def loadProcess(self):
        # dProcess = {}
        sql = self.sqlProcess
        para = None
        # condition = ''
        # if main.objType == 'p':
        #     condition = ' and m.process_name=:PROCESS_NAME'
        #     para = {'PROCESS_NAME': main.obj}
        #     sql = '%s%s' % (self.sqlProcess, condition)
        cur = self.conn.prepareSql(sql)
        self.conn.executeCur(cur, para)
        rows = self.conn.fetchall(cur)
        cur.close()
        aNets = None
        aProcess = None
        aProcName = None
        aHosts = None
        if main.objType == 'n':
            aNets = main.obj.split(',')
        elif main.objType == 'p':
            aProcess = main.obj.split(',')
            aProcName = self.parseProcess(aProcess)
        if main.host:
            if main.host == 'a':
                aHosts = None
            else:
                aHosts = main.host.split(',')
        for row in rows:
            host = row[0]
            processName = row[2]
            acProcess = row[2]
            netName = row[3]
            procSort = row[4]
            acProcInfo = list(row)
            if aHosts:
                if host not in aHosts:
                    continue
            if host in self.dAllProcess:
                self.dAllProcess[host].append(acProcInfo)
            else:
                self.dAllProcess[host] = [acProcInfo]

            if aProcess:
                if processName not in aProcName:
                    continue
                else:
                    acProcInfo[2] = self.getProcess(processName, aProcess)
            if aNets:
                if netName not in aNets:
                    continue
                else:
                    acProcInfo[2] = '%s%s' % ('app_ne|busicomm|', processName)

            # acProcInfo = [host, row[1], acProcess, netName, procSort]
            if host in self.dProcess:
                self.dProcess[host].append(acProcInfo)
            else:
                self.dProcess[host] = [acProcInfo]
        return self.dProcess

    def parseProcess(self, acProcess):
        aProcName = []
        for proc in acProcess:
            aProc = proc.split('|')
            aProcName.append(aProc[2])
        return aProcName

    def getProcess(self, name, acProcess):
        for acPro in acProcess:
            aProc = acPro.split('|')
            if name == aProc[2]:
                return acPro
        return None

    def loadHosts(self):
        cur = self.conn.prepareSql(self.sqlHost)
        self.conn.executeCur(cur)
        rows = self.conn.fetchall(cur)
        cur.close()
        for row in rows:
            self.dHosts[row[0]] = KtHost(*row)
        return self.dHosts

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


class Director(object):
    def __init__(self, builder):
        self.builder = builder
        self.shutDown = None
        self.fRsp = None

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.dParam['BILL_ID'], order.getStatus()))

    def start(self):
        appcmd = self.builder.buildCmd()
        # localIp = self.factory.getLocalIp()
        logging.info(appcmd)
        dHosts = self.builder.loadHosts()
        dProcess = self.builder.loadProcess()
        logging.info('handling host:')
        self.builder.printAllHost()
        logging.info('all process:')
        aProcessStatus = self.builder.printProcess(self.builder.dAllProcess)
        logging.info(aProcessStatus )
        logging.info('handling process:')
        aProcessStatus = self.builder.printProcess(dProcess)
        logging.info(aProcessStatus)

        localHost = socket.gethostname()
        aAcCons = self.builder.buildAcConsole()
        # self.builder.printAllProcess()
        acNum = len(aAcCons)
        for ac in aAcCons:
            logging.info(ac.host)
            ac.start()

        for ac in aAcCons:
            ac.join()
            logging.info('host %s cmd completed.', ac.host.hostName)
        logging.info('all cmd done, process status:')
        aProcessStatus = self.builder.printProcess(self.builder.dAllProcess)
        print(aProcessStatus)
        self.printOut(aProcessStatus)
        logging.info('all %d remotesh completed.', acNum)

    def printOut(self, sMsg):
        fOut = open(self.builder.main.outFile, 'w')
        fOut.write(sMsg)
        fOut.close()


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.conn = None
        self.cmd = None
        self.objType = None
        self.host = None
        self.obj = None

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
            opts, arvs = getopt.getopt(argvs, "c:t:h:")
        except getopt.GetoptError, e:
            print 'get opt error:%s. %s' % (argvs, e)
            self.usage()
        self.cmd = 'q'
        self.objType = 'h'
        self.host = 'a'
        for opt, arg in opts:
            if opt == '-c':
                self.cmd = arg
            elif opt == '-t':
                self.objType = arg
            elif opt == '-h':
                self.host = arg
        if len(arvs) > 0:
            self.obj = arvs[0]

    def usage(self):
        print "Usage: %s [-c r/s/d/q] [-t h/n/p] [-h a/KTNEW_01,KTNEW_02]" % self.baseName
        print('-c r/s/d/q   ktac command')
        print('   r  restartup')
        print('   s  startup')
        print('   d  shutdown')
        print('   q  query(default)')
        print('-t h/n/p   command object type')
        print('   h  host(default)')
        print('   n  network element')
        print('   p  process')
        print('-h a/KTNEW_01,KTNEW_02   hosts ')
        print('   a  all hosts(default)')
        print('   KTNEW_01,KTNEW_02  some hosts')
        print "example:  %s %s" % (self.baseName,' -c r -t h -h a ')
        print "\t%s %s" % (self.baseName, ' -c r')
        print "\t%s %s" % (self.baseName, ' -c d -t n VOLTE_AS')
        print "\t%s %s" % (self.baseName, " -c s -t p 'app_dispatcher|dispatcher|dispatch_proc_A110'")
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

        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()
        # self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        self.cfg.loadDbinfo()
        self.connectServer()
        builder = AcBuilder(self)
        # remoteShell.loger = loger
        director = Director(builder)
        director.start()

        # remoteShell.join()


# main here
if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.baseName)
