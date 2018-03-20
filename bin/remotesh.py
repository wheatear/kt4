#!/app/kt4/local/python2.7/bin/python
# -*- coding: utf-8 -*-

#!/usr/bin/env python
"""emergency provisioning ps control model"""

import sys
import os
import time
import multiprocessing
import Queue
import signal
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
# from config import *


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.aClient = []
        self.fCfg = None

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


class ReHost(object):
    def __init__(self, hostName, hostIp):
        self.hostName = hostName
        self.hostIp = hostIp
        self.dUser = {}

    def setUser(self, user, passwd, prompt):
        self.dUser[user] = (passwd, prompt)


class ReCmd(object):
    def __init__(self, user, aCmds):
        self.user = user
        self.aCmds = aCmds


class RemoteSh(multiprocessing.Process):
    def __init__(self, reCmd, reHost):
        multiprocessing.Process.__init__(self)
        self.reCmd = reCmd
        self.host = reHost

        # for hosts
        # def runClient(self,float_ip,cluster_code,user_name,user_pw):
        # for cluster
    def run(self):
        clt = pexpect.pxssh.pxssh()
        # flog = open('%s/remotesh_%s.log' % (self.cfg.logDir, self.host.hostName), 'w')
        # clt.logfile = flog
        clt.logfile = sys.stdout
        logging.info('connect to host: %s %s %s', self.host.hostName, self.host.hostIp, self.reCmd.user)
        print 'connect to host: %s %s %s' % (self.host.hostName, self.host.hostIp, self.reCmd.user)

        # plain_pw = base64.decodestring(user_pw)
        # con = clt.login(float_ip,user_name,plain_pw)
        con = clt.login(self.host.hostIp, self.reCmd.user, self.host.dUser[self.reCmd.user][0])
        logging.info('connect: %s', con)
        cmdcontinue = 0
        for cmd in self.reCmd.aCmds:
            logging.info('exec: %s', cmd)
            print 'exec: %s' % (cmd)
            cmd = cmd.replace('$USER', self.reCmd.user)
            if cmd[:2]=='su':
                suUser = cmd[3:]
                suPwd = self.host.dUser[suUser][0]
                su = self.doSu(clt, cmd, suPwd)
                if su:
                    continue
                else:
                    continue

            clt.sendline(cmd)
            if cmd[:2] == 'if':
                cmdcontinue = 1
            if cmd[0:2] == 'fi':
                cmdcontinue = 0
            if cmdcontinue == 1:
                continue
            clt.prompt()
            logging.info('exec: %s', clt.before)
        clt.logout()
        # cltcmd = '/usr/bin/ksh -c "nohup /jfdata01/kt/operation/test/python/scandir/bin/scandirclient.py %s &"' % serv_ip
        # # clt.sendline('/usr/bin/ksh -c "nohup /nms/scandir/bin/scandirclient.py &"')
        # clt.sendline(cltcmd)
        # clt.prompt()
        # self.loger.writeLog('exec: %s' % (clt.before))

    def doSu(self, clt, suCmd, pwd):
        clt.sendline(suCmd)
        i = clt.expect(['密码：',pexpect.TIMEOUT,pexpect.EOF])
        if i==0:
            clt.sendline(pwd)
            i = self.clt(["(?i)are you sure you want to continue connecting", pexpect.original_prompt,
                             "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                             pexpect.TIMEOUT])
        else:
            clt.close()
            raise pexpect.ExceptionPxssh('unexpected su response ')

        if i==1:
            pass
        else:
            clt.close()
            raise pexpect.ExceptionPxssh('unexpected login response')
        if clt.auto_prompt_reset:
            if not clt.set_unique_prompt():
                clt.close()
                raise pexpect.ExceptionPxssh('could not set shell prompt '
                                     '(received: %r, expected: %r).' % (
                                         self.before, self.PROMPT,))
        return True

class ReShFac(object):
    def __init__(self,cfg,cmdfile,dest):
        self.cfg = cfg
        self.cmdFile = cmdfile
        self.dest = dest
        self.aHosts = []
        # self.aCmds = []
        self.cmdUser = 'nms'
        self.cmdPwd = 'ailk,123'
        self.localIp = None

    def makeCmd(self):
        fCmd = open(self.cmdFile,'r')
        i = 0
        user = None
        aCmds = []
        for line in fCmd:
            line = line.strip()
            if len(line) == 0:
                continue
            i += 1
            if i == 1:
                aUser = line.split(' ')
                if len(aUser) < 2:
                    logging.warn('comd no user,exit!')
                    exit(1)
                user = aUser[1]
                continue

            if line[0] == '#':
                continue
            aCmds.append(line)
        fCmd.close()
        cmd = ReCmd(user, aCmds)
        return cmd

    def makeReSh(self, host, cmd):
        logging.info('create remote shell of %s', host.hostName)
        reSh = RemoteSh(cmd, host)
        return reSh

    def run(self):
        # self.loger.openLog('%s/CLIENTCALLER.log' % self.cfg.logDir)
        logging.info('client starter(%d) running' ,os.getpid())
        self.getLocalIp()
        self.getAllHosts()
        self.readCmd()
        self.startAll()
        # self.asyncAll()

    def makeAllHosts(self):
        # for host
        # fHosts = open(self.cfg.hostClu, 'r')
        # for cluster
        conn = sqlite3.connect('kthosts.db')
        cursor = conn.cursor()
        cursor.execute('SELECT hostname,hostip FROM kthosts')
        rows = cursor.fetchall()
        dHosts = {}
        for row in rows:
            host = ReHost(*row)
            dHosts[row[0]] = host
        # cursor.close()
        userSql = 'select hostname,user,passwd,prompt from hostuser'
        cursor.execute(userSql)
        rows = cursor.fetchall()
        for row in rows:
            hostName = row[0]
            user = row[1]
            passwd = row[2]
            prompt = row[3]
            dHosts[hostName].setUser(user, passwd, prompt)

        cursor.close()
        conn.close()
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

    # for hosts
    # def runClient(self,float_ip,cluster_code,user_name,user_pw):
    # for cluster
    def runClient(self,cluster_code, float_ip, user_name, user_pw):
        clt = pexpect.pxssh.pxssh()
        flog = open('%s/pxssh.log' % (self.cfg.logDir),'w')
        # clt.logfile = flog
        clt.logfile = sys.stdout
        logging.info('connect to host: %s %s %s' ,cluster_code,float_ip,user_name)
        print 'connect to host: %s %s %s' % (cluster_code,float_ip,user_name)

        plain_pw = base64.decodestring(user_pw)
        # con = clt.login(float_ip,user_name,plain_pw)
        con = clt.login(float_ip, user_name, 'Ngtst!234')
        logging.info('connect: %s' ,con)
        cmdcontinue = 0
        for cmd in self.aCmds:
            logging.info('exec: %s' ,cmd)
            print 'exec: %s' % ( cmd)
            cmd = cmd.replace('$USER',user_name)
            clt.sendline(cmd)
            if cmd[:2] == 'if':
                cmdcontinue = 1
            if cmd[0:2] == 'fi':
                cmdcontinue = 0
            if cmdcontinue == 1:
                continue
            clt.prompt()
            logging.info('exec: %s' ,clt.before)
        clt.logout()
        # cltcmd = '/usr/bin/ksh -c "nohup /jfdata01/kt/operation/test/python/scandir/bin/scandirclient.py %s &"' % serv_ip
        # # clt.sendline('/usr/bin/ksh -c "nohup /nms/scandir/bin/scandirclient.py &"')
        # clt.sendline(cltcmd)
        # clt.prompt()
        # self.loger.writeLog('exec: %s' % (clt.before))

    def getLocalIp(self):
        self.hostname = socket.gethostname()
        logging.info('local host: %s' ,self.hostname)
        self.localIp = socket.gethostbyname(self.hostname)
        return self.localIp


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.dParam['BILL_ID'], order.getStatus()))

    def start(self):
        scmd = self.factory.makeCmd()
        # localIp = self.factory.getLocalIp()
        dHosts = self.factory.makeAllHosts()
        localHost = socket.gethostname()

        i = 0
        aReSh = []
        for hostName in dHosts:
            if hostName == localHost:
                continue
            i += 1
            logging.debug('timeer %f host %s', time.time(), hostName)
            host = dHosts[hostName]
            reSh = self.factory.makeReSh(host, scmd)
            aReSh.append(reSh)
            reSh.start()
        logging.info('start %d remotesh.', i)

        for reSh in aReSh:
            reSh.join()
            logging.info('host %s cmd completed.', reSh.host.hostName)
        logging.info('all %d remotesh completed.', i)



# def main():
#     callName = sys.argv[0]
#     cmdfile = sys.argv[1]
#     dest = sys.argv[2]
#     cfg = Conf(callName)
#     logging.basicConfig(filename=cfg.logFile, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y%m%d%I%M%S')
#     # loger = LogWriter(cfg)
#     # logging.info('connect to oracle')
#     # cfg.getDb()
#     # baseName = os.path.basename(callName)
#     # sysName = os.path.splitext(baseName)[0]
#     # logFile = '%s/%s%s' % ('../log', sysName, '.log')
#     # cfgFile = '%s.cfg' % 'scandir'
#     # cfgFile = '%s.cfg' % 'scandir'
#     # cfg = Conf(cfgFile)
#     remoteShell = RemoteSh(cfg,cmdfile,dest)
#     # remoteShell.loger = loger
#     remoteShell.start()
#
#     remoteShell.join()
#
#     # dirManager = startManager()

class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        dirName,appName = os.path.split(self.Name)
        self.dirName = dirName
        appFull,ext = os.path.splitext(self.Name)
        self.appFull = appFull
        self.appExt = ext
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.cfgFile = '%s.cfg' % self.appFull
        self.logFile = '%s.log' % self.appFull
        self.cmdFile = None
        self.caseDs = None

    def checkArgv(self):
        if self.argc < 2:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        self.cmdFile = sys.argv[1]
        # self.caseDs = sys.argv[2]
        # self.logFile = '%s%s' % (self.caseDs, '.log')
        # self.resultOut = '%s%s' % (self.caseDs, '.rsp')

    def usage(self):
        print "Usage: %s cmdfile" % self.baseName
        print "example:   %s %s" % (self.baseName,'mkdir.sh')
        exit(1)

    def start(self):
        self.checkArgv()

        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        factory = ReShFac(self.cfg, self.cmdFile, 'ALL')
        # remoteShell.loger = loger
        director = Director(factory)
        director.start()

        # remoteShell.join()

        # factory = CentrexFac(self.cfg, self.cmdFile, self.caseDs)
        # director = Director(factory)
        # director.start()


# main here
if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
