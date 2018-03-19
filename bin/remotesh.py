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


class RemoteSh(multiprocessing.Process):
    def __init__(self,cfg,cmdfile,dest):
        multiprocessing.Process.__init__(self)
        self.cfg = cfg
        self.cmdFile = cmdfile
        self.dest = dest
        self.aHosts = []
        self.aCmds = []
        self.cmdUser = 'nms'
        self.cmdPwd = 'ailk,123'

    def readCmd(self):
        fCmd = open(self.cmdFile,'r')
        for line in fCmd:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            self.aCmds.append(line)
        fCmd.close()

    def run(self):
        # self.loger.openLog('%s/CLIENTCALLER.log' % self.cfg.logDir)
        logging.info('client starter(%d) running' ,os.getpid())
        self.getLocalIp()
        self.getAllHosts()
        self.readCmd()
        self.startAll()
        # self.asyncAll()

    def getAllHosts(self):
        # for host
        # fHosts = open(self.cfg.hostClu, 'r')
        # for cluster
        if self.dest == '-c':
            hostFile = self.cfg.cluster
        elif self.dest == '-h':
            hostFile = self.cfg.hostClu
        print hostFile
        fHosts = open(hostFile, 'r')
        for line in fHosts:
            line = line.strip()
            if len(line) == 0:
                continue
            aLine = line.split(' ')
            if self.dest == '-c':
                aInfo = aLine
            elif self.dest == '-h':
                aInfo = [aLine[1],aLine[0],aLine[2],aLine[3]]
            if aInfo[1] == self.localIp:
                continue
            self.aHosts.append(aInfo)
        fHosts.close()

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



class DirsManager(BaseManager):
    pass




def main():
    callName = sys.argv[0]
    cmdfile = sys.argv[1]
    dest = sys.argv[2]
    cfg = Conf(callName)
    logging.basicConfig(filename=cfg.logFile, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y%m%d%I%M%S')
    # loger = LogWriter(cfg)
    # logging.info('connect to oracle')
    # cfg.getDb()
    # baseName = os.path.basename(callName)
    # sysName = os.path.splitext(baseName)[0]
    # logFile = '%s/%s%s' % ('../log', sysName, '.log')
    # cfgFile = '%s.cfg' % 'scandir'
    # cfgFile = '%s.cfg' % 'scandir'
    # cfg = Conf(cfgFile)
    remoteShell = RemoteSh(cfg,cmdfile,dest)
    # remoteShell.loger = loger
    remoteShell.start()

    remoteShell.join()

    # dirManager = startManager()

if __name__ == '__main__':
    main()
