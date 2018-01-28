#!/usr/bin/env python

"""kt client"""

import sys
import os
import time
import datetime
import getopt
# import random
import re
import signal
import logging
# import cx_Oracle as orcl
from socket import *

ls = os.linesep

class HssUser(object):
    def __init__(self, msisdn, imsi, client):
        self.msisdn = msisdn
        self.imsi = imsi
        self.client = client
        self.isHssUser = 0

    def qryHss(self):
        pass


class CmdTemplate(object):
    def __init__(self, cmdTmpl):
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'\^<(.+?)\^>'
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)

    def setMsg(self, cmdMsg):
        self.cmdTmpl = cmdMsg
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtSyncClt(object):
    def __init__(self, cfg, cmdfile):
        self.cfg = cfg
        self.cmdFile = cmdfile
        self.httpHead = None
        self.httpRequest = None
        self.serverIp = None
        self.port = None
        self.url = None
        self.user = None
        self.passwd = None
        self.rtsname = None
        self.aCmdTemplates = []
        self.httpHead = None
        self.httpBody = None
        self.remoteServer = None

    def loadCmd(self):
        tmpl = None
        tmplMsg = 'TRADE_ID=11111111;ACTION_ID=1;DISP_SUB=4;PS_SERVICE_TYPE=HLR;MSISDN=\^<%s\^>;IMSI=\^<%s\^>;'
        tmpl = CmdTemplate(tmplMsg)
        self.aCmdTemplates.append(tmpl)

    def loadClient(self):
        fCfg = self.cfg.openCfg()
        clientSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line == '#kt sync server conf':
                clientSection = 1
                continue
            if clientSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if param[0] == 'server':
                self.serverIp = param[1]
            elif param[0] == 'sockPort':
                self.port = param[1]
            elif param[0] == 'GLOBAL_USER':
                self.user = param[1]
            elif param[0] == 'GLOBAL_PASSWD':
                self.passwd = param[1]
            elif param[0] == 'GLOBAL_RTSNAME':
                self.rtsname = param[1]
            elif param[0] == 'GLOBAL_URL':
                self.url = param[1]
        fCfg.close()

    def makeCmdTempate(self):
        for tmpl in self.aCmdTemplates:
            msg = tmpl.cmdTmpl.replace('^<GLOBAL_USER^>', self.user)
            msg = msg.replace('^<GLOBAL_PASSWD^>', self.passwd)
            tmpl.setMsg(msg)

    def makeHttpHead(self):
        httpHead = 'POST /imsservice/services/CentrexService HTTP/1.1\r\n'
        httpHead = '%s%s' % (httpHead, 'Accept: */*\r\n')
        httpHead = '%s%s' % (httpHead, 'Cache-Control: no-cache\r\n')
        httpHead = '%s%s' % (httpHead, 'Connection: close\r\n')
        httpHead = '%s%s' % (httpHead, 'Content-Length: ^<body_length^>\r\n')
        httpHead = '%s%s' % (httpHead, 'Content-Type: text/xml; charset=utf-8\r\n')
        httpHead = '%s%s' % (httpHead, 'Host: ^<server^>:^<sockPort^>\r\n')
        httpHead = '%s%s' % (httpHead, 'Soapaction: ""\r\n')
        httpHead = '%s%s' % (httpHead, 'User-Agent: Jakarta Commons-HttpClient/3.1\r\n\r\n')
        httpHead = httpHead.replace('^<server^>', self.serverIp)
        httpHead = httpHead.replace('^<sockPort^>', self.port)
        self.httpHead = httpHead

    def makeHttpMsg(self, order):
        for tmpl in self.aCmdTemplates:
            httpBody = tmpl.cmdTmpl
            for var in tmpl.aVariables:
                logging.debug('find var %s', var)
                logging.debug('find var %s', order.dParam.keys())
                if var not in order.dParam.keys():
                    logging.debug('dont find var %s', var)
                    logging.fatal('%s have no field %s.', order.dParam['BILL_ID'], var)
                    return -1
                paName = '^<%s^>' % var
                if httpBody.find(paName) > -1:
                    httpBody = httpBody.replace(paName, order.dParam[var])
            contentLength = len(httpBody)
            httpHead = self.httpHead.replace('^<body_length^>', str(contentLength))
            httpRequest = '%s%s' % (httpHead, httpBody)
            logging.debug(httpRequest)
            order.aReqMsg.append(httpRequest)

    def sendOrder(self, order):
        self.makeHttpMsg(order)
        # logging.debug(order.httpRequest)
        for req in order.aReqMsg:
            self.remoteServer.send(req)
            self.recvResp(order)

    def connectServer(self):
        if self.remoteServer: self.remoteServer.close()
        self.remoteServer = TcpClt(self.serverIp, int(self.port))
        # self.remoteServer.connect()
        return self.remoteServer

    def recvResp(self, order):
        logging.info('receive orders response...')
        rspMsg = self.remoteServer.recv()
        logging.debug(rspMsg)
        logging.info('parse orders response...')
        resp = {}
        if rspMsg.find('<ns22:resultcode xmlns:ns22="http://msg.centrex.imsservice.chinamobile.com"><value>0</value></ns22:resultcode>') > -1:
            resp['response'] = 'success'
            resp['status'] = 'success'
        else:
            m = re.search(r'(<ns22:.+?Response.+</ns22:.+?Response>)', rspMsg)
            if m is not None:
                resp['response'] = m.group(1)
            else:
                resp['response'] = rspMsg
            resp['status'] = 'fail'
        order.aResp.append(resp)
        logging.info('response %s %s %s' % (order.dParam['BILL_ID'], resp['status'], resp['response']))
        # order.rspMsg = rspMsg

    def saveResp(self, order):
        pass


class KtSyncFac(object):
    def __init__(self, cfg, orderDs):
        self.cfg = cfg
        # self.cmdFile = cmdFile
        self.orderDsName = orderDs
        self.orderDs = None
        self.aClient = []
        self.respName = '%s.rsp' % self.orderDsName
        self.resp = None

    def openDs(self):
        if self.orderDs: return self.orderDs
        try:
            self.orderDs = open(self.orderDsName, 'r')
        except IOError, e:
            print('Can not open orderDs file %s: %s' % (self.orderDsName, e))
            exit(2)

    def closeDs(self):
        self.orderDs.close()

    def openRsp(self):
        if self.resp: return self.resp
        try:
            self.resp = open(self.respName, 'w')
        except IOError, e:
            print('Can not open orderDs file %s: %s' % (self.respName, e))
            exit(2)
        return self.resp

    def makeOrderHead(self):
        colHead = self.orderDs.readline()
        self.aColHead = colHead.split()

    def makeTelOrder(self):
        # self.openDs()
        for line in self.orderDs:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aParams = line.split()
            order = TelOrder()
            order.setParaName(self.aColHead)
            order.setPara(aParams)
            return order
        return None
        # self.closeDs()

    def makeClient(self):
        fCfg = self.cfg.openCfg()
        clientSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                clientSection = 0
                if client is not None: self.aClient.append(client)
                client = None
                continue
            if line == '#centrex client conf':
                if clientSection == 1:
                    clientSection = 0
                    if client is not None: self.aClient.append(client)
                    client = None

                clientSection = 1
                client = CentrexClient(self.cfg, self.cmdFile)
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
        for centrex in self.aClient:
            # centrex.connectServer()
            centrex.loadCmd()
            centrex.makeCmdTempate()
            centrex.makeHttpHead()
        return self.aClient

    def start(self):
        pass


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.dParam['BILL_ID'], order.getStatus()))

    def start(self):
        client = self.factory.makeClient()[0]
        self.factory.openDs()
        self.factory.makeOrderHead()
        self.fRsp = self.factory.openRsp()
        # client.loadClient()
        # client.loadCmd()
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load order', time.time())
            order = self.factory.makeTelOrder()
            if order is None:
                logging.info('load all orders,')
                break
            i += 1
            client.connectServer()
            client.sendOrder(order)
            # client.recvResp(order)
            # client.saveResp(order)
            client.remoteServer.close()
            self.saveOrderRsp(order)
        self.factory.closeDs()
        self.fRsp.close()


class Conf(object):
    def __init__(self, cfgfile):
        self.cfgFile = cfgfile
        self.logLevel = None
        self.aClient = []

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
                # clientSection = 0
                # if client is not None: self.aClient.append(client)
                # client = None
                continue
            if line == '#provisioning client conf':
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


class TcpClt(object):
    def __init__(self, host, port, bufSize=1024):
        self.addr = (host, port)
        self.bufSize = bufSize
        try:
            tcpClt = socket(AF_INET, SOCK_STREAM)
            tcpClt.connect(self.addr)
        except Exception, e:
            print 'Can not create socket to %s:%s. %s' % (host, port, e)
            exit(-1)
        self.tcpClt = tcpClt

    def send(self, message):
        reqLen = len(message) + 12
        reqMsg = '%08d%s%s' % (reqLen, 'REQ:', message)
        logging.debug(reqMsg)
        self.tcpClt.sendall(reqMsg)

    def recv(self):
        rspHead = self.tcpClt.recv(12)
        while len(rspHead) < 12:
            rspHead = '%s%s' % (rspHead, self.tcpClt.recv(12))
        logging.debug('recv package head:%s', rspHead)
        rspLen = int(rspHead[0:8])
        rspBodyLen = rspLen - 12
        rspMsg = self.tcpClt.recv(rspBodyLen)
        logging.debug('recv: %s', rspMsg)
        rcvLen = len(rspMsg)
        while rcvLen < rspBodyLen:
            rspTailLen = rspBodyLen - rcvLen
            rspMsg = '%s%s' % (rspMsg, self.tcpClt.recv(rspTailLen))
            logging.debug(rspMsg)
            rcvLen = len(rspMsg)
        rspMsg = '%s%s' % (rspHead, rspMsg)
        logging.debug('recv: %s', rspMsg)
        return rspMsg


class QSub(object):
    def __init__(self, tcpClt, outPa=None,
                 reqTpl="TRADE_ID=11111111;ACTION_ID=1;DISP_SUB=4;PS_SERVICE_TYPE=HLR;MSISDN=%s;IMSI=%s;"):
        self.tcpClt = tcpClt
        self.reqTpl = reqTpl
        self.outPa = outPa

    def makeReqMsg(self, msisdn, imsi):
        self.msisdn = msisdn
        self.imsi = imsi
        reqMsg = self.reqTpl % (msisdn, imsi)
        return reqMsg

    def sendReq(self, msg):
        self.tcpClt.send(msg)

    def recvRsp(self):
        rspMsg = self.tcpClt.recv()
        aRspInfo = rspMsg.split(';')

        subInfo = ';'.join(aRspInfo[1:3])
        if self.outPa:
            outPara = ['MSISDN1', 'IMSI1'] + self.outPa
            for para in outPara:
                key = '%s%s' % (para, '=')
                for val in aRspInfo:
                    if key in val:
                        subInfo = '%s;%s' % (subInfo, val)
                        break
        else:
            subInfo = ';'.join(aRspInfo[1:])
        return subInfo

    def qrySub(self, msisdn, imsi):
        qryMsg = self.makeReqMsg(msisdn, imsi)
        self.sendReq(qryMsg)
        self.recvRsp()


class KtClient(object):
    def __init__(self):
        # self.cfgFile = cfgfile
        self.ktName = None
        self.ktType = None
        self.dbUser = None
        self.dbPwd = None
        self.dbHost = None
        self.dbPort = None
        self.dbSid = None
        self.orderTablePre = 'i_provision'
        self.syncServer = None
        self.sockPort = None

        self.conn = None
        self.tcpClt = None
        self.month = time.strftime("%Y%m", time.localtime())

        self.dAsyncSendCur = {}
        self.dAsyncStatusCur = {}
        self.dAsyncTaskCur = {}

    def connDb(self):
        if self.conn: return self.conn
        try:
            connstr = '%s/%s@%s/%s' % (self.dbUser, self.dbPwd, self.dbHost, self.dbSid)
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
        except Exception, e:
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.dbHost, self.dbUser, self.dbSid, e)
            exit()
        return self.conn


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
        # self.cmdFile = None
        self.caseDs = None

    def checkArgv(self):
        if self.argc < 2:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        # self.cmdFile = sys.argv[1]
        self.caseDs = sys.argv[1]
        self.logFile = '%s%s' % (self.caseDs, '.log')
        self.resultOut = '%s%s' % (self.caseDs, '.rsp')

    def buildCentrexClient(self):
        logging.info('build centrex client.')
        self.aKtClient = self.cfg.loadClient()
        for cli in self.aKtClient:
            cli.connDb()
            self.client = cli

    def usage(self):
        print "Usage: %s datafile" % self.baseName
        print "example:   %s %s" % (self.baseName,'teldata')
        exit(1)

    def start(self):
        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        factory = KtSyncFac(self.cfg, self.caseDs)
        director = Director(factory)
        director.start()


# main here
if __name__ == '__main__':
    main = Main()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
