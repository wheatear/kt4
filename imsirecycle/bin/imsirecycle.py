#!/usr/bin/env python

"""kt client"""

import sys
import os
import time
import string
import datetime
import getopt
# import random
import glob
import shutil
import re
import signal
import logging
# import cx_Oracle as orcl
from socket import *
import ConfigParser

ls = os.linesep


class CmdTemplate(object):
    def __init__(self, cmdTmpl):
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'\^<(.+?)\^>'
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)

    def setMsg(self, cmdMsg):
        self.cmdTmpl = cmdMsg
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtSyncClt(object):
    def __init__(self, netInfo, cmdfile):
        self.dNetInfo = netInfo
        self.cmdFile = cmdfile
        self.httpHead = None
        self.httpRequest = None
        # self.serverIp = None
        # self.port = None
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
        tmplMsg = 'TRADE_ID=1111111;ACTION_ID=5;DISP_GPRS=4;PS_SERVICE_TYPE=HLR;MSISDN=^<BILL_ID^>;IMSI=^<IMSI^>;'
        tmpl = CmdTemplate(tmplMsg)
        self.aCmdTemplates.append(tmpl)

    def makeCmdTempate(self):
        pass
        # for tmpl in self.aCmdTemplates:
        #     msg = tmpl.cmdTmpl.replace('^<GLOBAL_USER^>', self.user)
        #     msg = msg.replace('^<GLOBAL_PASSWD^>', self.passwd)
        #     tmpl.setMsg(msg)

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
        httpHead = httpHead.replace('^<server^>', self.dNetInfo['SERVER'])
        httpHead = httpHead.replace('^<sockPort^>', self.dNetInfo['SOCKPORT'])
        self.httpHead = httpHead

    def makeMsgHead(self):
        self.msgHead = '%08dREQ:'

    def makeMsgBody(self, order):
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
            order.aReqMsg.append(httpBody)
            # contentLength = len(httpBody)
            # httpHead = self.msgHead.replace('^<body_length^>', str(contentLength))
            # httpRequest = '%s%s' % (httpHead, httpBody)
            # logging.debug(httpRequest)
            # order.aReqMsg.append(httpRequest)

    def sendOrder(self, order):
        self.makeMsgBody(order)
        # logging.debug(order.httpRequest)
        for req in order.aReqMsg:
            self.remoteServer.send(req)
            self.recvResp(order)

    def connectServer(self):
        # if self.remoteServer: self.remoteServer.close()
        if self.remoteServer: return self.remoteServer
        self.remoteServer = TcpClt(self.dNetInfo['SERVER'], int(self.dNetInfo['SOCKPORT']))
        # self.remoteServer.connect()
        return self.remoteServer

    def recvResp(self, order):
        logging.info('receive orders response...')
        rspMsg = self.remoteServer.recv()
        logging.debug(rspMsg)
        logging.info('parse orders response...')
        resp = 0
        if rspMsg.find('ERR_CODE=9;') > -1:
            resp = 1
        else:
            # m = re.search(r'(<ns22:.+?Response.+</ns22:.+?Response>)', rspMsg)
            # if m is not None:
            #     resp['response'] = m.group(1)
            # else:
            #     resp['response'] = rspMsg
            # resp['status'] = 'fail'
            pass
        order.aResp.append(resp)
        logging.info('response %s %s' % (order.dParam['BILL_ID'], resp))
        # order.rspMsg = rspMsg

    def saveResp(self, order):
        pass

    def dealFile(self, aFileInfo):
        # self.dFiles[fi] = [fileBase,count, cmdTpl, fileWkRsp, fWkRsp, fileOutRsp]
        fWkRsp = aFileInfo[4]
        fWkRsp.close()
        fileWkRsp = aFileInfo[3]
        fileOutRsp = aFileInfo[5]
        fileBkRsp = os.path.join(self.main.dirBack, os.path.basename(fileWkRsp))
        shutil.copy(fileWkRsp, fileBkRsp)
        os.rename(fileWkRsp, fileOutRsp)
        logging.info('%s complete', aFileInfo[0])


class ReqOrder(object):
    def __init__(self):
        self.no = None
        self.dParam = {}
        self.aReqMsg = []
        self.aResp = []
        self.aParamName = []

    def setParaName(self, aParaNames):
        self.aParamName = aParaNames

    def setPara(self, paras):
        for i, pa in enumerate(paras):
            key = self.aParamName[i]
            self.dParam[key] = pa

    def getStatus(self):
        status = ''
        for resp in self.aResp:
            status = '%s[%s:%s]' % (status, resp['status'], resp['response'])
        return status


class KtSyncFac(object):
    def __init__(self, dNetTypes, inFile):
        self.dNetTypes = dNetTypes
        self.facName = 'KtSync'
        self.cmdFile = None
        self.inFile = inFile
        self.inFileFull = None
        self.orderDs = None
        # self.aClient = []
        self.dClient = {}
        # self.respName = '%s.hlr' % self.inFile
        self.resp = None
        self.fileWk = None
        self.respWk = None

    def findFile(self):
        logging.info('find files ')
        filePatt = os.path.join(main.dirIn, self.inFile)
        self.aFiles = glob.glob(filePatt)
        if len(self.aFiles) > 0:
            self.inFileFull = self.aFiles[0]
            fName = os.path.basename(self.inFileFull)
            fileWk = os.path.join(main.dirWork, fName)
            fileBack = os.path.join(main.dirBack, fName)
            shutil.copy(self.inFileFull, fileBack)
            os.rename(self.inFileFull, fileWk)
            self.fileWk = fileWk
            self.respName = '%s.hlr' % fName
            self.respWk = os.path.join(main.dirWork, self.respName)
            self.respOut = os.path.join(main.dirOut, self.respName)
            self.respBack = os.path.join(main.dirBack, self.respName)

        logging.info('process files: %s', self.fileWk)
        return self.fileWk

    def dealFile(self):
        # self.dFiles[fi] = [fileBase,count, cmdTpl, fileWkRsp, fWkRsp, fileOutRsp]
        if not self.resp.closed:
            self.resp.close()
        os.remove(self.fileWk)
        shutil.copy(self.respWk, self.respBack)
        os.rename(self.respWk, self.respOut)
        logging.info('%s complete', self.respOut)

    def openDs(self):
        if self.orderDs: return self.orderDs
        try:
            self.orderDs = open(self.fileWk, 'r')
        except IOError, e:
            print('Can not open orderDs file %s: %s' % (self.fileWk, e))
            exit(2)

    def closeDs(self):
        if not self.orderDs.closed:
            self.orderDs.close()

    def closeRsp(self):
        if not self.resp.closed:
            self.resp.close()

    def loadOrderHead(self):
        # colHead = self.orderDs.readline()
        # self.aColHead = colHead.split()
        self.aColHead = ('ICCID','IMSI','BILL_ID','EXPIRY')

    def openRsp(self):
        if self.resp: return self.resp
        try:
            self.resp = open(self.respWk, 'w')
        except IOError, e:
            print('Can not open orderDs file %s: %s' % (self.respWk, e))
            exit(2)
        return self.resp

    def loadOrder(self):
        # self.openDs()
        for line in self.orderDs:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aParams = line.split()
            order = ReqOrder()
            order.setParaName(self.aColHead)
            order.setPara(aParams)
            order.line = line
            logging.debug('load order %s', line)
            yield order
        # self.closeDs()

    def makeClient(self):
        dNets = self.dNetTypes[self.facName]
        for nt in dNets:
            clt = KtSyncClt(dNets[nt], self.cmdFile)
            self.dClient[nt] = clt
        logging.info('load %d clients.', len(self.dClient))
        for nt in self.dClient:
            clt = self.dClient[nt]
            clt.loadCmd()
            clt.makeCmdTempate()
            clt.makeMsgHead()
        return self.dClient

    def start(self):
        pass


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None
        self.netCode = 'kt4'

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.line, order.aResp[0]))

    def start(self):
        client = self.factory.makeClient()[self.netCode]
        if not self.factory.findFile():
            logging.info('no find imsi file,exit.')
            return
        self.factory.openDs()
        self.factory.loadOrderHead()
        self.fRsp = self.factory.openRsp()
        client.connectServer()
        i = 0
        # while not self.shutDown:
        for order in self.factory.loadOrder():
            if self.shutDown: break

            # order = self.factory.loadOrder()
            if order is None:
                logging.info('load all orders,')
                break
            i += 1
            logging.debug('load order %d', i)
            client.connectServer()
            client.sendOrder(order)
            # client.recvResp(order)
            # client.saveResp(order)
            # client.remoteServer.close()
            self.saveOrderRsp(order)
        self.factory.closeDs()
        self.factory.closeRsp()
        self.factory.dealFile()
        # self.fRsp.close()


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
        self.argc = len(sys.argv)
        self.cfgFile = None
        self.caseDs = None
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())

    def checkArgv(self):
        self.dirBase = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.appName = os.path.basename(self.Name)
        self.appNameBody, self.appNameExt = os.path.splitext(self.appName)

        if self.argc < 2:
            # self.usage()
            self.caseDs = 'wsdx_cycle_%s.cy4' % self.today
        else:
            self.caseDs = sys.argv[1]

    def parseWorkEnv(self):
        self.dirBin = os.path.join(self.dirBase, 'bin')
        self.dirLog = os.path.join(self.dirBase, 'log')
        # self.dirCfg = os.path.join(self.dirBase, 'config')
        self.dirCfg = self.dirBin
        self.dirBack = os.path.join(self.dirBase, 'back')
        self.dirIn = os.path.join(self.dirBase, 'input')
        # self.dirLib = os.path.join(self.dirBase, 'lib')
        self.dirOut = os.path.join(self.dirBase, 'output')
        self.dirWork = os.path.join(self.dirBase, 'work')
        self.dirTpl = os.path.join(self.dirBase, 'template')

        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.caseDs, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s.hlr' % (self.caseDs)
        tplName = '*.tpl'
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.tplFile = os.path.join(self.dirTpl, tplName)
        # self.logPre = os.path.join(self.dirLog, logPre)
        self.workFile = os.path.join(self.dirWork, outName)
        self.outFile = os.path.join(self.dirOut, outName)

    def readCfg(self):
        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(self.cfgFile)
        self.dDbInfo = {}
        self.dNetTypes = {}

        # if 'db' not in self.cfg.sections():
        #     # logging.fatal('there is no db info in confige file')
        #     exit(-1)
        # for inf in self.cfg.items('db'):
        #     self.dDbInfo[inf[0]] = inf[1]
        # # print(self.dDbInfo)

        for sec in self.cfg.sections():
            if "nettype" in self.cfg.options(sec):
                nt = self.cfg.get(sec,"nettype")
                netInfo = {}
                for ntin in self.cfg.items(sec):
                    netInfo[string.upper(ntin[0])] = ntin[1]
                if nt in self.dNetTypes:
                    self.dNetTypes[nt][sec] = netInfo
                else:
                    self.dNetTypes[nt] = {}
                    self.dNetTypes[nt][sec] = netInfo
                # if nt in self.dNetTypes:
                #     self.dNetTypes[nt].append(netInfo)
                # else:
                #     self.dNetTypes[nt] = [netInfo]

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
        self.checkArgv()
        self.parseWorkEnv()
        self.readCfg()

        self.logLevel = eval('logging.%s' % self.cfg.get("main", "loglevel"))
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...', self.appName)

        factory = KtSyncFac(self.dNetTypes, self.caseDs)
        director = Director(factory)
        director.start()


# main here
if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    # main.parseWorkEnv()
    main.start()
    logging.info('%s complete.', main.appName)
