#!/usr/bin/env python

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
from socket import *


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

    def loadNet(self):
        fCfg = self.openCfg()
        netSection = 0
        net = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                if net is not None:
                    netType = net['NETTYPE']
                    if len(self.dNet[netType]) == 0:
                        self.dNet[netType] = [net]
                    else:
                        self.dNet[netType].append(net)
                net = None
                netSection = 0
                continue
            if line[:7] == '#NETTYPE':
                if net is not None:
                    netType = net['NETTYPE']
                    if len(self.dNet[netType]) == 0:
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
            net[param[0]] = param[1]

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

    def connect(self):
        try:
            self.tcpClt.connect(self.addr)
        except Exception, e:
            print 'Can not create socket to %s. %s' % (self.addr, e)
            exit(-1)

    def close(self):
        self.tcpClt.close()

    def send(self, message):
        # reqLen = len(message) + 12
        # reqMsg = '%08d%s%s' % (reqLen, 'REQ:', message)
        reqMsg = message
        logging.debug(reqMsg)
        self.tcpClt.sendall(reqMsg)

    # def recv(self):
    #     rspHead = self.tcpClt.recv(12)
    #     while len(rspHead) < 12:
    #         rspHead = '%s%s' % (rspHead, self.tcpClt.recv(12))
    #     logging.debug('recv package head:%s', rspHead)
    #     rspLen = int(rspHead[0:8])
    #     rspBodyLen = rspLen - 12
    #     rspMsg = self.tcpClt.recv(rspBodyLen)
    #     logging.debug('recv: %s', rspMsg)
    #     rcvLen = len(rspMsg)
    #     while rcvLen < rspBodyLen:
    #         rspTailLen = rspBodyLen - rcvLen
    #         rspMsg = '%s%s' % (rspMsg, self.tcpClt.recv(rspTailLen))
    #         logging.debug(rspMsg)
    #         rcvLen = len(rspMsg)
    #     rspMsg = '%s%s' % (rspHead, rspMsg)
    #     logging.debug('recv: %s', rspMsg)
    #     return rspMsg

    def recv(self):
        rspMsg = self.tcpClt.recv(2000)
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

    # def connDb(self):
    #     if self.conn: return self.conn
    #     try:
    #         connstr = '%s/%s@%s/%s' % (self.dbUser, self.dbPwd, self.dbHost, self.dbSid)
    #         self.conn = orcl.Connection(connstr)
    #         # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
    #         # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
    #         # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
    #     except Exception, e:
    #         logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.dbHost, self.dbUser, self.dbSid, e)
    #         exit()
    #     return self.conn


class CmdTemplate(object):
    def __init__(self, cmdTmpl):
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'\^<(.+?)\^>'
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)

    def setMsg(self, cmdMsg):
        self.cmdTmpl = cmdMsg
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)



class CentrexClient(object):
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
        try:
            cmd = open(self.cmdFile, 'r')
        except IOError, e:
            print('Can not open cmd file %s: %s' % (self.cmdFile, e))
            exit(2)
        tmpl = None
        tmplMsg = ''
        for line in cmd:
            line = line.strip()
            if len(line) == 0:
                if len(tmplMsg) > 0:
                    logging.info(tmplMsg)
                    tmpl = CmdTemplate(tmplMsg)
                    logging.info(tmpl.aVariables)
                    self.aCmdTemplates.append(tmpl)
                    tmpl = None
                    tmplMsg = ''
                continue
            tmplMsg = '%s%s' % (tmplMsg, line)
        if len(tmplMsg) > 0:
            logging.info(tmplMsg)
            tmpl = CmdTemplate(tmplMsg)
            logging.info(tmpl.aVariables)
            self.aCmdTemplates.append(tmpl)

        logging.info('load %d cmd templates.' % len(self.aCmdTemplates))
        cmd.close()

    def loadClient(self):
        fCfg = self.cfg.openCfg()
        clientSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line == '#centrex client conf':
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
        # logging.info('load %d clients.', len(self.aClient))
        # return self.aClient

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


class FileFac(object):
    def _init__(self, main):
        self.main = main
        self.netType = main.netType
        self.orderDsName = main.caseDs
        self.orderDs = None
        self.aNetInfo = []
        self.dNetClient = {}
        self.respName = '%s.rsp' % os.path.basename(self.orderDsName)
        self.respFullName = os.path.join(self.main.dirOutput, self.respName)
        self.resp = None

    def openDs(self):
        if self.orderDs: return self.orderDs
        self.orderDs = self.main.openFile(self.orderDsName, 'r')
        if self.orderDs is None:
            logging.fatal('Can not open orderDs file %s.', self.orderDsName)
            exit(2)
        return self.orderDs

    def closeDs(self):
        if self.orderDs:
            self.orderDs.close()

    def openRsp(self):
        if self.resp: return self.resp
        self.resp = self.main.openFile(self.respFullName, 'a')
        if self.orderDs is None:
            logging.fatal('Can not open response file %s.', self.respName)
            exit(2)
        return self.resp

    def makeOrderFildName(self):
        fildName = self.orderDs.readline()
        self.aFildName = fildName.split()

    def makeOrder(self):
        for line in self.orderDs:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aParams = line.split()
            orderClassName = '%sOrder' % self.netType
            order = self.main.createInstance(self.main.appNameBody, orderClassName)
            order.setParaName(self.aFildName)
            order.setPara(aParams)
            order.net = self.dNetClient[0]
            return order
        return None

    def makeNet(self):
        cfg = self.main.cfg
        aNetInfo = cfg.dNet[self.netType]
        netClassName = '%sClient' % self.netType
        logging.info('load %d net info.', len(aNetInfo))
        for netInfo in self.aNetInfo:
            net = self.main.createInstance(self.main.appNameBody, netClassName, netInfo, self.main.fCmd)
            net.loadCmd()
            net.tmplReplaceNetInfo()
            net.makeHttpHead()
            self.dNetClient[netInfo['NetCode']] = net
        return self.dNetClient


class HttpShortClient(object):
    def __init__(self, netInfo, cmdfile):
        self.dNetInfo = netInfo
        self.fCmd = cmdfile
        self.httpHead = None
        self.httpRequest = None
        self.aCmdTemplates = []
        self.httpHead = None
        self.httpBody = None
        self.remoteServer = None

    def loadCmd(self):
        tmpl = None
        tmplMsg = ''
        for line in self.fCmd:
            line = line.strip()
            if len(line) == 0:
                if len(tmplMsg) > 0:
                    logging.info(tmplMsg)
                    tmpl = CmdTemplate(tmplMsg)
                    logging.info(tmpl.aVariables)
                    self.aCmdTemplates.append(tmpl)
                    tmpl = None
                    tmplMsg = ''
                continue
            tmplMsg = '%s%s' % (tmplMsg, line)
        if len(tmplMsg) > 0:
            logging.info(tmplMsg)
            tmpl = CmdTemplate(tmplMsg)
            logging.info(tmpl.aVariables)
            self.aCmdTemplates.append(tmpl)

        logging.info('load %d cmd templates.' % len(self.aCmdTemplates))
        self.fCmd.close()

    # def makeCmdTempate(self):
    #     for tmpl in self.aCmdTemplates:
    #         msg = tmpl.cmdTmpl.replace('^<GLOBAL_USER^>', self.user)
    #         msg = msg.replace('^<GLOBAL_PASSWD^>', self.passwd)
    #         tmpl.setMsg(msg)

    def tmplReplaceNetInfo(self):
        for var in self.dNetInfo:
            varPlace = '^<%s^>' % var
            for tmpl in self.aCmdTemplates:
                if var in tmpl.aVariables:
                    msg = tmpl.cmdTmpl.replace(varPlace, self.dNetInfo[var])
                    tmpl.setMsg(msg)

    def httpheadReplaceNetInfo(self):
        for var in self.dNetInfo:
            varPlace = '^<%s^>' % var
            if varPlace in self.httpHead:
                httpHead = httpHead.replace(varPlace, self.dNetInfo[var])
                self.httpHead = httpHead

    def makeHttpHead(self):
        httpHead = 'POST ^<GLOBAL_URL^> HTTP/1.1\r\n'
        httpHead = '%s%s' % (httpHead, 'Accept: */*\r\n')
        httpHead = '%s%s' % (httpHead, 'Cache-Control: no-cache\r\n')
        httpHead = '%s%s' % (httpHead, 'Connection: close\r\n')
        httpHead = '%s%s' % (httpHead, 'Content-Length: ^<body_length^>\r\n')
        httpHead = '%s%s' % (httpHead, 'Content-Type: text/xml; charset=utf-8\r\n')
        httpHead = '%s%s' % (httpHead, 'Host: ^<Ip^>:^<Port^>\r\n')
        httpHead = '%s%s' % (httpHead, 'Soapaction: ""\r\n')
        httpHead = '%s%s' % (httpHead, 'User-Agent: Jakarta Commons-HttpClient/3.1\r\n\r\n')
        self.httpHead = httpHead
        self.httpheadReplaceNetInfo()

    def makeHttpMsg(self, order):
        for tmpl in self.aCmdTemplates:
            httpBody = tmpl.cmdTmpl
            for var in tmpl.aVariables:
                logging.debug('find var %s', var)
                logging.debug('find var %s', order.dParam.keys())
                if var not in order.dParam:
                    logging.debug('dont find var %s', var)
                    logging.fatal('%s order have no field %s.', order.dParam['BILL_ID'], var)
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
        # if self.remoteServer: return self.remoteServer
        self.remoteServer = TcpClt(self.dNetInfo['Ip'], int(self.dNetInfo['Port']))
        return self.remoteServer

    def recvResp(self, order):
        logging.info('receive orders response...')
        rspMsg = self.remoteServer.recv()
        logging.debug(rspMsg)
        logging.info('parse orders response...')
        resp = {}
        resp['response'] = 'rspMsg'
        resp['status'] = 'UNKNOWN'
        if 'RSP_SUCCESS' not in self.dNetInfo:
            order.aResp.append(resp)
            logging.info('response %s %s %s' % (order.dParam['BILL_ID'], resp['status'], resp['response']))
            return True

        if rspMsg.find(self.dNetInfo['RSP_SUCCESS']) > -1:
            resp['response'] = 'success'
            resp['status'] = 'success'
        else:
            resp['status'] = 'fail'
        order.aResp.append(resp)
        logging.info('response %s %s %s' % (order.dParam['BILL_ID'], resp['status'], resp['response']))
        # order.rspMsg = rspMsg
        return True


class ReqOrder(object):
    def __init__(self):
        self.no = None
        self.aParamName = []
        self.dParam = {}
        self.net = None
        self.aReqMsg = []
        self.aResp = []

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


class HttpShortOrder(ReqOrder):
    pass

class CentrexFac(NetFac):
    def __init__(self, main, netType, orderDs):
        super(self.__class__, self).__init__(main, netType, orderDs)


class CentrexFac(object):
    def __init__(self, cfg, cmdFile, orderDs):
        self.cfg = cfg
        self.cmdFile = cmdFile
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


class HttpFac(object):
    def __init__(self, cfg, cmdFile, orderDs):
        self.cfg = cfg
        self.cmdFile = cmdFile
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


class HttpClient(object):
    def __init__(self, cfg, cmdfile):
        self.cfg = cfg
        self.cmdFile = cmdfile
        self.serverIp = None
        self.port = None
        self.url = None
        self.user = None
        self.passwd = None
        # self.rtsname = None
        self.aCmdTemplates = []
        self.httpHead = None
        self.httpBody = None
        self.httpRequest = None
        self.remoteServer = None

    def loadCmd(self):
        try:
            cmd = open(self.cmdFile, 'r')
        except IOError, e:
            print('Can not open cmd file %s: %s' % (self.cmdFile, e))
            exit(2)
        tmpl = None
        tmplMsg = ''
        for line in cmd:
            line = line.strip()
            if len(line) == 0:
                if len(tmplMsg) > 0:
                    logging.info(tmplMsg)
                    tmpl = CmdTemplate(tmplMsg)
                    logging.info(tmpl.aVariables)
                    self.aCmdTemplates.append(tmpl)
                    tmpl = None
                    tmplMsg = ''
                continue
            tmplMsg = '%s%s' % (tmplMsg, line)
        if len(tmplMsg) > 0:
            logging.info(tmplMsg)
            tmpl = CmdTemplate(tmplMsg)
            logging.info(tmpl.aVariables)
            self.aCmdTemplates.append(tmpl)

        logging.info('load %d cmd templates.' % len(self.aCmdTemplates))
        cmd.close()

    def loadClient(self):
        fCfg = self.cfg.openCfg()
        clientSection = 0
        client = None
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line == '#centrex client conf':
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
        # logging.info('load %d clients.', len(self.aClient))
        # return self.aClient

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


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def saveOrderRsp(self, order):
        self.fRsp.write('%s %s\r\n' % (order.dParam['BILL_ID'], order.getStatus()))

    def start(self):
        self.factory.makeNet()
        self.factory.openDs()
        self.factory.makeOrderHead()
        self.fRsp = self.factory.openRsp()
        # client.loadClient()
        # client.loadCmd()
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load order', time.time())
            order = self.factory.makeOrder()
            client = order.net
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


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.cmdFile = None
        self.caseDs = None
        self.netType = None

    def parseWorkEnv(self):
        dirBin, appName = os.path.split(self.Name)
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
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)

    def checkArgv(self):
        if self.argc < 3:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        self.cmdFile = sys.argv[1]
        self.inDs = sys.argv[2]
        self.logFile = '%s%s' % (self.caseDs, '.log')
        self.resultOut = '%s%s' % (self.caseDs, '.rsp')

    def usage(self):
        print "Usage: %s cmdfile datafile" % self.baseName
        print "example:   %s %s" % (self.baseName,'creatUser teldata')
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError, e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    def createInstance(module_name, class_name, *args, **kwargs):
        # module_meta = __import__(module_name, globals(), locals(), [class_name])
        class_meta = getattr(module_name, class_name)
        obj = class_meta(*args, **kwargs)
        return obj

    def makeFactory(self):
        if not self.fCmd:
            self.fCmd = self.openFile(self.cmdFile, 'r')
            if not self.fCmd:
                logging.fatal('can not open command file %s. exit.', self.cmdFile)
                exit(2)

        for line in self.fCmd:
            if line[:8] == '#NETTYPE':
                aType = line.split()
                self.netType = aType[1]
        if self.netType is None:
            logging.fatal('no find net type,exit.')
            exit(3)
        # facName = '%sFac' % self.netType
        # fac_meta = getattr(self.appNameBody, facName)
        # fac = fac_meta(self)
        fac = FileFac()
        return fac

    def start(self):
        self.parseWorkEnv()
        self.checkArgv()

        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()

        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)

        factory = self.makeFactory()
        director = Director(factory)
        director.start()


# main here
if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
