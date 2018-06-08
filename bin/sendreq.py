#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
import copy
import time
import datetime
import getopt
import re
import signal
import logging
from socket import *
import cx_Oracle as orcl

class Conf(object):
    def __init__(self, main, cfgfile):
        self.main = main
        self.cfgFile = cfgfile
        self.logLevel = None
        # self.fCfg = None
        self.dNet = {}
        self.rows = []
        self.dbinfo = {}

    def loadLogLevel(self):
        rows = self.openCfg()
        loglevelRow = []
        for i, line in enumerate(rows):
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            if line[:8] == 'LOGLEVEL':
                param = line.split(' = ', 1)
                logLevel = 'logging.%s' % param[1]
                self.logLevel = eval(logLevel)
                loglevelRow.append(i)
                break
        self.removeUsed(loglevelRow)
        return self.logLevel

    def removeUsed(self, lines):
        for line in lines:
            self.rows.pop(line)

    def openCfg(self):
        if len(self.rows) > 0 : return self.rows
        fCfg = self.main.openFile(self.cfgFile, 'r')
        self.rows = fCfg.readlines()
        fCfg.close()
        return self.rows

    # def closeCfg(self):
    #     if self.fCfg: self.fCfg.close()

    def loadNet(self):
        rows = self.openCfg()
        netSection = 0
        net = None
        netRows = []
        for i, line in enumerate(rows):
            line = line.strip()
            if len(line) == 0:
                if net is not None:
                    netType = net['NETTYPE']
                    if netType not in self.dNet:
                        self.dNet[netType] = [net]
                    else:
                        self.dNet[netType].append(net)
                net = None
                netSection = 0
                continue
            if line[:8] == '#NETTYPE':
                if net is not None:
                    netType = net['NETTYPE']
                    if netType not in self.dNet:
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
            netRows.append(i)
            param = line.split(' = ', 1)
            if len(param) > 1:
                net[param[0]] = param[1]
            else:
                net[param[0]] = None
        # self.removeUsed(netRows)
        logging.info('load %d net.', len(self.dNet))
        return self.dNet

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


class HttpShortClient(object):
    def __init__(self, netInfo):
        self.dNetInfo = netInfo
        # self.fCmd = cmdfile
        self.httpHead = None
        self.httpRequest = None
        self.aCmdTemplates = []
        self.httpHead = None
        self.httpBody = None
        self.remoteServer = None

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
                httpHead = self.httpHead.replace(varPlace, self.dNetInfo[var])
                self.httpHead = httpHead

    def prepareTmpl(self):
        self.tmplReplaceNetInfo()
        self.makeHttpHead()

    def makeHttpHead(self):
        httpHead = 'POST ^<GLOBAL.URL^> HTTP/1.1\r\n'
        # httpHead = '%s%s' % (httpHead, 'Accept: */*\r\n')
        # httpHead = '%s%s' % (httpHead, 'Cache-Control: no-cache\r\n')
        # httpHead = '%s%s' % (httpHead, 'Connection: close\r\n')
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
        self.connectServer()
        # logging.debug(order.httpRequest)
        for req in order.aReqMsg:
            logging.debug('send:%s', req)
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
        resp['response'] = rspMsg.replace('\r\n','\\r\\n')
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


class CmdTemplate(object):
    def __init__(self, cmdTmpl):
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'\^<(.+?)\^>'
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)

    def setMsg(self, cmdMsg):
        self.cmdTmpl = cmdMsg
        self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtPsTmpl(CmdTemplate):
    def __init__(self, cmdTmpl):
        # super(self.__class__, self).__init__(cmdTmpl)
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'@(.+?)@'

    def setMsg(self, tmpl):
        pass
        # self.cmdTmpl = tmpl
        # for field in tmpl:
        #     self.aVariables = re.findall(self.varExpt, self.cmdTmpl)


class KtPsOrder(ReqOrder):
    def __init__(self):
        super(self.__class__, self).__init__()
        self.aWaitPs = []
        self.dWaitNo = {}

    def getStatus(self):
        return self.aResp


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


class KtPsClient(HttpShortClient):
    dSql = {}
    dSql['OrderId'] = 'select SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL FROM (select 1 from all_objects where rownum <= :PSNUM)'
    dSql['RegionCode'] = "select region_code,ps_net_code from ps_net_number_area t where :BILL_ID between start_number and end_number"
    dSql['SendPs'] = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES,notes) values(:PS_ID,0,:DONE_CODE,0,80,:PS_SERVICE_TYPE,:BILL_ID,:SUB_BILL_ID,sysdate,:CREATE_DATE,sysdate,:ACTION_ID,:PS_PARAM,0,530,:REGION_CODE,100,0,5,:NOTES)'
    dSql['RecvPs'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:PS_ID order by end_date desc'
    dSql['AsyncStatus'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s_%s where create_date>=:firstDate and create_date<=:lastDate'
    dCur = {}

    def __init__(self, netInfo):
        self.dNetInfo = netInfo
        self.orderTablePre = 'i_provision'
        self.conn = None

    def connectServer(self):
        if self.conn is not None: return self.conn
        self.conn = DbConn(self.dNetInfo)
        self.conn.connectServer()
        return self.conn

    def getCurbyName(self, curName):
        if curName in self.dCur: return self.dCur[curName]
        if (curName[:6] != 'SendPs') and (curName not in self.dSql):
            return None
        sql = ''
        if curName[:6] == 'SendPs':
            namePre = curName[:6]
            regionCode = curName[6:]
            sql = self.dSql[namePre] % (self.orderTablePre, regionCode)
        else:
            sql = self.dSql[curName]
        cur = self.conn.prepareSql(sql)
        self.dCur[curName] = cur
        return cur

    def prepareTmpl(self):
        return True

    def setOrderCmd(self, order):
        order.aReqMsg = copy.deepcopy(self.aCmdTemplates)
        dtNow = datetime.datetime.now()
        for cmd in order.aReqMsg:
            for field in cmd.cmdTmpl:
                if field in order.dParam:
                    cmd.cmdTmpl[field] = order.dParam[field]
            cmd.cmdTmpl['CREATE_DATE'] = dtNow
            # cmd['tableMonth'] = dtNow.strftime('%Y%m')
            psParam = cmd.cmdTmpl['PS_PARAM']
            for para in order.dParam:
                pattern = r'[;^]%s=(.*?);' % para
                m = re.search(pattern, psParam)
                if m is not None:
                    rpl = ';%s=%s;' % (para, order.dParam[para])
                    cmd.cmdTmpl['PS_PARAM'] = psParam.replace(m.group(), rpl)

    def getOrderId(self, order):
        # sql = self.__class__.dSql['OrderId']
        cur = self.getCurbyName('OrderId')
        dPara = {'PSNUM':len(order.aReqMsg)}
        num = len(order.aReqMsg)
        self.conn.executeCur(cur, dPara)
        rows = self.conn.fetchall(cur)
        aWaitPs = [None] * len(order.aReqMsg)
        dWaitNo = {}
        for i,cmd in enumerate(order.aReqMsg):
            cmd.cmdTmpl['PS_ID'] = rows[i][0]
            cmd.cmdTmpl['DONE_CODE'] = rows[i][1]
            waitPs = {'PS_ID': rows[i][0]}
            logging.debug('psid: %d %d', rows[i][0], i)
            aWaitPs[i] = waitPs
            dWaitNo[rows[i][0]] = i
        order.aWaitPs = aWaitPs
        order.dWaitNo = dWaitNo

    def getRegionCode(self, order):
        # sql = self.__class__.dSql['RegionCode']
        cur = self.getCurbyName('RegionCode')
        if 'BILL_ID' not in order.dParam: return False
        dVar = {'BILL_ID':order.dParam['BILL_ID']}
        self.conn.executeCur(cur, dVar)
        row = self.conn.fetchone(cur)
        order.dParam['REGION_CODE'] = row[0]
        for req in order.aReqMsg:
            req.cmdTmpl['REGION_CODE'] = order.dParam['REGION_CODE']

    def sendOrder(self, order):
        self.connectServer()
        self.setOrderCmd(order)
        self.getOrderId(order)
        self.getRegionCode(order)
        curName = 'SendPs%s' % order.dParam['REGION_CODE']
        cur = self.getCurbyName(curName)
        # logging.debug(order.aReqMsg)
        aParam = []
        for req in order.aReqMsg:
            # aParam.append(req.cmdTmpl)
            # logging.debug('order cmd: %s', req.cmdTmpl)
            cmd = req.cmdTmpl
            cmd['NOTES'] = '%s : %s' % (cmd.pop('PS_MODEL_NAME'), cmd.pop('OLD_PS_ID'))
            aParam = [req.cmdTmpl]
            self.conn.executemanyCur(cur, aParam)
            cur.connection.commit()
            time.sleep(3)
        # logging.debug(aParam)
        # self.conn.executemanyCur(cur, aParam)
        # cur.connection.commit()
        # for req in order.aReqMsg:
        #     req['REGION_CODE'] = order.dParam['REGION_CODE']
        #     logging.debug('send:%s', req)
        #     self.remoteServer.send(req)
        #     self.recvResp(order)
    def recvOrder(self, order):
        self.connectServer()
        regionCode = order.dParam['REGION_CODE']
        # month = time.strftime("%Y%m", time.localtime())
        month = order.aReqMsg[0]['CREATE_DATE'].strftime('%Y%m')
        curName = 'RecvPs%s_%s' % (regionCode, month)
        cur = self.getCurbyName(curName)
        aPsid = order.aWaitPs
        while (len(aPsid) > 0):
            self.conn.executemanyCur(cur, aPsid)
            rows = self.conn.fetchall(cur)
            for row in rows:
                psId = row[0]
                psStatus = row[1]
                failReason = row[2]
                indx = order.dWaitNo[psId]
                order.aResp[indx] = row
                order.aWaitPs.pop(indx)
            num = len(rows)
            aPsid = order.aWaitPs
            logging.info('get %d result, remain %d ps', num, len(aPsid))
            time.sleep(30)


class FileFac(object):
    def __init__(self, main):
        self.main = main
        self.netType = main.netType
        self.netCode = main.netCode
        self.orderDsName = main.dsIn
        self.orderDs = None
        self.aNetInfo = []
        self.dNetClient = {}
        self.respName = '%s.rsp' % os.path.basename(self.main.outFile)
        self.respFullName = os.path.join(self.main.dirOutput, self.respName)
        self.resp = None
        self.aCmdTemplates = []

    def openDs(self):
        if self.orderDs: return self.orderDs
        logging.info('open ds %s', self.orderDsName)
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
        logging.info('open response file: %s', self.respName)
        if self.resp is None:
            logging.fatal('Can not open response file %s.', self.respName)
            exit(2)
        return self.resp

    def saveResp(self, order):
        for rsp in order.aResp:
            self.resp.write('%s %s%s' % (order.dParam['BILL_ID'], rsp, os.linesep))

    def loadCmd(self):
        tmpl = None
        tmplMsg = ''
        for line in self.main.fCmd:
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
        self.main.fCmd.close()

    def makeNet(self):
        cfg = self.main.cfg
        if self.netType not in cfg.dNet:
            logging.fatal('no find net type %s', self.netType)
            exit(2)
        self.aNetInfo = cfg.dNet[self.netType]
        netClassName = '%sClient' % self.netType
        logging.info('load %d net info.', len(self.aNetInfo))
        for netInfo in self.aNetInfo:
            # print netInfo
            net = createInstance(self.main.appNameBody, netClassName, netInfo)
            net.aCmdTemplates = self.aCmdTemplates
            net.prepareTmpl()
            # net.tmplReplaceNetInfo()
            # net.makeHttpHead()
            netCode = netInfo['NetCode']
            self.dNetClient[netCode] = net
        return self.dNetClient

    def makeOrderFildName(self):
        fildName = self.orderDs.readline()
        logging.info('field: %s', fildName)
        fildName = fildName.upper()
        self.aFildName = fildName.split()

    def makeOrder(self):
        orderClassName = '%sOrder' % self.netType
        logging.debug('load order %s.', orderClassName)
        for line in self.orderDs:
            line = line.strip()
            logging.debug(line)
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aParams = line.split()

            order = createInstance(self.main.appNameBody, orderClassName)
            order.setParaName(self.aFildName)
            order.setPara(aParams)
            logging.debug('order param: %s', order.dParam)
            # netCode = self.aNetInfo[0]['NetCode']
            order.net = self.dNetClient[self.netCode]
            return order
        return None


class TableFac(FileFac):
    dSql = {}
    dSql['LOADTMPL'] = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param,ps_model_name from %s where status=1 order by sort'
    dSql['LOADTMPLBYPS'] = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param,ps_model_name from %s where status=1 and ps_id=:PS_ID order by sort'
    dCur = {}
    def __init__(self, main):
        super(self.__class__, self).__init__(main)
        self.respName = '%s_rsp' % os.path.basename(self.main.outFile)
        self.respFullName = self.respName
        self.conn = main.conn
        self.cmdTab = self.main.cmdFile

    def getCurbyName(self, curName):
        if curName in self.dCur: return self.dCur[curName]
        if curName not in self.dSql:
            return None
        sql = self.dSql[curName] % self.cmdTab
        cur = self.conn.prepareSql(sql)
        self.dCur[curName] = cur
        return cur

    def loadCmd(self):
        tmpl = None
        tmplMsg = ''
        # sql = 'select ps_id,region_code,bill_id,sub_bill_id,ps_service_type,action_id,ps_param from %s where status=1 order by sort' % self.cmdTab
        logging.info('load cmd template.')
        para = None
        if self.main.psId:
            cur = self.getCurbyName('LOADTMPLBYPS')
            para = {'PS_ID': self.main.psId}
        else:
            cur = self.getCurbyName('LOADTMPL')
        self.conn.executeCur(cur, para)
        rows = self.conn.fetchall(cur)
        for line in rows:
            cmd = {}
            for i,field in enumerate(cur.description):
                cmd[field[0]] = line[i]
            cmd['OLD_PS_ID'] = cmd['PS_ID']
            tmpl = KtPsTmpl(cmd)
            self.aCmdTemplates.append(tmpl)
            # logging.info(line)
            logging.info('cmd template: %s', cmd)
        cur.close()
        logging.info('load %d cmd templates.' % len(self.aCmdTemplates))



class HttpShortOrder(ReqOrder):
    pass


class Director(object):
    def __init__(self, factory):
        self.factory = factory
        self.shutDown = None
        self.fRsp = None

    def start(self):
        self.factory.loadCmd()
        self.factory.makeNet()
        self.factory.openDs()
        self.factory.makeOrderFildName()
        self.fRsp = self.factory.openRsp()
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load order', time.time())
            order = self.factory.makeOrder()

            if order is None:
                logging.info('load all orders,')
                break
            # logging.debug(order.dParam)
            client = order.net
            i += 1
            # client.connectServer()
            logging.info('send order:')
            client.sendOrder(order)
            # client.recvResp(order)
            # client.saveResp(order)
            # client.remoteServer.close()
            self.factory.saveResp(order)
        self.factory.closeDs()
        self.factory.resp.close()
        logging.info('all order completed.')


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.fCmd = None
        self.caseDs = None
        self.netType = None
        self.netCode = None
        self.conn = None
        self.psId = None

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
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logNamePre = '%s_%s' % (self.appNameBody, self.today)
        outFileName = '%s_%s' % (os.path.basename(self.dsIn), self.today)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)
        self.outFile = os.path.join(self.dirOutput, outFileName)
        # logging.info('outfile: %s', self.outFile)

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.appName = appName
        if self.argc < 3:
            self.usage()
        # self.checkopt()
        argvs = sys.argv[1:]
        self.facType = 'f'
        try:
            opts, arvs = getopt.getopt(argvs, "t:p:")
        except getopt.GetoptError, e:
            orderMode = 't'
            print 'get opt error:%s. %s' % (argvs,e)
            # self.usage()
        for opt, arg in opts:
            # print 'opt: %s' % opt
            if opt == '-t':
                self.facType = 't'
                self.cmdFile = arg
            elif opt == '-p':
                self.psId = arg
        if self.facType == 'f':
            self.cmdFile = arvs[0]
            self.dsIn = arvs[1]
        else:
            self.dsIn = arvs[0]
        # self.logFile = '%s%s' % (self.dsIn, '.log')
        # self.resultOut = '%s%s' % (self.dsIn, '.rsp')

    def usage(self):
        print "Usage: %s [-t] cmdfile [-p psid] datafile" % self.appName
        print "example:   %s %s" % (self.appName,'creatUser -p 2577133267 teldata')
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

    # def connDb(self):
    #     if self.conn: return self.conn
    #     try:
    #         connstr = self.cfg.dbinfo['connstr']
    #         self.conn = orcl.Connection(connstr)
    #         # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
    #         # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
    #         # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
    #     except Exception, e:
    #         logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['dbhost'], self.cfg.dbinfo['dbusr'], self.cfg.dbinfo['dbsid'], e)
    #         exit()
    #     return self.conn

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError, e:
            logging.error('prepare sql err: %s', sql)
            return None
        return cur

    def makeFactory(self):
        if self.facType == 't':
            return self.makeTableFactory()
        elif self.facType == 'f':
            return self.makeFileFactory()

    def makeTableFactory(self):
        self.netType = 'KtPs'
        self.netCode = 'kt4'
        logging.info('net type: %s  net code: %s', self.netType, self.netCode)
        fac = TableFac(self)
        return fac

    def makeFileFactory(self):
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

    @staticmethod
    def createInstance(module_name, class_name, *args, **kwargs):
        module_meta = __import__(module_name, globals(), locals(), [class_name])
        class_meta = getattr(module_meta, class_name)
        obj = class_meta(*args, **kwargs)
        return obj

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        self.cfg = Conf(main, self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)

        self.cfg.loadDbinfo()
        self.connectServer()
        self.cfg.loadNet()
        factory = self.makeFactory()
        print('respfile: %s' % factory.respFullName)
        director = Director(factory)
        director.start()


def createInstance(module_name, class_name, *args, **kwargs):
    module_meta = __import__(module_name, globals(), locals(), [class_name])
    class_meta = getattr(module_meta, class_name)
    obj = class_meta(*args, **kwargs)
    return obj

# main here
if __name__ == '__main__':
    main = Main()
    # main.checkArgv()
    main.start()
    logging.info('%s complete.', main.appName)
