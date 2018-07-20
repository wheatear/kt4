#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""kt appc.sh tool"""

import sys
import os
import shutil
import time
import datetime
import copy
import multiprocessing
import Queue
import signal
import getopt
import cx_Oracle as orcl
import socket
import glob
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


class KtOrder(object):
    def __init__(self, line, file):
        # self.tradeId = tradeId
        # self.msisdn = msisdn
        self.line = line
        self.file = file

        self.no = None
        self.aParamName = []
        self.dParam = {}
        self.net = None
        self.aReq = []
        self.aResp = []
        self.aWaitPs = []
        self.dWaitNo = {}

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

class CheckRead(threading.Thread):
    sqlPsid = "select SEQ_PS_ID.NEXTVAL FROM dual"
    def __init__(self, builder):
        threading.Thread.__init__(self)
        self.builder = builder
        self.main = builder.main
        self.conn = self.main.conn
        self.aFiles = builder.aFiles
        self.dFiles = builder.dFiles
        self.kt = builder.kt
        self.orderQueue = builder.orderQueue
        self.curPsid = self.conn.prepareSql(self.sqlPsid)

    def loadPsId(self):
        sql = self.sqlPsid
        para = None
        # cur = self.conn.prepareSql(sql)
        self.conn.executeCur(self.curPsid, para)
        row = self.conn.fetchone(self.curPsid)
        psId = row[0]
        return psId

    def run(self):
        for fi in self.aFiles:
            # fileBody, fileExt = os.path.splitext(fi)
            # aCmdTpl = self.builder.dFildCmdMap[fileExt]
            logging.info('process file %s', fi)
            tplName = self.dFiles[fi][2]
            if tplName not in self.builder.dCmdTplGrp:
                logging.error('no cmd template for %s', fi)
                exit
            self.kt.aCmdTemplates = self.builder.dCmdTplGrp[tplName]

            i = 0
            fp = self.main.openFile(fi, 'r')
            for line in fp:
                line = line.strip()
                if len(line) < 1:
                    continue
                i += 1
                if i > 199:
                    i = 0
                    time.sleep(3)
                    while self.orderQueue.qsize() > 1000:
                        logging.info('order queue size exceed 1000, sleep 10')
                        time.sleep(10)
                logging.debug(line)
                aPara = line.split()
                billId = aPara[2]
                subBillId = aPara[1]
                aFieldName = ['BILL_ID', 'SUB_BILL_ID']
                aKtPara = [billId, subBillId]
                order = KtOrder(line, fi)
                order.setParaName(aFieldName)
                order.setPara(aKtPara)
                self.kt.sendOrder(order)
                self.orderQueue.put(order, 1)

            fp.close()
            logging.info('read %s complete, and delete.', fi)
            os.remove(fi)


class CheckWrite(threading.Thread):
    def __init__(self, builder):
        threading.Thread.__init__(self)
        self.main = builder.main
        self.aFiles = builder.aFiles
        self.dFiles = builder.dFiles
        self.kt = builder.kt
        self.orderQueue = builder.orderQueue
        self.dOrder = {}
        self.doneOrders = {}
        self.pattImsi = re.compile(r'IMSI1=(\d{15});')

    def run(self):
        if len(self.dFiles) == 0:
            return
        for file in self.dFiles:
            self.doneOrders[file] = 0
        time.sleep(10)
        emptyCounter = 0
        i = 0
        while 1:
            order = None
            if self.orderQueue.empty():
                emptyCounter += 1
                if emptyCounter > 20:
                    logging.info('exceed 20 times empty, exit.')
                    break
                continue
            i += 1
            if i > 199:
                i = 0
                time.sleep(3)
            order = self.orderQueue.get(1)
            logging.debug('get order %s %d from queue', order.dParam['BILL_ID'], order.aReq[0].cmdTmpl['PS_ID'])
            self.kt.recvOrder(order)
            if len(order.aWaitPs) > 0:
                self.orderQueue.put(order, 1)
                continue
            self.makeRsp(order)
            inFile = order.file
            self.doneOrders[inFile] += 1
            if self.doneOrders[inFile] == self.dFiles[inFile][1]:
                logging.info('file %s completed after process %d lines.', inFile, self.doneOrders[inFile])
                aFileInfo = self.dFiles.pop(inFile)
                self.dealFile(aFileInfo)
            if len(self.dFiles) == 0:
                break

    def checkSub(self, orderRsp, inFile):
        rsp = orderRsp[3]
        m = None
        m = self.pattImsi.search(rsp)
        fp = None
        if m:
            logging.debug(m.groups())
            fp = self.dFiles[inFile][6]
        else:
            logging.debug('no find %s', orderRsp[0])
            fp = self.dFiles[inFile][7]
        return fp

    def makeRsp(self, order):
        inFile = order.file
        fp = self.dFiles[inFile][4]
        for rsp in order.aResp:
            psStatus = rsp[1]
            failReason = rsp[2]
            if psStatus == 9:
                continue
            if failReason.find('Subscriber not defined, KI not loaded') > -1:
                continue
            if psStatus < 9:
                fp.write('%s%s' % (order.line, os.linesep))
                break

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


class SubCheck(object):
    def __init__(self, builder):
        self.builder = builder
        self.shutDown = None
        self.fRsp = None

    def start(self):
        pass


class TcpClt(object):
    def __init__(self, host, port, bufSize=5120):
        self.addr = (host, port)
        self.bufSize = bufSize
        try:
            tcpClt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
        # logging.debug('recv head: %s', rspHead)
        lenHead = len(rspHead)
        while lenHead < 12:
            lenLast = 12 - lenHead
            rspHead = '%s%s' % (rspHead, self.tcpClt.recv(lenLast))
            # logging.debug('loop recv head: %s', rspHead)
            lenHead = len(rspHead)
        logging.debug('recv package head:%s', rspHead)
        rspLen = int(rspHead[0:8])
        rspBodyLen = rspLen - 12
        rspMsg = self.tcpClt.recv(rspBodyLen)
        # logging.debug('recv: %s', rspMsg)
        rcvLen = len(rspMsg)
        while rcvLen < rspBodyLen:
            rspTailLen = rspBodyLen - rcvLen
            rspMsg = '%s%s' % (rspMsg, self.tcpClt.recv(rspTailLen))
            logging.debug(rspMsg)
            rcvLen = len(rspMsg)
        rspMsg = '%s%s' % (rspHead, rspMsg)
        logging.debug('recv: %s', rspMsg)
        return rspMsg


class KtPsTmpl(object):
    def __init__(self, cmdTmpl):
        # super(self.__class__, self).__init__(cmdTmpl)
        self.cmdTmpl = cmdTmpl
        self.varExpt = r'@(.+?)@'

    def setMsg(self, tmpl):
        pass


class KtClient(object):
    dSql = {}
    dSql['OrderId'] = 'select SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL FROM (select 1 from all_objects where rownum <= :PSNUM)'
    # dSql['OrderId'] = 'select SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL FROM DUAL'
    dSql['RegionCode'] = "select region_code,ps_net_code from ps_net_number_area t where :BILL_ID between start_number and end_number"
    dSql['SendPs'] = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES,notes) values(:PS_ID,0,:DONE_CODE,0,80,:PS_SERVICE_TYPE,:BILL_ID,:SUB_BILL_ID,sysdate,:CREATE_DATE,sysdate,:ACTION_ID,:PS_PARAM,0,530,:REGION_CODE,100,0,5,:NOTES)'
    dSql['RecvPs'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s where ps_id=:PS_ID order by end_date desc'
    dSql['AsyncStatus'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s_%s where create_date>=:firstDate and create_date<=:lastDate'
    dSql['SyncServ'] = "select substr(a.class,9),c.rpc_ip,a.value from kt4.sys_config a, kt4.sys_machine_process b, kt4.rpc_register c where substr(a.class,9)=b.process_name and b.machine_name=c.machine_name and c.app_name='appControl' and a.class like 'spliter|sync_split%' and a.name='ServicePort' and b.state=1"
    dSql['TradeId'] = 'select SEQ_PS_ID.NEXTVAL FROM (select 1 from all_objects where rownum <= :PSNUM)'

    def __init__(self, conn, writeConn):
        self.conn = conn
        self.writeConn = writeConn
        self.orderTablePre = 'i_provision'
        self.orderHisTablePre = 'ps_provision'
        self.syncServer = None
        self.syncPort = None
        self.tcpClt = None
        # self.loadSyncServer()
        # self.connTcpServer()
        self.dCur = {}
        self.aCmdTemplates = []

    def loadSyncServer(self):
        sql = self.dSql['SyncServ']
        para = None
        cur = self.conn.prepareSql(sql)
        self.conn.executeCur(cur, para)
        rows = self.conn.fetchall(cur)
        self.syncServer = rows[0][1]
        self.syncPort = rows[0][2]
        cur.close()
        logging.info('server ip: %s  port: %s', self.syncServer ,self.syncPort)

    def getSyncOrderInfo(self):
        cur = self.ktClient.conn.cursor()
        sql = "select SEQ_PS_ID.NEXTVAL from dual"
        cur.execute(sql)
        result = cur.fetchone()
        if result:
            self.tradeId = result[0]
        cur.close()
        logging.debug('load tradeId: %d', self.tradeId)

    def connTcpServer(self):
        if self.tcpClt: return self.tcpClt
        self.tcpClt = TcpClt(self.syncServer, int(self.syncPort))

    def syncSendOne(self, order):
        reqMsg = 'TRADE_ID=%d;ACTION_ID=1;PS_SERVICE_TYPE=HLR;DISP_SUB=4;MSISDN=%s;IMSI=111111111111111' % (
        order.tradeId, order.msisdn)
        # logging.debug('send order num:%d , order name:%s', order.ktCase.psNo, order.ktCase.psCaseName)
        logging.debug(reqMsg)
        self.tcpClt.send(reqMsg)
        # rspMsg = self.recvResp()
        # order.response = rspMsg
        # logging.debug('%s has response %s',order.ktCase.psCaseName, order.response)

    def syncSend(self):
        # "TRADE_ID=5352420;ACTION_ID=1;DISP_SUB=4;PS_SERVICE_TYPE=HLR;MSISDN=%s;IMSI=%s;"
        i = 0
        logging.info('send syncronous orders to %s', self.ktName)
        for order in self.ordGroup.aOrders:
            i += 1
            self.syncSendOne(order)
        logging.info('send %d orders.', i)

    def syncRecv(self):
        # '00000076RSP:TRADE_ID=2287919;ERR_CODE=-1;ERR_DESC=????????????!'
        logging.info('receive %s synchrous orders response...', 'kt')
        # orderNum = self.ordGroup.orderNum
        regx = re.compile(r'RSP:TRADE_ID=(\d+);ERR_CODE=(.+?);ERR_DESC=([^;]*);(.*)')
        # i = 0
        # while i < orderNum:
        rspMsg = self.recvResp()
        # logging.debug(rspMsg)
        m = regx.search(rspMsg)
        orderRsp = None
        if m is not None:
            tridId = int(m.group(1))
            errCode = int(m.group(2))
            errDesc = m.group(3)
            resp = m.group(4)

            orderRsp = [tridId, errCode, errDesc, resp]
            logging.debug('recv order response ,tradeId:%s, syncStatus:%d, syncDesc:%s, response:%s.', tridId,
                          errCode, errDesc, resp)
        return orderRsp

    def recvResp(self):
        rspMsg = self.tcpClt.recv()
        # logging.debug(rspMsg)
        return rspMsg

    def getCurbyName(self, curName):
        if curName in self.dCur: return self.dCur[curName]
        if (curName[:6] != 'SendPs') and (curName[:6] != 'RecvPs') and (curName not in self.dSql):
            logging.error('no such cur sql: %s', curName)
            return None
        sql = ''
        if curName[:6] == 'SendPs':
            namePre = curName[:6]
            regionCode = curName[6:]
            sql = self.dSql[namePre] % (self.orderTablePre, regionCode)
        elif curName[:6] == 'RecvPs':
            namePre = curName[:6]
            regionCode = curName[6:]
            sql = self.dSql[namePre] % (regionCode)
        else:
            sql = self.dSql[curName]
        if curName[:6] == 'RecvPs':
            cur = self.writeConn.prepareSql(sql)
        else:
            cur = self.conn.prepareSql(sql)
        self.dCur[curName] = cur
        return cur

    def prepareTmpl(self):
        return True

    def setOrderCmd(self, order):
        order.aReq = copy.deepcopy(self.aCmdTemplates)
        dtNow = datetime.datetime.now()
        for cmd in order.aReq:
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
        dPara = {'PSNUM':len(order.aReq)}
        num = len(order.aReq)
        self.conn.executeCur(cur, dPara)
        rows = self.conn.fetchall(cur)
        aWaitPs = [None] * len(order.aReq)
        aResp = [None] * len(order.aReq)
        dWaitNo = {}
        for i,cmd in enumerate(order.aReq):
            psId = rows[i][0]
            doneCode = rows[i][1]
            cmd.cmdTmpl['PS_ID'] = psId
            cmd.cmdTmpl['DONE_CODE'] = doneCode
            waitPs = {'PS_ID': psId}
            logging.debug('psid: %d %d', psId, i)
            aWaitPs[i] = waitPs
            dWaitNo[psId] = i
        order.aWaitPs = aWaitPs
        order.dWaitNo = dWaitNo
        order.aResp = aResp

    def getRegionCode(self, order):
        # sql = self.__class__.dSql['RegionCode']
        cur = self.getCurbyName('RegionCode')
        if 'BILL_ID' not in order.dParam: return False
        dVar = {'BILL_ID':order.dParam['BILL_ID']}
        self.conn.executeCur(cur, dVar)
        row = self.conn.fetchone(cur)
        order.dParam['REGION_CODE'] = '100'
        if row:
            order.dParam['REGION_CODE'] = row[0]
        for req in order.aReq:
            req.cmdTmpl['REGION_CODE'] = order.dParam['REGION_CODE']

    def sendOrder(self, order):
        self.setOrderCmd(order)
        self.getOrderId(order)
        self.getRegionCode(order)
        curName = 'SendPs%s' % order.dParam['REGION_CODE']
        cur = self.getCurbyName(curName)
        # logging.debug(order.aReqMsg)
        aParam = []
        for req in order.aReq:
            # aParam.append(req.cmdTmpl)
            # logging.debug('order cmd: %s', req.cmdTmpl)
            cmd = req.cmdTmpl
            cmd['NOTES'] = 'kidel.py %s : %s' % (cmd.pop('PS_MODEL_NAME'), cmd.pop('OLD_PS_ID'))
            aParam = [req.cmdTmpl]
            self.conn.executemanyCur(cur, aParam)
            cur.connection.commit()
            # time.sleep(3)
        # logging.debug(aParam)
        # self.conn.executemanyCur(cur, aParam)
        # cur.connection.commit()
        # for req in order.aReqMsg:
        #     req['REGION_CODE'] = order.dParam['REGION_CODE']
        #     logging.debug('send:%s', req)
        #     self.remoteServer.send(req)
        #     self.recvResp(order)

    def recvOrder(self, order):
        # self.connectServer()
        regionCode = order.dParam['REGION_CODE']
        # month = time.strftime("%Y%m", time.localtime())
        month = order.aReq[0].cmdTmpl['CREATE_DATE'].strftime('%Y%m')
        curName = 'RecvPs%s_%s' % (regionCode, month)
        cur = self.getCurbyName(curName)
        aPsid = order.aWaitPs
        j = 0
        for i,psId in enumerate(aPsid):
            self.conn.executeCur(cur, psId)
            row = self.conn.fetchone(cur)
        # for row in rows:
            if not row:
                continue
            j += 1
            psId = row[0]
            psStatus = row[1]
            failReason = row[2]
            # indx = order.dWaitNo[psId]
            logging.debug(row)
            order.aResp[i] = row
            order.aWaitPs.pop(i)
        logging.info('get %d result, remain %d ps', j, len(order.aWaitPs))
        # while (len(aPsid) > 0):
        #     self.conn.executemanyCur(cur, aPsid)
        #     rows = self.conn.fetchall(cur)
        #     for row in rows:
        #         psId = row[0]
        #         psStatus = row[1]
        #         failReason = row[2]
        #         indx = order.dWaitNo[psId]
        #         order.aResp[indx] = row
        #         order.aWaitPs.pop(indx)
        #     num = len(rows)
        #     aPsid = order.aWaitPs
        #     logging.info('get %d result, remain %d ps', num, len(aPsid))
        #     time.sleep(30)


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

class Builder(object):

    def __init__(self, main, inFile):
        self.main = main
        self.inFile = inFile
        self.conn = main.conn
        self.writeConn = main.writeConn
        self.aFiles = []
        self.dFiles = {}
        self.kt = None
        self.orderQueue = None
        self.dCmdTplGrp = {}
        self.dFildCmdMap = {'.cy1':'delsub', '.cy2':'delauc', '.cy3':'delsub'}

    def findFile(self):
        logging.info('find files ')
        filePatt = os.path.join(self.main.dirIn, self.inFile)
        self.aFiles = glob.glob(filePatt)
        logging.info('find files: %s', self.aFiles)
        return self.aFiles

    def lineCount(self):
        for fi in self.aFiles:
            fileBase = os.path.basename(fi)
            nameBody,nameExt = os.path.splitext(fileBase)
            cmdTpl = self.dFildCmdMap[nameExt]
            fileBack = os.path.join(self.main.dirBack, fileBase)
            shutil.copy(fi, fileBack)
            fileRsp = '%s.rsp' % fileBase
            fileWkRsp = os.path.join(self.main.dirWork, fileRsp)
            fWkRsp = self.main.openFile(fileWkRsp, 'w')
            fileOutRsp = os.path.join(self.main.dirOut, fileRsp)

            count = -1
            for count, line in enumerate(open(fi, 'rU')):
                pass
            count += 1
            self.dFiles[fi] = [fileBase,count, cmdTpl, fileWkRsp, fWkRsp, fileOutRsp]
            logging.info('file: %s %d', fi, count)
            # self.dFileSize[fi] = count
        return self.dFiles

    def loadCmd(self):
        logging.info('loading cmd template')
        tplPatt = self.main.tplFile
        aTplFiles = glob.glob(tplPatt)
        logging.info('template files: %s', aTplFiles)
        for tplFile in aTplFiles:
            fileName = os.path.basename(tplFile)
            tplName = os.path.splitext(fileName)[0]
            aCmdTmpl = []
            tmplCmd = {}
            i = 0
            fCmd = self.main.openFile(tplFile, 'r')
            for line in fCmd:
                line = line.strip()
                if len(line) == 0:
                    continue
                if line[0] == '#':
                    continue
                if line == '$END$':
                    if len(tmplCmd) > 0:
                        i += 1
                        tmplCmd['OLD_PS_ID'] = i
                        tmplCmd['PS_MODEL_NAME'] = tplFile
                        logging.info(tmplCmd)
                        tmpl = KtPsTmpl(tmplCmd)
                        aCmdTmpl.append(tmpl)
                        tmpl = None
                        tmplCmd = {}
                    continue
                if line == 'KT_REQUEST':
                    tmpl = None
                    tmplCmd = {}
                    continue
                aParam = line.split(' ', 1)
                if len(aParam) < 1:
                    continue
                tmplCmd[aParam[0]] = aParam[1]
            self.dCmdTplGrp[tplName] = aCmdTmpl

    def buildKtClient(self):
        self.kt = KtClient(self.conn, self.writeConn)
        return self.kt

    def buildQueue(self):
        self.orderQueue = Queue.Queue(1000)
        return self.orderQueue

    def buildCheckRead(self):
        checkReader = CheckRead(self)
        return checkReader

    def buildCheckWrite(self):
        checkWriter = CheckWrite(self)
        return checkWriter

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
            logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.dbInfo['dbhost'], self.dbInfo['dbusr'], self.dbInfo['dbsid'], e)
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
        # logging.info('execute cur %s : %s', cur.statement, params)
        try:
            cur.executemany(None, params)
        except orcl.DatabaseError, e:
            logging.error('executemany sql err %s:%s ', e, cur.statement)
            return None
        return cur

    def executeCur(self, cur, params=None):
        # logging.info('execute cur %s', cur.statement)
        try:
            if params is None:
                cur.execute(None)
            else:
                cur.execute(None, params)
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s %s', e, cur.statement, params)
            return None
        return cur

    def fetchmany(self, cur):
        # logging.debug('fetch %d rows from %s', cur.arraysize, cur.statement)
        try:
            rows = cur.fetchmany()
        except orcl.DatabaseError, e:
            logging.error('fetchmany sql err %s:%s ', e, cur.statement)
            return None
        return rows

    def fetchone(self, cur):
        # logging.debug('fethone from %s', cur.statement)
        try:
            row = cur.fetchone()
        except orcl.DatabaseError, e:
            logging.error('fetchone sql err %s:%s ', e, cur.statement)
            return None
        return row

    def fetchall(self, cur):
        # logging.debug('feth all from %s', cur.statement)
        try:
            rows = cur.fetchall()
        except orcl.DatabaseError, e:
            logging.error('fetchall sql err %s:%s ', e, cur.statement)
            return None
        return rows


class Director(object):
    def __init__(self, builder):
        self.builder = builder
        self.shutDown = None
        self.fRsp = None

    def start(self):
        logging.info('find in files')
        self.builder.findFile()
        self.builder.lineCount()
        self.builder.loadCmd()
        self.builder.buildKtClient()
        queue = self.builder.buildQueue()
        reader = self.builder.buildCheckRead()
        writer = self.builder.buildCheckWrite()

        logging.info('reader start.')
        reader.start()
        logging.info('writer start.')
        writer.start()

        reader.join()
        logging.info('reader complete.')
        writer.join()
        logging.info('writer complete.')


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.conn = None
        self.writeConn = None
        self.inFile = None
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        appNameBody, appNameExt = os.path.splitext(self.appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt

        if self.argc < 2:
            self.usage()
            # self.inFile = 'wsdx_cycle_%s.cy?' % self.today
        else:
            self.inFile = sys.argv[1]
        # argvs = sys.argv[1:]

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
        self.dirLog = os.path.join(self.dirApp, 'log')
        # self.dirCfg = os.path.join(self.dirApp, 'config')
        self.dirCfg = self.dirBin
        self.dirBack = os.path.join(self.dirApp, 'back')
        self.dirIn = os.path.join(self.dirApp, 'input')
        self.dirLib = os.path.join(self.dirApp, 'lib')
        self.dirOut = os.path.join(self.dirApp, 'output')
        self.dirWork = os.path.join(self.dirApp, 'work')
        self.dirTpl = os.path.join(self.dirApp, 'template')

        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        outName = '%s_%s' % (self.appNameBody, self.nowtime)
        tplName = '*.tpl'
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.tplFile = os.path.join(self.dirTpl, tplName)
        # self.logPre = os.path.join(self.dirLog, logPre)
        # self.outFile = os.path.join(self.dirOut, outName)

    def usage(self):
        print "Usage: %s [-t|f orderTmpl] [-p|r psid|all] [datefile]" % self.appName
        print "example:  %s -t %s" % (self.appName,'ps_model_summary')
        print "\t%s -f %s" % (self.appName, 'kt_hlr')
        print "\t%s -t ps_model_summary -p 2451845353" % (self.appName)
        print "\t%s -t ps_model_summary -p 2451845353 pccnum" % (self.appName)
        print "\t%s -f kt_hlr -p 2451845353 pccnum" % (self.appName)
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
        self.writeConn = DbConn(self.cfg.dbinfo)
        self.writeConn.connectServer()
        return self.conn

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        self.cfg = Conf(self.cfgFile)
        self.logLevel = self.cfg.loadLogLevel()
        # self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...' % self.appName)
        logging.info('infile: %s' % self.inFile)

        self.cfg.loadDbinfo()
        self.connectServer()
        builder = Builder(self, self.inFile)
        director = Director(builder)
        director.start()


# main here
if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.appName)