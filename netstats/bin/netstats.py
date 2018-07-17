#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""kt appc.sh tool"""

import sys
import os
import shutil
import time
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
    def __init__(self, tradeId, msisdn, line, file):
        self.tradeId = tradeId
        self.msisdn = msisdn
        self.line = line
        self.file = file

class CheckRead(threading.Thread):
    sqlPsid = "select SEQ_PS_ID.NEXTVAL FROM dual"
    def __init__(self, builder):
        threading.Thread.__init__(self)
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
            i = 0
            fp = self.main.openFile(fi, 'r')
            for line in fp:
                line = line.strip()
                if len(line) < 1:
                    continue
                i += 1
                if i > 199:
                    i = 0
                    if self.orderQueue.qsize() > 1000:
                        logging.info('order queue size exceed 1000, sleep 10')
                        time.sleep(10)
                tradeId = self.loadPsId()
                aMsisdn = line.split()
                msisdn = aMsisdn[1]
                order = KtOrder(tradeId, msisdn, line, fi)
                self.orderQueue.put(order, 1)
                self.kt.syncSendOne(order)
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
        self.doneFile = {}
        self.pattImsi = re.compile(r'IMSI1=(\d{15});')

    def run(self):
        if len(self.dFiles) == 0:
            return
        for file in self.dFiles:
            self.doneFile[file] = 0
        while 1:
            order = None
            # if not self.orderQueue.empty():
            #     order = self.orderQueue.get(1)
            #     self.dOrder[order.tradeId] = order
            #     logging.debug('get order %d from queue', order.tradeId)
            orderRsp = self.kt.syncRecv()
            logging.debug(orderRsp)
            tradeId = int(orderRsp[0])
            logging.debug('get order %d from socket', tradeId)

            while tradeId not in self.dOrder:
                order = self.orderQueue.get(1)
                self.dOrder[order.tradeId] = order
                logging.debug('get order %d from queue', order.tradeId)
            order = None
            if tradeId in self.dOrder:
                order = self.dOrder.pop(tradeId)
            else:
                logging.error('tradeid %d is not check order', tradeId)
                continue
            inFile = order.file
            # fp = self.dFiles[inFile][6]
            fp = self.checkSub(orderRsp, inFile)
            logging.debug('write %d to rsp file', tradeId)
            fp.write('%s%s' % (order.line, os.linesep))
            self.doneFile[inFile] += 1
            if self.doneFile[inFile] == self.dFiles[inFile][1]:
                logging.info('file %s completed after process %d lines.', inFile, self.doneFile[inFile])
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

    def dealFile(self, aFileInfo):
        # self.dFiles[fi] = [fileBase, count, fileWkSuc, fileWkErr, fileOutSuc, fileOutErr, fWkSuc, fWkErr]
        fSuc = aFileInfo[6]
        fErr = aFileInfo[7]
        fSuc.close()
        fErr.close()
        fileWkSuc = aFileInfo[2]
        fileWkErr = aFileInfo[3]
        fileOutSuc = aFileInfo[4]
        fileOutErr = aFileInfo[5]
        fileBkSuc = os.path.join(self.main.dirBack, os.path.basename(fileWkSuc))
        fileBkErr = os.path.join(self.main.dirBack, os.path.basename(fileWkErr))
        shutil.copy(fileWkSuc, fileBkSuc)
        shutil.copy(fileWkErr, fileBkErr)
        os.rename(fileWkSuc, fileOutSuc)
        os.rename(fileWkErr, fileOutErr)
        logging.info('%s complete', aFileInfo[0])


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


class KtClient(object):
    def __init__(self, ip, port):
        self.syncServer = ip
        self.syncPort = port
        self.tcpClt = None
        self.connTcpServer()

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


class Builder(object):
    sqlSyncServ = "select substr(a.class,9),c.rpc_ip,a.value from kt4.sys_config a, kt4.sys_machine_process b, kt4.rpc_register c where substr(a.class,9)=b.process_name and b.machine_name=c.machine_name and c.app_name='appControl' and a.class like 'spliter|sync_split%' and a.name='ServicePort' and b.state=1"

    def __init__(self, main, checkFile):
        self.main = main
        self.checkFile = checkFile
        self.conn = main.conn
        self.aFiles = []
        # self.dFileSize = {}
        self.dFiles = {}
        self.kt = None
        self.orderQueue = None

    def findFile(self):
        logging.info('build checkread ')
        filePatt = os.path.join(self.main.dirIn, self.checkFile)
        self.aFiles = glob.glob(filePatt)
        # for f in glob.iglob(filePatt):
        #     fName = os.path.basename(f)
        #     self.aFiles.append(fName)
        logging.info('check file: %s', self.aFiles)
        return self.aFiles

    def lineCount(self):
        for fi in self.aFiles:
            fileBase = os.path.basename(fi)
            fileBack = os.path.join(self.main.dirBack, fileBase)
            shutil.copy(fi, fileBack)
            fileSuc = '%s_suc.txt' % fileBase
            fileErr = '%s_err.txt' % fileBase
            fileWkSuc = os.path.join(self.main.dirWork, fileSuc)
            fileWkErr = os.path.join(self.main.dirWork, fileErr)
            fWkSuc = self.main.openFile(fileWkSuc, 'w')
            fWkErr = self.main.openFile(fileWkErr, 'w')
            fileOutSuc = os.path.join(self.main.dirOut, fileSuc)
            fileOutErr = os.path.join(self.main.dirOut, fileErr)
            count = -1
            for count, line in enumerate(open(fi, 'rU')):
                pass
            count += 1
            self.dFiles[fi] = [fileBase,count, fileWkSuc, fileWkErr, fileOutSuc, fileOutErr, fWkSuc, fWkErr]
            logging.info('file: %s %d', fi, count)
            # self.dFileSize[fi] = count
        return self.dFiles

    def buildKtClient(self):
        sql = self.sqlSyncServ
        para = None
        cur = self.conn.prepareSql(sql)
        self.conn.executeCur(cur, para)
        rows = self.conn.fetchall(cur)
        ip = rows[0][1]
        port = rows[0][2]
        cur.close()
        logging.info('server ip: %s  port: %s', ip ,port)
        self.kt = KtClient(ip, port)
        # self.kt = KtClient('10.7.5.164', '16101')
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


class NetStati(object):
    sqlNetCount = "select ps_net_code,ps_status,count(*) from v_ps_split_his_%s where create_date>=trunc(sysdate-1) and create_date<trunc(sysdate) group by ps_net_code,ps_status order by 1,2"
    sqlNetTime = "select ps_net_code,round(avg(end_date-start_date)*86400),round(max(end_date-start_date)*86400) from v_ps_split_his_%s where create_date>=trunc(sysdate-1) and create_date<trunc(sysdate) group by ps_net_code order by 1"
    sqlSaveNet = "insert into PS_SPLIT_STATISTICS_HIS_%s(ps_date,Ps_Net_Code,Ps_Total,Ps_Success,Ps_Fail,Ps_Average,PS_MAX) values(:PS_DATE,:PS_NET_CODE,:PS_TOTAL,:PS_SUCCESS,:PS_FAIL,:PS_AVERAGE,:PS_MAX)"
    aSaveNetField = ['PS_DATE','PS_NET_CODE','PS_TOTAL','PS_SUCCESS','PS_FAIL','PS_AVERAGE','PS_MAX']
    def __init__(self, main):
        self.main = main
        self.conn = main.conn
        self.dNetStati = {}

    def prepareEnv(self):
        click = time.time()
        self.yesClick = click - 86400
        self.yesTime = time.localtime(self.yesClick)
        self.yesDate = time.strftime('%Y%m%d', self.yesTime)
        self.iYesDate = int(self.yesDate)
        self.yesMon = time.strftime('%Y%m', self.yesTime)

        self.sqlNetCount = self.sqlNetCount % self.yesMon
        self.sqlNetTime = self.sqlNetTime % self.yesMon
        self.sqlSaveNet = self.sqlSaveNet % self.yesMon

    def getNetCount(self):
        self.curNetCount = self.conn.prepareSql(self.sqlNetCount)
        self.conn.executeCur(self.curNetCount)
        i = 0
        for row in self.curNetCount:
            netCode = row[0]
            status = row[1]
            netCount = row[2]
            if netCode not in self.dNetStati:
                self.dNetStati[netCode] = {'PS_DATE':self.yesDate, 'PS_NET_CODE':netCode}
            dNet = self.dNetStati[netCode]
            if status == -1:
                dNet['PS_FAIL'] = netCount
            elif status == 9:
                dNet['PS_SUCCESS'] = netCount
        self.curNetCount.close()

    def getNetTime(self):
        self.curNetTime = self.conn.prepareSql(self.sqlNetTime)
        self.conn.executeCur(self.curNetTime)
        i = 0
        for row in self.curNetTime:
            netCode = row[0]
            avgTime = row[1]
            maxTime = row[2]
            if netCode not in self.dNetStati:
                self.dNetStati[netCode] = {'PS_DATE':self.yesDate, 'PS_NET_CODE':netCode}
            dNet = self.dNetStati[netCode]
            dNet['PS_AVERAGE'] = avgTime
            dNet['PS_MAX'] = maxTime
        self.curNetTime.close()

    def saveNetInfo(self):
        cur = self.conn.prepareSql(self.sqlSaveNet)
        for net in self.dNetStati:
            dNet = self.dNetStati[net]
            for fi in self.aSaveNetField:
                if fi in dNet:
                    continue
                if fi in ('PS_SUCCESS', 'PS_FAIL'):
                    dNet[fi] = 0
                else:
                    dNet[fi] = None
            dNet['PS_TOTAL'] = dNet['PS_SUCCESS'] + dNet['PS_FAIL']
            self.conn.executeCur(cur, dNet)
            cur.connection.commit()
        cur.close()

    def start(self):
        self.prepareEnv()
        logging.info('1.取网元处理量统计数据...')
        self.getNetCount()
        logging.info('2.取网元处理时长数据...')
        self.getNetTime()
        logging.info('3.插入统计表...')
        self.saveNetInfo()
        self.conn.close()


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
        logging.info('execute cur %s : %s', cur.statement, params)
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

    def close(self):
        self.conn.close()


class Director(object):
    def __init__(self, builder):
        self.builder = builder
        self.shutDown = None
        self.fRsp = None

    def start(self):
        self.builder.findFile()
        self.builder.lineCount()
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
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.conn = None
        # self.psFile = 'failps'

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        appNameBody, appNameExt = os.path.splitext(self.appName)
        self.appNameBody = appNameBody
        self.appNameExt = appNameExt
        # if self.argc < 2:
        #     self.usage()
        # argvs = sys.argv[1:]
        # if self.argc > 1:
        #     self.psFile = sys.argv[1]

        # try:
        #     opts, arvs = getopt.getopt(argvs, "c:t:h:")
        # except getopt.GetoptError, e:
        #     print 'get opt error:%s. %s' % (argvs, e)
        #     self.usage()
        # self.cmd = 'q'
        # self.objType = 'h'
        # self.host = 'a'
        # for opt, arg in opts:
        #     if opt == '-c':
        #         self.cmd = arg
        #     elif opt == '-t':
        #         self.objType = arg
        #     elif opt == '-h':
        #         self.host = arg
        # if len(arvs) > 0:
        #     self.obj = arvs[0]

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

        self.today = time.strftime("%Y%m%d", time.localtime())
        self.nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        cfgName = '%s.cfg' % self.appNameBody
        logName = '%s_%s.log' % (self.appNameBody, self.today)
        logPre = '%s_%s' % (self.appNameBody, self.today)
        # outName = '%s_%s' % (self.appNameBody, self.nowtime)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        # self.wkFile = os.path.join(self.dirWork, self.psFile)
        # self.outFile = os.path.join(self.dirOut, self.psFile)

    def usage(self):
        print "Usage: %s datafile" % self.baseName
        print "example:  %s %s" % (self.baseName,'res*.txt')
        print "\t%s %s" % (self.baseName, 'res_to_kt_20180614.txt')
        print "\t%s %s" % (self.baseName, 'res_callback_to_kt_20180601.txt')
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
        netStatier = NetStati(self)
        netStatier.start()


# main here
if __name__ == '__main__':
    main = Main()
    main.start()
    logging.info('%s complete.', main.baseName)
