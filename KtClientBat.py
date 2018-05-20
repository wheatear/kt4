#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""kt client"""

import sys
import os
import time
import datetime
import getopt
import random
import re
import signal
import logging
import multiprocessing
import cx_Oracle as orcl
from socket import *

ls = os.linesep


#
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
        # self.month = time.strftime("%Y%m", time.localtime())
        self.month = '201709'

        self.dAsyncSendCur = {}
        self.dAsyncStatusCur = {}
        self.dAsyncTaskCur = {}

        self.dcommCur = {}
        self.curCaseDs = None
        self.curLoadSyncInfo = None
        self.curLoadAsyncInfo = None
        self.curOrderId = None
        self.curLoadRegion = None
        self.curInsCaseResult = None
        self.curUpdCaseResult = None

        self.curResultDs = None
        self.curCheckDs = None
        self.curInsertCaseResult = None
        self.curUpdateAsyncStatus = None
        self.curUpdateCompareResult = None
        self.curTask = None


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

    def getDsCur(self, sql):
        if self.caseDsCur is not None: return self.caseDsCur
        cur = self.prepareSql(sql)
        self.caseDsCur = cur
        return cur

    def getCurbyName(self, curName, sql):
        if self.__dict__[curName] is not None: return self.__dict__[curName]
        cur = self.prepareSql(sql)
        self.__dict__[curName] = cur
        return cur

    def getAsyncSendCur(self, curKey, sql):
        if curKey in self.dAsyncSendCur: return self.dAsyncSendCur[curKey]
        cur = self.prepareSql(sql)
        self.dAsyncSendCur[curKey] = cur
        return cur

    def getAsyncStatusCur(self, curKey, sql):
        logging.debug('get cursor for %s : %s' , curKey,sql)
        if curKey in self.dAsyncStatusCur:
            # logging.debug('cur %s', self.dAsyncStatusCur[curKey])
            return self.dAsyncStatusCur[curKey]
        logging.info('make cursor for %s', sql)
        cur = self.prepareSql(sql)
        self.dAsyncStatusCur[curKey] = cur
        return cur

    def getAsyncTaskCur(self, curKey, sql):
        logging.debug('get cursor for %s' , sql)
        if curKey in self.dAsyncTaskCur:
            logging.debug('%s', self.dAsyncTaskCur[curKey])
            return self.dAsyncTaskCur[curKey]
        cur = self.prepareSql(sql)
        self.dAsyncTaskCur[curKey] = cur
        return cur

    def closeAllCur(self):
        logging.info('close all opened cursor.')
        for cur in self.dCursors:
            logging.info('close cur: %s' % cur.statement)
            cur.close()

    def prepareSql(self, sql):
        logging.info('prepare sql: %s', sql)
        cur = self.conn.cursor()
        try:
            cur.prepare(sql)
        except orcl.DatabaseError, e:
            logging.error('prepare sql err: %s', sql)
            return None
        return cur

    def fetchone(self, cur):
        logging.debug('fethone from %s', cur.statement)
        try:
            row = cur.fetchone()
        except orcl.DatabaseError, e:
            logging.error('execute sql err %s:%s ', e, cur.statement)
            return None
        return row

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

    def executemanyCur(self, cur, params):
        logging.info('execute cur %s', cur.statement)
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

    # def openAsyncSendCur(self):
    #     logging.info('open %s asynchrous order send cursor.', self.ktName)
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         sql = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,:createDate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)' % (
    #             self.orderTablePre, region)
    #         cur = self.conn.cursor()
    #         cur.prepare(sql)
    #         self.dAsyncSendCur[region] = cur
    #     logging.info('make 10 cursors for 100-190 region.')
    #
    # def closeAsyncSendCur(self):
    #     logging.info('close %s asynchrous order send curser.', self.ktName)
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         cur = self.dAsyncSendCur[region]
    #         cur.close()
    #     logging.info('close 10 cursors for 100-190 region.')

    # def openAsyncStatusCur(self):
    #     logging.debug('open %s query asynchrous order status cursor' % ( self.ktName))
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (
    #             region, self.month)
    #         cur = self.conn.cursor()
    #         cur.prepare(sql)
    #         self.dAsyncStatusCur[region] = cur
    #     logging.info('make 10 query asynchrous order status cursors for 100-190 region.')
    #
    # def openBatAsyncStatusCur(self):
    #     logging.debug('open %s query asynchrous order status cursor' % ( self.ktName))
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         sql = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s_%s where create_date>=:firstDate and create_date<=:lastDate' % (
    #             region, self.month)
    #         cur = self.conn.cursor()
    #         cur.prepare(sql)
    #         cur.arraysize = 1000
    #         self.dAsyncStatusCur[region] = cur
    #     logging.info('make 10 query asynchrous order status cursors for 100-190 region.')

    # def closeAsyncStatusCur(self):
    #     logging.info('close %s asynchrous order status cursor.', self.ktName)
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         cur = self.dAsyncStatusCur[region]
    #         cur.close()
    #     logging.info('close 10 query asynchrous order status cursors for 100-190 region.')
    #
    # def openAsyncTaskCur(self):
    #     logging.debug('open %s query asynchrous task cursor' % ( self.ktName))
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         sql = 'select rownum,old_ps_id,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where old_ps_id=:psId and bill_id=:billId order by ps_id' % (
    #             region, self.month)
    #         cur = self.conn.cursor()
    #         cur.prepare(sql)
    #         self.dAsyncTaskCur[region] = cur
    #     logging.info('make 10 query asynchrous task cursors for 100-190 region.')
    #
    # def openBatAsyncTaskCur(self):
    #     logging.debug('open %s query asynchrous task cursor' % ( self.ktName))
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         sql = 'select rownum,old_ps_id,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where create_date>=:firstDate and create_date<=:lastDate order by old_ps_id,ps_id' % (
    #             region, self.month)
    #         cur = self.conn.cursor()
    #         cur.prepare(sql)
    #         cur.arraysize = 1000
    #         self.dAsyncTaskCur[region] = cur
    #     logging.info('make 10 query asynchrous task cursors for 100-190 region.')
    #
    # def closeAsyncTaskCur(self):
    #     logging.info('close %s asynchrous task cursor.', self.ktName)
    #     for i in range(100, 200, 10):
    #         region = str(i)
    #         cur = self.dAsyncTaskCur[region]
    #         cur.close()
    #     logging.info('close 10 query asynchrous task cursors for 100-190 region.')

    def connTcpServer(self):
        if self.tcpClt: return self.tcpClt
        self.tcpClt = TcpClt(self.syncServer, int(self.sockPort))

    def setOrderTable(self, tablePre):
        self.orderTablePre = tablePre

    def syncSendOne(self,reqMsg):
        # reqMsg = 'TRADE_ID=%d;ACTION_ID=%s;PS_SERVICE_TYPE=%s;MSISDN=%s;IMSI=%s;%s' % (
        # order.tradId, order.ktCase.actionId, order.ktCase.psServiceType, order.ktCase.billId, order.ktCase.subBillId,
        # order.ktCase.psParam)
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
        logging.info('receive %s synchrous orders response...', self.ktName)
        orderNum = self.ordGroup.orderNum
        regx = re.compile(r'RSP:TRADE_ID=(\d+);ERR_CODE=(.+?);ERR_DESC=(.*);(.*)')
        i = 0
        while i < orderNum:
            rspMsg = self.recvResp()
            m = regx.search(rspMsg)
            if m is not None:
                tridId = int(m.group(1))
                errCode = int(m.group(2))
                errDesc = m.group(3)
                resp = m.group(4)

                logging.debug('recv response order No.%d ,tradId %s.', i, tridId)
                # logging.debug('sync order keys: %s.', self.ordGroup.dOrders.keys())
                order = self.ordGroup.dOrders[tridId]
                order.syncStatus = errCode
                order.syncDesc = errDesc
                order.response = resp
                i += 1
                logging.debug('recv order response ,tradId:%s, syncStatus:%d, syncDesc:%s, response:%s.', tridId,
                              order.syncStatus, order.syncDesc, order.response)
                # order.response = rspMsg
                logging.debug('receive %d of %d orders', i, orderNum)
                # logging.debug('receive response %s',rspMsg)
        logging.info('receive %s synchrous orders %d.', self.ktName, i)

    def recvResp(self):
        rspMsg = self.tcpClt.recv()
        # logging.debug(rspMsg)
        return rspMsg

    # def getStsOne(self, psOrder):
    #     logging.debug('get %d status ' % (psOrder.psId))
    #     params = {'psId': psOrder.psId}
    #     sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (
    #     psOrder.regionCode, self.month)
    #     logging.debug('%s. %s' % (sql, params))
    #     cur = self.conn.cursor()
    #     try:
    #         cur.execute(sql, params)
    #         result = cur.fetchone()
    #     except Exception, e:
    #         logging.error('can not execute sql: %s :%s. %s', sql, params, e)
    #         cur.close()
    #         return 0
    #     if result:
    #         logging.debug('get ps_id:%d status %d', psOrder.psId, result[0])
    #         psOrder.psStatus = result[0]
    #         psOrder.failReason = result[1]
    #         cur.close()
    #         return 1
    #     else:
    #         cur.close()
    #         return 0

    # def getSts(self):
    #     if self.ordGroup.doneNum == self.ordGroup.all:
    #         return self.ordGroup.all
    #     aOrders = self.ordGroup.aOrders
    #     logging.info('get %s asynchrous orders status...', self.ktName)
    #     cur = self.conn.cursor()
    #     i = 0
    #     for order in aOrders:
    #         if order.psStatus > 0:
    #             i += 1
    #             continue
    #         # logging.debug('get ps_id:%d status ' % (order.psId))
    #         params = {'psId': order.psId}
    #         sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (
    #             order.regionCode, self.month)
    #         logging.debug('%s. %s' % (sql, params))
    #         try:
    #             cur.execute(sql, params)
    #             result = cur.fetchone()
    #         except Exception, e:
    #             logging.error('can not execute sql: %s :%s. %s', sql, params, e)
    #             continue
    #         if result:
    #             status = result[0]
    #             i += 1
    #             logging.debug('get ps_id:%d status:%d failreason:%s', order.psId, status, result[1])
    #             order.psStatus = status
    #             order.failReason = result[1]
    #             if status == 9:
    #                 self.ordGroup.sucNum += 1
    #             else:
    #                 self.ordGroup.failNum += 1
    #     cur.close()
    #     logging.info('get %d orders status.', i)
    #     self.ordGroup.doneNum = i
    #     return i

    def getTasks(self):
        aOrders = self.ordGroup.aOrders
        for order in aOrders:
            if len(order.aTask) > 0: continue
            self.getTasksOne(order)


class KtCase(object):
    """provisioning synchronous order case"""
    dCurSql = {}
    dCurSql['curResultDs'] = 'select caseno,casename,billId,createDate,regioncode_kt3,regioncode_kt4,psId_kt3,psId_kt4,psStatus_kt3,psStatus_kt4,failReason_kt3,failReason_kt4,tradId_kt3,tradId_kt4,errCode_kt3,errCode_kt4,errDesc_kt3,errDesc_kt4,resp_kt3,resp_kt4 from %s  where psstatus_kt3 is null or psstatus_kt4 is null or psstatus_kt3 =0 or psstatus_kt4 =0 order by createdate'
    dCurSql['curCheckDs'] = 'select caseno,casename,billId,createDate,regioncode_kt3,regioncode_kt4,psId_kt3,psId_kt4,psStatus_kt3,psStatus_kt4,failReason_kt3,failReason_kt4,tradId_kt3,tradId_kt4,errCode_kt3,errCode_kt4,errDesc_kt3,errDesc_kt4,resp_kt3,resp_kt4 from %s   order by createdate'
    # where    comparestatus is null or comparestatus > 0

    def __init__(self, caseNo, caseName, psServiceType, actionId, psParam, billId='', subBillId='', comment='', createdate=None):
        self.caseNo = caseNo
        self.psCaseName = caseName
        self.psServiceType = psServiceType
        self.billId = billId
        self.subBillId = subBillId
        self.actionId = actionId
        self.psParam = psParam
        self.comment = comment
        # self.createDate = createdate
        self.createDate = datetime.datetime.now()

        self.aCmdKey = []  # keys of commandes send to ne
        # self.taskCount = 0
        self.dOrder = {}
        self.compareStatus = None

        # ktclient_result
        self.psIdA = None
        self.psIdB = None
        self.psStatusA = None
        self.psStatusB = None
        self.failReasonA = None
        self.failReasonB = None
        self.tradIdA = None
        self.tradIdB = None
        self.errCodeA = None
        self.errCodeB = None
        self.errDescA = None
        self.errDescB = None
        self.respA = None
        self.respB = None

        self.dTableMap ={'caseno':'caseNo', 'casename':'psCaseName','billId':'billId','comparestatus':'compareStatus', 'createDate':'createDate'}

    def sendOrders(self):
        logging.debug('send orders of case %d %s', self.caseNo, self.psCaseName)
        logging.debug('timeer %f send orders %d %s', time.time(), self.caseNo, self.psCaseName)
        for orderKey in self.dOrder:
            logging.debug('timeer %f send order %d %s %s', time.time(), self.caseNo, self.psCaseName, orderKey)
            self.dOrder[orderKey].send()
        logging.debug('timeer %f send orders success %d %s', time.time(), self.caseNo, self.psCaseName)

    def qryOrderStatus(self):
        logging.debug('query status of case %d %s', self.caseNo, self.psCaseName)
        i = 0
        for orderKey in self.dOrder:
            i += self.dOrder[orderKey].qryStatus()
        return i

    def qryTask(self):
        logging.debug('query task of case %d %s', self.caseNo, self.psCaseName)
        for orderKey in self.dOrder:
            self.dOrder[orderKey].qryTask()

    def compareOrder(self):
        logging.debug('query task of case %d %s', self.caseNo, self.psCaseName)
        destOrder = None
        sourOrder = None
        for orderKey in self.dOrder:
            order = self.dOrder[orderKey]
            if order.ktClient.ktType == 'KTA':
                destOrder = order
            else:
                sourOrder = order
        self.compareStatus = destOrder.compare(sourOrder)

    def makeCaseResultParams(self, case):
        logging.debug('make sql params for case %s', case.psCaseName)
        fields = ''
        paramNames = ''
        values = {}
        for key in case.dTableMap:
            fields = '%s%s,' % (fields, case.dTableMap[key])
            paramNames = '%s:%s,' % (paramNames, key)
            values[key] = case.__dict__[key]
        for orderKey in case.dOrder:
            order = case.dOrder[orderKey]
            for key in order.dTableMap:
                attrName = order.dTableMap[key]
                fields = '%s%s,' % (fields, order.dTableMap[key])
                paramNames = '%s:%s,' % (paramNames, key)
                values[key] = order.__dict__[attrName]
        fldLen = len(fields) - 1
        prmLen = len(paramNames) - 1
        if fldLen > 0:
            fields = fields[:fldLen]
        if prmLen > 0:
            paramNames = paramNames[:prmLen]
        return (fields, paramNames, values)

    def makeCaseUpdateParams(self, case):
        pass

    def makeValues(self, afields):
        dValues = {}
        for key in self.dTableMap:
            logging.debug('key %s', key)
            if key in afields:
                attr = self.dTableMap[key]
                dValues[key] = self.__dict__[attr]
                continue
        for orderKey in self.dOrder:
            order = self.dOrder[orderKey]
            for keyMap in order.dTableMap:
                logging.debug('key %s', keyMap)
                if keyMap in afields:
                    attrName = order.dTableMap[keyMap]
                    dValues[keyMap] = order.__dict__[attrName]
        return dValues

    def insertCaseResult(self, cur, afields):
        logging.debug('insert case %d %s to table %s', self.caseNo, self.psCaseName, afields)
        dValues = self.makeValues(afields)
        logging.debug(dValues)
        try:
            cur.execute(None,dValues)
            logging.debug('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
            # cur.connection.commit()
        except orcl.DatabaseError, e:
            logging.fatal('Can not insert case: %s', e)
            logging.debug('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
            return e

    def updateCaseResult(self, cur, afields):
        logging.debug('update case %d %s to table %s', self.caseNo, self.psCaseName, afields)
        dValues = self.makeValues(afields)
        logging.debug(dValues)
        cur.execute(None, dValues)
        cur.connection.commit()

    def updateCaseCheckResult(self, cur, afields):
        logging.debug('update case %d %s compare status to table %s', self.caseNo, self.psCaseName, afields)
        dValues = self.makeValues(afields)
        logging.debug(dValues)
        cur.execute(None, dValues)
        cur.connection.commit()

class CaseGrp(KtCase):
    def __init__(self, size):
        self.size = size


class KtOrder(object):
    """provisioning synchronous order"""
    dCurSql = {}
    dCurSql['curOrderId'] = 'select SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL FROM (select 1 from all_objects where rownum <= %d)'
    dCurSql['curAsyncStatus'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s_%s where create_date>=:firstDate and create_date<=:lastDate'

    def __init__(self, ktcase, ktclient, mode):
        self.ktCase = ktcase
        self.ktClient = ktclient
        self.mode = mode
        self.psId = None
        self.doneCode = None
        self.createDate = None
        self.tradId = None
        self.regionCode = None
        self.psStatus = 0
        self.failReason = None
        self.syncStatus = 0
        self.syncDesc = None
        self.response = None
        self.orderTablePre = None
        self.month = None
        self.aTask = []

        self.ktName = self.ktClient.ktName
        tablePsId = 'psId_%s' % self.ktName
        tableRegionCode = 'regionCode_%s' % self.ktName
        tablePsStatus = 'psStatus_%s' % self.ktName
        tableFailReason = 'failReason_%s' % self.ktName
        tableTradId = 'tradId_%s' % self.ktName
        tableSyncStatus = 'errCode_%s' % self.ktName
        tableSyncDesc = 'errDesc_%s' % self.ktName
        tableSyncResp = 'resp_%s' % self.ktName
        self.dTableMap = {tablePsId:'psId', tableRegionCode:'regionCode', tablePsStatus:'psStatus', tableFailReason:'failReason', tableTradId:'tradId', tableSyncStatus:'syncStatus', tableSyncDesc:'syncDesc', tableSyncResp:'response'}

    def getSyncOrderInfo(self):
        cur = self.ktClient.conn.cursor()
        sql = "select SEQ_PS_ID.NEXTVAL from dual"
        cur.execute(sql)
        result = cur.fetchone()
        if result:
            self.tradId = result[0]
        cur.close()
        logging.debug('load tradId: %d', self.tradId)

    def getAsyncOrderInfo(self):
        self.orderTablePre = self.ktClient.orderTablePre
        params = {'billId': self.ktCase.billId}
        cur = self.ktClient.conn.cursor()
        regionCode = '100'
        if re.match(r'\d{11}$', self.ktCase.billId):
            sql = 'select region_code,SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL from ps_net_number_area where :billId between start_number and end_number'
            cur.execute(sql, params)
        else:
            sql = "select '100',SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL from dual"
            cur.execute(sql)
        result = cur.fetchone()
        if result:
            self.regionCode = result[0]
            self.psId = result[1]
            self.doneCode = result[2]
        else:
            sql2 = "select '100',SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL from dual"
            cur.execute(sql2)
            result = cur.fetchone()
            self.regionCode = random.randrange(100, 200, 10)
            self.psId = result[1]
            self.doneCode = result[2]
        logging.debug('load ps_id:%s done_code:%s regioncode:%s bill_id:%s', self.psId, self.doneCode, self.regionCode,
                      self.ktCase.billId)

    def loadOrderInfo(self):
        if self.mode.find('s') > -1:
            if self.tradId is not None:
                return
            logging.debug('load synchroud orders info.')
            self.getSyncOrderInfo()
        if self.mode.find('t') > -1:
            if self.psId is not None:
                return
            logging.debug('load asynchroud orders info.')
            self.getAsyncOrderInfo()

    def send(self):
        if self.mode.find('s') > -1:
            logging.debug('send synchrous order %s', self.ktCase.psCaseName)
            self.syncSend()
        if self.mode.find('t') > -1:
            logging.debug('send asynchrous order %s', self.ktCase.psCaseName)
            self.asyncSend()

    def syncSend(self):
        # reqMsg = 'TRADE_ID=%d;ACTION_ID=%s;PS_SERVICE_TYPE=%s;MSISDN=%s;IMSI=%s;%s' % (
        #     self.tradId, self.ktCase.actionId, self.ktCase.psServiceType, self.ktCase.billId,
        #     self.ktCase.subBillId,self.ktCase.psParam)
        reqMsg = self.ktCase.psParam
        logging.debug('send synchrous order name:%s', self.ktCase.psCaseName)
        logging.debug(reqMsg)
        self.ktClient.tcpClt.send(reqMsg)

    def asyncSend(self):
        params = {'psId': self.psId, 'doneCode': self.doneCode, 'psServiceType': self.ktCase.psServiceType,
                  'billId': self.ktCase.billId, 'subBillId': self.ktCase.subBillId,
                  'actionId': self.ktCase.actionId, 'psParam': self.ktCase.psParam, 'regionCode': self.regionCode, 'createDate':self.ktCase.createDate}
        # sql = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,sysdate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)' % (
        #     self.ktClient.orderTablePre, self.regionCode)
        try:
            logging.debug('timeer %f get cur %d', time.time(), self.psId, )
            cur = self.ktClient.dAsyncSendCur[self.regionCode]
            logging.debug('%s: %s' % (cur.statement, params))
            logging.debug('timeer %f execute cur %d', time.time(), self.psId, )
            cur.execute(None, params)
            logging.debug('timeer %f commit %d', time.time(), self.psId, )
            # cur.connection.commit()
            logging.debug('timeer %f done %d', time.time(), self.psId, )
            # logging.debug('bind values: %s' % (cur.bindvars))
            # cur.close()
        except Exception, e:
            logging.error('can not execute sql: %s : %s', cur.statement, e)

    def qryTask(self):
        if self.mode.find('t') > -1:
            self.qryAsyncTask()

    def qryAsyncTask(self):
        logging.debug('query %d task from %s' % (self.psId, self.ktName))
        sql = 'select rownum,old_ps_id,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where old_ps_id=:psId and bill_id=:billId order by ps_id' % (
            self.regionCode, self.ktClient.month)
        argus = {'psId': self.psId, 'billId': self.ktCase.billId}
        cur = self.ktClient.conn.cursor()
        try:
            cur.execute(sql, argus)
            rows = cur.fetchall()
        except Exception, e:
            logging.error('can not execute sql: %s :billid:%s psId:%s %s', sql, self.ktCase.billId, self.psId, e)
            cur.close()
            return 0
        i = 0
        for item in rows:
            task = KtTask(self.psId)
            task.setTask(item)
            logging.debug('load task: %d %d %d', self.psId, item[0], item[1])
            logging.debug('psId:%s subPsId:%s billid:%s failLog:%s', self.psId, item[0], self.ktCase.billId,
                          task.fail_log)
            # print 'psId:%s billid:%s failLog:%s' % (self.psId, self.billId, task.fail_log)
            self.aTask.append(task)
            i += 1
        cur.close()
        return i

    def qryStatus(self):
        logging.debug('order %s mode :%s', self.ktCase.psCaseName, self.mode)
        if self.mode.find('s') > -1:
            logging.debug('query synchrous order status %s', self.ktCase.psCaseName)
            return self.qrySyncStatus()
        if self.mode.find('t') > -1:
            logging.debug('query asynchrous order status %s', self.ktCase.psCaseName)
            return self.qryAsyncStatus()

    def qryAsyncStatus(self):
        logging.debug('query %d status from %s' % (self.psId, self.ktName))
        sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (
            self.regionCode, self.ktClient.month)
        params = {'psId': self.psId}
        logging.debug('%s. %s' % (sql, params))
        cur = self.ktClient.dAsyncStatusCur[self.regionCode]
        try:
            cur.execute(None, params)
            result = cur.fetchone()
        except Exception, e:
            logging.error('can not execute sql: %s :%s. %s', sql, params, e)
            return 0
        if result:
            logging.debug('get ps_id:%d status %d %s', self.psId, result[0], result[1])
            self.psStatus = result[0]
            self.failReason = result[1]
            return 1
        else:
            return 0

    def qrySyncStatus(self):
        logging.debug('query %s status from %s' % (self.psId, self.ktName))
        regx = re.compile(r'RSP:TRADE_ID=(.+?);ERR_CODE=(.+?);ERR_DESC=(.*);(.*)')
        rspMsg = self.ktClient.recvResp()
        logging.debug('recv: %s', rspMsg)
        m = regx.search(rspMsg)
        if m is not None:
            tridId = m.group(1)
            errCode = int(m.group(2))
            errDesc = m.group(3)
            resp = rspMsg

            logging.debug('recv response order ,tradId %s.',  tridId)
            # logging.debug('sync order keys: %s.', self.ordGroup.dOrders.keys())
            self.syncStatus = errCode
            self.syncDesc = errDesc
            self.response = resp

            logging.debug('recv order response ,tradId:%s, syncStatus:%d, syncDesc:%s, response:%s.', tridId,
                          self.syncStatus, self.syncDesc, self.response)
            # order.response = rspMsg
            return 1
        return 0


class CompareKtOrder(KtOrder):
    dCurSql = KtOrder.dCurSql

    def __init__(self,  ktcase, ktclient, mode):
        super(self.__class__, self).__init__(ktcase, ktclient, mode)
        self.asyncComparision = None
        self.syncComparision = None

    def compare(self, psOrder):
        if self.mode.find('s') > -1:
            logging.debug('compare synchrous order %s', self.ktCase.psCaseName)
            return self.compareSync(psOrder)
        if self.mode.find('t') > -1:
            logging.debug('compare asynchrous order %s', self.ktCase.psCaseName)
            return self.compareAsync(psOrder)

    def compareAsync(self,psOrder):
        logging.debug('compare asynchrous orders %s', self.ktClient.ktName)
        myTaskNum = len(self.aTask)
        cmpaTaskNum = len(psOrder.aTask)
        if myTaskNum != cmpaTaskNum or myTaskNum == 0:
            logging.debug('%s: %d', self.ktClient.ktName, myTaskNum)
            logging.debug('%s: %d', psOrder.ktClient.ktName, cmpaTaskNum)
            time.sleep(3)
            self.qryTask()
            # self.getTasks(self.conn,self.month)
            psOrder.qryTask()
            # psOrder.getTasks(psOrder.conn,psOrder.month)
            myTaskNum = len(self.aTask)
            cmpaTaskNum = len(psOrder.aTask)
            if myTaskNum != cmpaTaskNum:
                logging.debug('%s: %d' , self.ktClient.ktName, myTaskNum)
                logging.debug('%s: %d' ,psOrder.ktClient.ktName, cmpaTaskNum)
                self.asyncComparision = 1

                return self.asyncComparision  # 子工单数不一致
            if myTaskNum == 0:
                return self.compareAsyncOrder(psOrder)
        # logging.debug('compare asyncronous orders')
        for i in range(myTaskNum):
            myTask = self.aTask[i]
            cmpaTask = psOrder.aTask[i]
            cmpaCode = myTask.compare(cmpaTask)
            if cmpaCode > 0:
                self.asyncComparision = cmpaCode
                logging.debug('compare %d to %d result:%d', self.psId, psOrder.psId, cmpaCode)
                return cmpaCode
        if self.psStatus == -1:
            self.asyncComparision = 14
            logging.debug('compare %d to %d result:%d', self.psId, psOrder.psId, self.asyncComparision)
            return self.asyncComparision  #
        self.asyncComparision = 0
        logging.debug('compare %d to %d result:%d', self.psId, psOrder.psId, self.asyncComparision)
        return self.asyncComparision #

    def compareAsyncOrder(self,psOrder):
        logging.info('compare order:  %d : %d', self.psId, psOrder.psId)
        if self.psStatus != psOrder.psStatus:
            self.asyncComparision = 9
            logging.debug('psstatus:%d,%d : %d,%d', self.psId, self.psStatus, psOrder.psId, psOrder.psStatus)
            return 9 # ps_status 不一致
        if self.failReason != psOrder.failReason:
            logging.debug('psstatus:%d,%s : %d,%s', self.psId, self.failReason, psOrder.psId, psOrder.failReason)
            self.asyncComparision = 10
            return 10 #order fail_reason 不一致
        if self.psStatus == -1:
            self.asyncComparision = 14
        self.asyncComparision = 0
        logging.debug('compare %d to %d result:%d', self.psId, psOrder.psId, self.asyncComparision)
        return self.asyncComparision  #

    def compareSync(self,psOrder):
        response = re.sub(r'TRADE_ID=.+?;','TRADE_ID=99999999;', self.response)
        cmprResponse = re.sub(r'TRADE_ID=.+?;','TRADE_ID=99999999;', psOrder.response)
        if self.syncStatus != psOrder.syncStatus:
            self.syncComparision = 11 # sync errcode
            logging.debug('syncStatus:%d,%d : %d,%d.', self.tradId,self.syncStatus, psOrder.tradId, psOrder.syncStatus)
            return self.syncComparision
        if self.syncDesc != psOrder.syncDesc:
            self.syncComparision = 12 # sync desc
            logging.debug('syncDesc:%d,%s : %d,%s.', self.tradId, self.syncDesc, psOrder.tradId, psOrder.syncDesc)
            return self.syncComparision
        if self.response != psOrder.response:
            self.syncComparision = 13 # sync response
            logging.debug('response:%d,%s : %d,%s.', self.tradId, self.response, psOrder.tradId, psOrder.response)
            return self.syncComparision
        if self.syncStatus == -1:
            self.syncComparision = 15
        self.syncComparision = 0
        return self.syncComparision



class KtTask(object):
    'provisioning task sended to ne'

    def __init__(self, oldpsid):
        self.oldPsId = oldpsid

    def setTask(self, taskInfo):
        logging.debug('taskinfo: %s', taskInfo)
        self.taskNum = taskInfo[0]
        self.old_ps_id = taskInfo[1]
        self.ps_id = taskInfo[2]
        self.bill_id = taskInfo[3]
        self.sub_bill_id = taskInfo[4]
        self.action_id = taskInfo[5]
        self.ps_service_type = taskInfo[6]
        self.ps_param = taskInfo[7]
        self.ps_status = taskInfo[8]
        failReason = taskInfo[9]
        # failReason.replace('\r\n','')
        # failReason.replace('\r', '')
        # failReason.replace('\n', '')
        self.fail_reason = failReason
        failLog = taskInfo[10]
        # failLog.replace('\r\n','')
        # failLog.replace('\r', '')
        # failLog.replace('\n', '')
        self.fail_log = failLog
        self.ps_service_code = taskInfo[11]
        self.ps_net_code = taskInfo[12]

    def compare(self, task):
        logging.info('compare task:  %d %d %d : %d %d %d', self.oldPsId, self.taskNum, self.ps_id, task.oldPsId,
                     task.taskNum, task.ps_id)
        if self.action_id != task.action_id:
            return 2  # action_id 不一致
        if self.ps_param != task.ps_param:
            return 3  # ps_param 不一致
        if self.ps_service_type != task.ps_service_type:
            return 7  # ps_service_type 不一致
        # if self.ps_net_code != task.ps_net_code:
        #     return 6  # ps_net_code 不一致
        if self.ps_service_code != task.ps_service_code:
            return 5  # ps_service_code 不一致
        self.replaceVar()
        task.replaceVar()
        FailLog = str(self.fail_log)
        cmprFailLog = str(task.fail_log)
        if FailLog != cmprFailLog:
            logging.info('compare false: %d %d, %s', self.oldPsId, self.taskNum, FailLog)
            logging.info('compare false: %d %d, %s', task.oldPsId, task.taskNum, cmprFailLog)
            return 4  # fail_log 不一致
        if self.ps_status != task.ps_status:
            return 9  # ps_status 不一致
        if self.fail_reason != task.fail_reason:
            return 10  # fail_reason 不一致
        return 0  # 比对一致

    def replaceVar(self):
        failLog = str(self.fail_log)
        dReplaceVar = {r'<vol:MessageID>\d+</vol:MessageID>': '<vol:MessageID>999999</vol:MessageID>',
                       r'<serial>\d+</serial>': '<serial>99999999</serial>',
                       r'<time>\d+</time>': '<time>20990101010101</time>',
                       r'<reqTime>\d+</reqTime>': '<reqTime>20990101010101</reqTime>',
                       r'<sequence>\d+</sequence>': '<sequence>999999</sequence>',
                       r'<xsd:reqTime>\d+</xsd:reqTime>': '<xsd:reqTime>999999</xsd:reqTime>',
                       r'<xsd:sequence>\d+</xsd:sequence>': '<xsd:sequence>999999</xsd:sequence>',
                       r'<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">\d+</m:MessageID>': '<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">999999</m:MessageID>',
                       r'<serialNO>\d+</serialNO>': '<serialNO>999999</serialNO>',
                       r'<vol:MessageID>\d+</vol:MessageID>': '<vol:MessageID>99999999</vol:MessageID>',
                       r'<hss:CNTXID>\d+</hss:CNTXID>': '<hss:CNTXID>999</hss:CNTXID>',
                       r'<time>\d+</time>': '<time>209900101010101</time>',
                       r'<serial>\d+</serial>': '<serial>99999999</serial>',
                       r'<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">\d</m:MessageID>': '<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">999999</m:MessageID>',
                       r'<crm:tradeNo>\d+</crm:tradeNo>': '<crm:tradeNo>9999999999</crm:tradeNo>',
                       r'<crm:issueTime>\d+</crm:issueTime>': '<crm:issueTime>20990101010101</crm:issueTime>',
                       r'<ns1:time>\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}</ns1:time>': '<ns1:time>2099-01-09 01:01:01</ns1:time>',
                       r'<ns1:serial>\d+</ns1:serial>': '<ns1:serial>99999999</ns1:serial>',
                       r'<hlr:time>\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}</hlr:time>': '<hlr:time>2099-01-09 01:01:01</hlr:time>',
                       r'<hlr:serial>\d+</hlr:serial>': '<hlr:serial>99999999</hlr:serial>'}
        for varKey in dReplaceVar:
            if re.search(varKey, failLog):
                varValue = dReplaceVar[varKey]
                failLog = re.sub(varKey, varValue, failLog)
        self.fail_log = failLog


class KtBuilder(object):
    '''kt builder for case from file'''
    dCurSql = {}
    dCurSql['loadSyncInfoCur'] = 'select '

    def __init__(self, conf, caseds, ordermode):
        self.cfg = conf
        self.caseDsName = caseds
        # self.outFile = outfile
        self.orderMode = ordermode
        # self.caseDsType = casedstype
        self.aKtClient = []
        self.dKtClient = {}
        self.client = None
        self.all = 0
        self.caseNum = 0

        self.conn = None
        self.resultDsName = '%s_%s' % (self.caseDsName, 'result')
        # self.resultDsName = 'kt4.KTCLIENT_RESULT'
        self.caseDs = None
        self.resultDs = None
        self.checkDs = None

        self.curInsertCaseResult = None
        self.insertCaseResultFields = []
        self.curUpdateAsyncStatus = None
        self.updateAsyncStatusFields = []
        self.curUpdateSyncStatus = None
        self.updateSyncStatusFields = []
        self.curUpdateCompareResult = None
        self.updateCompareResultFields = []
        self.curTask = None
        self.qryTaskFields = []

    def openDs(self):
        if self.caseDs is not None: return self.caseDs
        try:
            self.caseDs = open(self.caseDsName, 'r')
        except IOError, e:
            logging.fatal('Can not open case file %s: %s', self.caseDsName, e)
            logging.fatal('exit.')
            exit()
        return self.caseDs

    def closeDs(self):
        if self.caseDs is not None:
            self.caseDs.close()
        logging.info('close case file.')

    def getConn(self):
        if self.conn is not None: return self.conn
        conn = None
        for cli in self.aKtClient:
            if cli.ktType == 'KTA':
                conn = cli.conn
                break
        self.conn = conn
        return self.conn

    def getClient(self):
        if self.client is not None: return self.client
        for cli in self.aKtClient:
            if cli.ktType == 'KTA':
                self.client = cli
                break
        return self.client

    def openResultDs(self):
        if self.resultDs is not None: return self.resultDs
        # resultSql = 'select caseno,casename,billId,createDate,regioncode_kt3,regioncode_kt4,psId_kt3,psId_kt4,psStatus_kt3,psStatus_kt4,failReason_kt3,failReason_kt4,tradId_kt3,tradId_kt4,errCode_kt3,errCode_kt4,errDesc_kt3,errDesc_kt4,resp_kt3,resp_kt4 from %s  where psstatus_kt3 is null or psstatus_kt4 is null or psstatus_kt3 =0 or psstatus_kt4 =0 order by createdate' % self.resultDsName
        resultSql = KtCase.dCurSql['curResultDs'] % self.resultDsName
        logging.info('load result case sql:%s', resultSql)
        client = self.getClient()
        cur = client.getCurbyName('curResultDs', resultSql)
        client.executeCur(cur)
        self.resultDs = cur
        return self.resultDs

    def closeResultDs(self):
        if self.resultDs is not None:
            self.resultDs.close()
        logging.info('close result case cursor.')

    def openCheckDs(self):
        if self.checkDs is not None: return self.checkDs
        # resultSql = 'select caseno,casename,billId,createDate,regioncode_kt3,regioncode_kt4,psId_kt3,psId_kt4,psStatus_kt3,psStatus_kt4,failReason_kt3,failReason_kt4,tradId_kt3,tradId_kt4,errCode_kt3,errCode_kt4,errDesc_kt3,errDesc_kt4,resp_kt3,resp_kt4 from %s  where comparestatus is null or comparestatus =0 order by createdate' % self.resultDsName
        resultSql = KtCase.dCurSql['curCheckDs'] % self.resultDsName
        logging.info('load check case sql:%s', resultSql)
        client = self.getClient()
        cur = client.getCurbyName('curCheckDs', resultSql)
        client.executeCur(cur)
        self.checkDs = cur
        return self.checkDs

    def closeCheckDs(self):
        if self.checkDs is not None:
            self.checkDs.close()
        logging.info('close checkDs cursor.')

    def makeInsertSqlFields(self, aFields):
        fields = ','.join(aFields)
        parms = ',:'.join(aFields)
        parms = ':%s' % parms
        return (fields, parms)

    def openInsertCaseResult(self):
        if self.curInsertCaseResult is not None: return self.curInsertCaseResult
        logging.info('open insert case result cur')
        self.insertCaseResultFields = ['caseno','casename','billId','createDate','regionCode_kt3','regionCode_kt4','psId_kt3','psId_kt4']
        fields, parms = self.makeInsertSqlFields(self.insertCaseResultFields)
        inserResltSql = 'insert into %s(%s) values(%s)' % (self.resultDsName, fields, parms)
        logging.info(inserResltSql)
        client = self.getClient()
        cur = client.getCurbyName('curInsertCaseResult', inserResltSql)
        self.curInsertCaseResult = cur
        return self.curInsertCaseResult

    def getTasksOne(self, psOrder):
        # self.aTask = []
        sql = 'select rownum,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where old_ps_id=:psId and bill_id=:billId order by ps_id' % (
            psOrder.regionCode, self.month)
        argus = {'psId': psOrder.psId, 'billId': psOrder.ktCase.billId}
        cur = self.conn.cursor()
        try:
            cur.execute(sql, argus)
            rows = cur.fetchall()
        except Exception, e:
            logging.error('can not execute sql: %s :billid:%s psId:%s %s', sql, psOrder.ktCase.billId, psOrder.psId, e)
            cur.close()
            return 0
        i = 0
        for item in rows:
            task = KtTask(psOrder.psId)
            task.setTask(item)
            logging.debug('%d %d %d', psOrder.psId, item[0], item[1])
            logging.debug('psId:%s subPsId:%s billid:%s failLog:%s', psOrder.psId, item[0], psOrder.ktCase.billId,
                          task.fail_log)
            # print 'psId:%s billid:%s failLog:%s' % (self.psId, self.billId, task.fail_log)
            psOrder.aTask.append(task)
            i += 1
        cur.close()
        return i

    def openUpdateAsyncStatus(self):
        if self.curUpdateAsyncStatus is not None: return self.curUpdateAsyncStatus
        logging.info('open update case status cur')
        self.updateAsyncStatusFields = ['caseno','casename','psStatus_kt3','psStatus_kt4','failReason_kt3','failReason_kt4']
        updateStatusSql = 'update %s set psStatus_kt3=:psStatus_kt3,psStatus_kt4=:psStatus_kt4,failReason_kt3=:failReason_kt3,failReason_kt4=:failReason_kt4 where casename=:casename and caseno=:caseno' % self.resultDsName
        logging.info(updateStatusSql)
        conn = self.getConn()
        cur = conn.cursor()
        try:
            cur.prepare(updateStatusSql)
        except Exception, e:
            logging.error('can not prepare caseSql: %s : %s', updateStatusSql, e)
            cur.close()
            return None
        self.curUpdateAsyncStatus = cur
        return self.curUpdateAsyncStatus

    def openUpdateSyncStatus(self):
        if self.curUpdateSyncStatus is not None: return self.curUpdateSyncStatus
        logging.info('open update case synchrous status cur')
        self.updateSyncStatusFields = ['caseno','casename','tradId_kt3','tradId_kt4','errCode_kt3','errCode_kt4','errDesc_kt3','errDesc_kt4','resp_kt3','resp_kt4']
        updateStatusSql = 'update %s set tradId_kt3=:tradId_kt3,tradId_kt4=:tradId_kt4,errCode_kt3=:errCode_kt3,errCode_kt4=:errCode_kt4,errDesc_kt3=:errDesc_kt3,errDesc_kt4=:errDesc_kt4,resp_kt3=:resp_kt3,resp_kt4=:resp_kt4 where casename=:casename and caseno=:caseno' % self.resultDsName
        logging.info(updateStatusSql)
        conn = self.getConn()
        cur = conn.cursor()
        try:
            cur.prepare(updateStatusSql)
        except Exception, e:
            logging.error('can not prepare caseSql: %s : %s', updateStatusSql, e)
            cur.close()
            return None
        self.curUpdateSyncStatus = cur
        return self.curUpdateSyncStatus

    def openUpdateCaseCheck(self):
        if self.curUpdateCompareResult is not None: return self.curUpdateCompareResult
        logging.info('open update case check status cur')
        self.updateCompareResultFields = ['caseno','casename','comparestatus']
        # fields, parms = self.makeSqlFields(self.updateCompareResultFields)
        updateCheckStatusSql = 'update %s set comparestatus=:comparestatus where casename=:casename and caseno=:caseno' % self.resultDsName
        logging.info(updateCheckStatusSql)
        conn = self.getConn()
        cur = conn.cursor()
        try:
            cur.prepare(updateCheckStatusSql)
        except Exception, e:
            logging.error('can not prepare caseSql: %s : %s', updateCheckStatusSql, e)
            cur.close()
            return None
        self.curUpdateCompareResult = cur
        return self.curUpdateCompareResult

    def loadNextRunningCase(self):
        logging.debug('load next running case.')
        if self.resultDs is None:
            self.openResultDs()
            # return None
        row = self.resultDs.fetchone()
        if row is None:
            return None
        logging.debug('load from table: %s', (row))
        caseNo = row[0]
        caseName = row[1]
        billId = row[2]
        regionCode_KT3 = row[3]
        regionCode_KT4 = row[4]
        psId_KT3 = row[5]
        psId_KT4 = row[6]
        tradId_KT3 = row[11]
        tradId_KT4 = row[12]
        case = KtCase(caseNo, caseName, None, None, None, billId)

        for cli in self.aKtClient:
            order = CompareKtOrder(case, cli, self.orderMode)
            if cli.ktName == 'kt3':
                order.psId = psId_KT3
                order.regionCode = regionCode_KT3
                order.tradId = tradId_KT3
            elif cli.ktName == 'kt4':
                order.psId = psId_KT4
                order.regionCode = regionCode_KT4
                order.tradId = tradId_KT4
            case.dOrder[cli.ktName] = order
            logging.debug('loaded order %s.', vars(order))
        logging.debug('load case: %s', vars(case))
        return case

    def buildKtClient(self):
        logging.info('build kt client.')
        self.aKtClient = self.cfg.loadClient()
        for cli in self.aKtClient:
            self.dKtClient[cli.ktType] = cli
            cli.connDb()
            cli.openAsyncSendCur()
            cli.openAsyncStatusCur()
            cli.openAsyncTaskCur()
            if self.orderMode.find('s') > -1:
                cli.connTcpServer()

    def commitAll(self):
        logging.info('all client commit.')
        for cli in self.aKtClient:
            logging.info('client %s commit.', cli.ktName)
            cli.conn.commit()

    def closeCaseCur(self):
        logging.info('close all the cursor about case')
        for cli in self.aKtClient:
            logging.info('close %s send asynchrous order cursor' % cli.ktName)
            # cli.closeAsyncSendCur()
            logging.info('close %s query asynchrous order status cursor' % cli.ktName)
            # cli.closeAsyncStatusCur()
            logging.info('close %s query asynchrous task cursor' % cli.ktName)
            # cli.closeAsyncTaskCur()

    def loadNext(self):
        if self.caseDs is None:
            logging.info('no open cursor')
            return None
        caseName = None
        comment = None
        cmdCount = None
        aCmd = []
        regionCode = None
        psServiceType = None
        billId = None
        subBillId = None
        actionId = None
        psParam = None
        for line in self.caseDs:
            line = line.strip()
            if len(line) == 0: continue
            logging.debug(line)
            if line[0] == '#':
                if len(line) < 3:
                    continue
                k2 = line[1]
                if k2 == '@':  # "#@" is mml command key  e.g. "#@Qry User:MSISDN=,Item=LOC"
                    aCmd.append(line[2:])
                elif k2 == '#':  # "##" is comment,ignore.
                    comment = line[2:]
                elif k2 == '%':  # "#%" is count of mml command
                    cmdCount = line[2:]
                elif k2 == '!':  # "#!" is request case name
                    caseName = line[2:]
                continue
            param = line.split(' ', 1)
            if param[0] == 'REGION_CODE':
                regionCode = param[1]
            elif param[0] == 'PS_SERVICE_TYPE':
                psServiceType = param[1]
            elif param[0] == 'BILL_ID':
                billId = param[1]
            elif param[0] == 'SUB_BILL_ID':
                subBillId = param[1]
            elif param[0] == 'ACTION_ID':
                actionId = param[1]
            elif param[0] == 'PS_PARAM':
                psParam = param[1]
            elif param[0] == '$END$':
                self.caseNum += 1
                if caseName is None:
                    caseName = 'case%d' % self.caseNum
                myCase = KtCase(self.caseNum, caseName, psServiceType, actionId, psParam, billId, subBillId)
                if comment: myCase.comment = comment
                if cmdCount: myCase.mmlCount = cmdCount
                if aCmd: myCase.aCmdKey = aCmd
                logging.debug('create case: %s', vars(myCase))
                return myCase
            elif param[0] == 'KT_REQUEST':
                continue
            else:
                logging.info('case file format error: %s', line)

    def loadOrder(self, case):
        logging.debug('load order of case %d %s', case.caseNo, case.psCaseName)
        for cli in self.aKtClient:
            order = CompareKtOrder(case, cli, self.orderMode)
            order.psId = case.psId
            order.doneCode = case.doneCode
            order.regionCode = case.regionCode
            order.loadOrderInfo()
            case.dOrder[cli.ktName] = order
            logging.debug('loaded order %s.', vars(order))

    def queryCaseStatus(self, case):
        logging.debug('query case %d %s status', case.caseNo, case.psCaseName)
        return case.qryOrderStatus()

    def insertCase(self, case):
        logging.debug('insert case %d %s', case.caseNo, case.psCaseName)
        insrt = case.insertCaseResult(self.curInsertCaseResult, self.insertCaseResultFields)
        if insrt.__str__().find('ORA-00942') > -1:
            logging.info('no result table, create the result table: %s', self.resultDsName)
            self.createResultTable()
            insrt = case.insertCaseResult(self.curInsertCaseResult, self.insertCaseResultFields)

    def createResultTable(self):
        create_table = """ 
                create table %s
(
  caseno        NUMBER(15),
  casename     VARCHAR2(64),
  billId       VARCHAR2(64),
  createDate   DATE,
  comparestatus  NUMBER(2),
  regioncode_kt3  VARCHAR2(6),
  regioncode_kt4  VARCHAR2(6),
  psId_kt3      NUMBER(15),
  psId_kt4       NUMBER(15),
  psStatus_kt3   NUMBER(2),
  psStatus_kt4   NUMBER(2),
  failReason_kt3 VARCHAR2(2000),
  failReason_kt4 VARCHAR2(2000),
  tradId_kt3     NUMBER(15),
  tradId_kt4     NUMBER(15),
  errCode_kt3    NUMBER(2),
  errCode_kt4    NUMBER(2),
  errDesc_kt3    VARCHAR2(2000),
  errDesc_kt4    VARCHAR2(2000),
  resp_kt3        VARCHAR2(4000),
  resp_kt4        VARCHAR2(4000)
)
                """
        createIndex = 'create index %s_CRE_IDX on %s (CREATEDATE)'
        createTableSql = create_table % self.resultDsName
        createIndexSql = createIndex % (self.resultDsName, self.resultDsName)
        logging.info(createTableSql)
        logging.info(createIndexSql)
        cur = self.getConn().cursor()
        try:
            cur.execute(createTableSql)
            cur.execute(createIndexSql)
        except orcl.DatabaseError,e:
            logging.error('can not execute Sql: %s : %s', cur.statement, e)

    def saveCaseStatus(self, case):
        logging.debug('save case %d %s status', case.caseNo, case.psCaseName)
        if self.orderMode.find('s') > -1:
            case.updateCaseResult(self.curUpdateSyncStatus, self.updateSyncStatusFields)
        if self.orderMode.find('t') > -1:
            case.updateCaseResult(self.curUpdateAsyncStatus, self.updateAsyncStatusFields)

    def sendCase(self, case):
        logging.debug('send case %s', case.psCaseName)
        case.sendOrders()

    def checkCase(self, case):
        logging.debug('check  case %d %s status', case.caseNo, case.psCaseName)
        case.qryTask()
        # logging.debug('compare case %d %s status', case.caseNo, case.psCaseName)
        case.compareOrder()

    def saveCaseCheck(self, case):
        logging.debug('save case %d %s check status', case.caseNo, case.psCaseName)
        if self.curUpdateCompareResult is None:
            self.openUpdateCaseCheck()
        case.updateCaseCheckResult(self.curUpdateCompareResult, self.updateCompareResultFields)

    def asyncSend(self):
        logging.info('send asynchronous orders...')
        for cli in self.aKtClient:
            logging.info('sending %s asynchronous orders', cli.ktName)
            cli.asyncSend()

    def syncSend(self):
        logging.info('send synchronous orders...')
        for cli in self.aKtClient:
            logging.info('sending %s synchronous orders', cli.ktName)
            cli.syncSend()

    def getSleepTime(self, donenum):
        unDone = self.all - donenum
        if unDone < 100:
            return unDone
        return unDone / len(str(unDone))

    def getOrderStatus(self):
        done_num = 0
        i = 0
        while done_num < self.all:
            sleepSecondes = self.getSleepTime(done_num)
            undone = self.all - done_num
            logging.info('all: %d , done: %d, undone: %d. sleep %d secondes.', self.all, done_num, undone,
                         sleepSecondes)
            logging.info('total %d asynchronous orders, sleep %d secondes.', self.all, sleepSecondes)
            time.sleep(sleepSecondes)
            done_num = 0
            i += 1
            for cli in self.aKtClient:
                logging.info('query status of %s asynchronous orders', cli.ktName)
                done = cli.getSts()
                done_num += done
            if i > 20:
                logging.info('query status %d times, exit.', i)
                break

    def getSyncResp(self):
        for cli in self.aKtClient:
            cli.syncRecv()

    def terminate(self, signum, frame):
        self.validate()
        self.writeResult()

    def validate(self):
        pass

    def writeResult(self):
        try:
            fOut = open(self.outFile, 'w')
        except IOError, e:
            logging.fatal('can not open out file %s: %s', self.outFile, e)
            return -1
        logging.info('write result of all orderes.')
        if self.orderMode.find('t') > -1:
            self.writeAsyncResult(fOut)
        if self.orderMode.find('s') > -1:
            self.writeSyncResult(fOut)

        self.writeFail(fOut)
        fOut.close()

    def writeAsyncResult(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s asynchronous orders result', cli.ktName)
            fOut.write('\n#%s asynchronous order\n' % cli.ktName)
            orderGrp = cli.ordGroup
            orderGrp.asyncFailNum = 0
            for ord in orderGrp.aOrders:
                if ord.psStatus == -1:
                    orderGrp.asyncFailNum += 1
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,"%s"\n' % (
                        ord.ktCase.psNo, ord.psId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName,
                        ord.ktCase.comment, ord.psStatus, ord.failReason))
                except IOError, e:
                    logging.error('can not write file: %s: %s', self.outFile, e)

    def writeSyncResult(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s synchronous orders result', cli.ktName)
            fOut.write('\n#%s synchronous order\n' % cli.ktName)
            orderGrp = cli.ordGroup
            orderGrp.syncFailNum = 0
            for ord in orderGrp.aOrders:
                if ord.syncStatus == -1:
                    orderGrp.syncFailNum += 1
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,"%s","%s"\n' % (
                        ord.ktCase.psNo, ord.tradId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName,
                        ord.ktCase.comment, ord.syncStatus, ord.syncDesc, ord.response))
                except IOError, e:
                    logging.error('can not write file: %s: %s', self.outFile, e)

    def writeFail(self, fOut):
        logging.info('write fail orders')
        if self.orderMode.find('t') > -1:
            self.writeAsyncFail(fOut)
        if self.orderMode.find('s') > -1:
            self.writeSyncFail(fOut)

    def writeAsyncFail(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s asynchronous fail orders', cli.ktName)
            orderGrp = cli.ordGroup
            fOut.write(
                '\n%s asynchronous orders all:%d  fial %d :\n' % (cli.ktName, orderGrp.orderNum, orderGrp.asyncFailNum))
            for ord in orderGrp.aOrders:
                if ord.psStatus == 9: continue
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,%s\n' % (
                        ord.ktCase.psNo, ord.psId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName,
                        ord.ktCase.comment, ord.psStatus, ord.failReason))
                except IOError, e:
                    logging.error('can not write file: %s: %s', self.outFile, e)

    def writeSyncFail(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s synchronous fail orders', cli.ktName)
            orderGrp = cli.ordGroup
            fOut.write(
                '\n%s synchronous orders all:%d  fial %d :\n' % (cli.ktName, orderGrp.orderNum, orderGrp.syncFailNum))
            for ord in orderGrp.aOrders:
                if ord.syncStatus == 9: continue
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,"%s","%s"\n' % (
                        ord.ktCase.psNo, ord.tradId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName,
                        ord.ktCase.comment, ord.syncStatus, ord.syncDesc, ord.response))
                except IOError, e:
                    logging.error('can not write file: %s: %s', self.outFile, e)


class CompareOrderGrp(CompareKtOrder):
    dCurSql = CompareKtOrder.dCurSql
    dCurSql['curAsyncSend'] = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,:createDate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)'
    # dCurSql['curAsyncSend'] = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,sysdate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)'

    dCurSql['curAsyncStatus'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s where create_date>=:firstDate and create_date<=:lastDate'
    # dCurSql['curAsyncStatus'] = 'select ps_id,ps_status,fail_reason from ps_provision_his_%s where ps_id=:psId'
    dCurSql['curAsyncTask'] = 'select rownum,old_ps_id,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s where create_date>=:firstDate and create_date<=:lastDate order by old_ps_id,ps_id'

    def __init__(self, casegrp, ktclient, mode):
        self.ktCase = casegrp
        self.ktClient = ktclient
        self.mode = mode
        self.size = casegrp.size
        self.ktName = self.ktClient.ktName
        self.aOrder = []
        self.dOrder = {}

    def loadOrderInfo(self):
        logging.debug('load sequence for ps_id and done_code.')
        # orderIdSql = CompareOrderGrp.dCurSql['curOrderId'] % self.size
        # cur = self.ktClient.getCurbyName('curOrderId', orderIdSql)
        # if self.size > 100:
        #     cur.arraysize = self.size
        # self.ktClient.executeCur(cur)
        # rows = self.ktClient.fetchmany(cur)
        for i in range(self.size):
            case = self.ktCase.aCase[i]
            order = CompareKtOrder(case, self.ktClient, self.mode)
            KtOrder.loadOrderInfo(order)
            # order.psId = rows[i][0]
            # # order.psId = case.psId
            # order.doneCode = case.doneCode
            # order.tradId = rows[i][0]
            # order.regionCode = case.regionCode
            order.createDate = case.createDate
            case.dOrder[self.ktClient.ktName] = order
            self.aOrder.append(order)
            self.dOrder[order.psId] = order

    def syncSend(self):
        logging.debug('send sync order')
        for order in self.aOrder:
            order.syncSend()

    def qrySyncStatus(self):
        logging.debug('query sync status %s', self.ktCase.psCaseName)
        i = 0
        for order in self.aOrder:
            i += order.qrySyncStatus()
        return i

    def compareSync(self, psOrder):
        logging.debug('compare sync ')
        for order in self.aOrder:
            order.compareSync(psOrder)

    def asyncSend(self):
        dParams = {'100':[],'110':[],'120':[],'130':[],'140':[],'150':[],'160':[],'170':[],'180':[],'190':[]}
        for order in self.aOrder:
            params = {'psId': order.psId, 'doneCode': order.doneCode, 'psServiceType': order.ktCase.psServiceType,
                  'billId': order.ktCase.billId, 'subBillId': order.ktCase.subBillId,
                  'actionId': order.ktCase.actionId, 'psParam': order.ktCase.psParam, 'regionCode': order.regionCode, 'createDate':order.ktCase.createDate}
            # params = {'psId': order.psId, 'doneCode': order.doneCode, 'psServiceType': order.ktCase.psServiceType,
            #       'billId': order.ktCase.billId, 'subBillId': order.ktCase.subBillId,
            #       'actionId': order.ktCase.actionId, 'psParam': order.ktCase.psParam, 'regionCode': order.regionCode, 'createDate':order.createDate}
            # params = {'psId': order.psId, 'doneCode': order.doneCode, 'psServiceType': order.ktCase.psServiceType,
            #           'billId': order.ktCase.billId, 'subBillId': order.ktCase.subBillId,
            #           'actionId': order.ktCase.actionId, 'psParam': order.ktCase.psParam,
            #           'regionCode': order.regionCode}

            dParams[order.regionCode].append(params)
        for region in dParams:
            aPa = dParams[region]
            num = len(aPa)
            if num == 0: continue
            try:
                logging.debug('timeer %f get cur %s %s', time.time(), self.ktCase.psCaseName, region)
                sql = CompareOrderGrp.dCurSql['curAsyncSend'] %(self.ktClient.orderTablePre, region)
                cur = self.ktClient.getAsyncSendCur(region, sql)
                logging.debug('%s: %d rows' % (cur.statement, num))
                logging.debug('timeer %f execute cur %s %s', time.time(), self.ktCase.psCaseName, region)
                self.ktClient.executemanyCur(cur, aPa)
                logging.debug('timeer %f commit %s %s', time.time(), self.ktCase.psCaseName, region)
                cur.connection.commit()
                logging.debug('timeer %f done %s %s', time.time(), self.ktCase.psCaseName, region)
            except Exception, e:
                logging.error('can not execute sql: %s : %s', cur.statement, e)

    def qryAsyncStatus(self):
        logging.debug('query %s order status from %s' % (self.ktCase.psCaseName, self.ktName))
        dParams = {}
        # dParams = {'100': {}, '110': {}, '120': {}, '130': {}, '140': {}, '150': {}, '160': {}, '170': {}, '180': {},
        #            '190': {}}
        for order in self.aOrder:
            # params = {'psId': order.psId}
            region = order.regionCode
            createDate = order.createDate
            psId = order.psId
            # tableMonth = createDate.strftime('%Y%m')
            tableMonth = time.strftime("%Y%m", time.localtime())
            paraKey = '%s_%s' % (region, tableMonth)
            if paraKey not in dParams:
                dParams[paraKey] = {'firstDate':createDate, 'lastDate':createDate}
                # dParams[paraKey] = {'psId': psId}
            else:
                dRegionPara = dParams[paraKey]
                if createDate < dRegionPara['firstDate']:
                    dRegionPara['firstDate'] = createDate
                elif createDate > dRegionPara['lastDate']:
                    dRegionPara['lastDate'] = createDate
        logging.debug('query para: %s', dParams)
        i = 0
        for regionMonth in dParams:
            dRegionPara = dParams[regionMonth]
            if len(dRegionPara) == 0:
                continue
            try:
                logging.debug('timeer %f get cur %s %s', time.time(), self.ktName, regionMonth)
                sql = CompareOrderGrp.dCurSql['curAsyncStatus'] % regionMonth
                logging.debug('query sql: %s: %s', sql, dRegionPara)
                logging.debug('now ok.')
                cur = self.ktClient.getAsyncStatusCur(regionMonth, sql)
                logging.debug('query status :%s: %s ,%s' % (cur.statement, cur.bindnames, dRegionPara))
                self.ktClient.executeCur(cur, dRegionPara)
                logging.debug('timeer %f fetch rows', time.time())
                result = self.ktClient.fetchmany(cur)
                rownum = len(result)
                logging.debug('timeer %f fetch %d rows.', time.time(), len(result))
                logging.debug('get %d status of %s %s %s', rownum, self.ktCase.psCaseName, self.ktName, regionMonth)
                while rownum > 0:
                    for row in result:
                        psId = row[0]
                        if psId in self.dOrder:
                            self.dOrder[psId].psStatus = row[1]
                            # fr = row[2].replace('{','')
                            # fr = fr.replace('}', '')
                            self.dOrder[psId].failReason = row[2]
                            logging.debug('order status : %s %s %s', psId, row[1], row[2])
                            i += 1
                    result = self.ktClient.fetchmany(cur)
                    rownum = len(result)
                    logging.debug('get %d status of %s %s %s', rownum, self.ktCase.psCaseName, self.ktName, regionMonth)

            except Exception, e:
                logging.error('can not execute sql:  %s', e)
                # logging.error('can not execute sql: %s:%s : %s', cur.statement, cur.bindvars, e)
                continue
            if rownum == 0:
                continue
        return i

    def qryAsyncTask(self):
        logging.debug('query %s task from %s' % (self.ktCase.psCaseName, self.ktName))
        dParams = {}
        # dParams = {'100': {}, '110': {}, '120': {}, '130': {}, '140': {}, '150': {}, '160': {}, '170': {}, '180': {},
        #            '190': {}}
        for order in self.aOrder:
            # params = {'psId': order.psId}
            region = order.regionCode
            createDate = order.createDate
            # tableMonth = createDate.strftime('%Y%m')
            tableMonth = time.strftime("%Y%m", time.localtime())
            paraKey = '%s_%s' % (region, tableMonth)
            if paraKey not in dParams:
                dParams[paraKey] = {'firstDate': createDate, 'lastDate': createDate}
            else:
                dRegionPara = dParams[paraKey]
                if createDate < dRegionPara['firstDate']:
                    dRegionPara['firstDate'] = createDate
                elif createDate > dRegionPara['lastDate']:
                    dRegionPara['lastDate'] = createDate
        logging.debug('query para: %s', dParams)
        i = 0
        for regionMonth in dParams:
            dRegionPara = dParams[regionMonth]
            if len(dRegionPara) == 0:
                continue
            try:
                logging.debug('timeer %f get cur %s %s', time.time(), self.ktName, regionMonth)
                sql = CompareOrderGrp.dCurSql['curAsyncTask'] % regionMonth
                cur = self.ktClient.getAsyncTaskCur(regionMonth, sql)
                logging.debug('query status :%s: %s ,%s' % (cur.statement, cur.bindnames, dRegionPara))
                self.ktClient.executeCur(cur, dRegionPara)
                logging.debug('timeer %f fetch rows', time.time())
                result = self.ktClient.fetchmany(cur)
                rownum = len(result)
                logging.debug('timeer %f fetch %d rows.', time.time(), rownum)
                logging.debug('get %d task of %s %s %s', rownum, self.ktCase.psCaseName, self.ktName, regionMonth)
                while rownum > 0:
                    for row in result:
                        psId = row[1]
                        if psId not in self.dOrder:
                            continue
                        task = KtTask(psId)
                        task.setTask(row)
                        self.dOrder[psId].aTask.append(task)
                        i += 1
                    result = self.ktClient.fetchmany(cur)
                    rownum = len(result)
                    logging.debug('get %d task of %s %s %s', rownum, self.ktCase.psCaseName, self.ktName, regionMonth)

            except Exception, e:
                logging.error('can not execute sql: %s:%s : %s', cur.statement, cur.bindvars, e)
                continue
            if rownum == 0:
                continue
        return i


class KtTableCase(KtCase):
    dCurSql = KtCase.dCurSql
    dCurSql['curCaseDs'] = "select rownum,ps_id,ps_service_type,action_id,ps_param,bill_id,sub_bill_id,ps_id as done_code,create_date,region_code from %s order by create_date,ps_id"

    def __init__(self, caseNo, caseName, psServiceType, actionId, psParam, billId='', subBillId='', comment='',createdate=None):
        super(self.__class__, self).__init__(caseNo, caseName, psServiceType, actionId, psParam, billId, subBillId, comment, createdate)
        self.psId = None
        self.doneCode = None
        self.tradId = None
        self.regionCode = None


class TableCaseGrp(KtTableCase):
    def __init__(self, size, num):
        self.size = size
        self.caseNo = num
        self.psCaseName = 'TCG_%d' % num
        self.aCase = []
        self.dOrder = {}
        self.compareStatus = []

    def setOriCase(self, rows):
        i = 0
        for row in rows:
            i += 1
            tableCase = KtTableCase(*row[:9])
            tableCase.psId = row[1]
            tableCase.doneCode = row[7]
            tableCase.regionCode = row[9]
            logging.debug('load case %d case: %s', i, vars(tableCase))
            self.aCase.append(tableCase)
        if len(self.aCase) > 0:
            self.dTableMap = self.aCase[0].dTableMap
        logging.debug('load case of %d cases ok.', i)

    def makeValues(self, afields):
        # logging.debug('make values: %s',afields)
        aValues = []
        for ca in self.aCase:
            dValues = {}
            for fi in afields:
                logging.debug('find %s value', fi)
                if fi in ca.dTableMap:
                    attr = ca.dTableMap[fi]
                    dValues[fi] = ca.__dict__[attr]
                    continue
                findOrderVal = 0
                for orderKey in ca.dOrder:
                    order = ca.dOrder[orderKey]
                    logging.debug(order.dTableMap)
                    if fi in order.dTableMap:
                        attr = order.dTableMap[fi]
                        dValues[fi] = order.__dict__[attr]
                        findOrderVal = 1
                        break
                if findOrderVal == 0:
                    logging.warn('field %s no find', fi)
            logging.debug('case parameter %s ', dValues)
            aValues.append(dValues)
        return aValues

    def insertCaseResult(self, cur, afields):
        logging.debug('insert case %d %s to table %s', self.caseNo, self.psCaseName, afields)
        aValues = self.makeValues(afields)
        logging.debug(aValues)
        try:
            cur.executemany(None,aValues)
            logging.debug('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
            cur.connection.commit()
        except orcl.DatabaseError, e:
            logging.fatal('Can not insert case: %s', e)
            logging.debug('%s %s %s', cur.statement, cur.bindnames(), cur.bindvars)
            return e

    def updateCaseResult(self, cur, afields):
        logging.debug('update case %d %s to table %s', self.caseNo, self.psCaseName, afields)
        aValues = self.makeValues(afields)
        logging.debug(aValues)
        logging.debug(cur.statement)
        logging.debug(cur.bindnames())
        cur.executemany(None, aValues)
        logging.debug(cur.bindvars)
        cur.connection.commit()

    def updateCaseCheckResult(self, cur, afields):
        logging.debug('update case %d %s compare status to table %s', self.caseNo, self.psCaseName, afields)
        aValues = self.makeValues(afields)
        logging.debug(aValues)
        cur.executemany(None, aValues)
        cur.connection.commit()

    def compareOrder(self):
        logging.info('compare case group')
        for ca in self.aCase:
            ca.compareOrder()


class KtTableBuilder(KtBuilder):
    '''kt builder for table case'''
    # define sql for cursor
    dCurSql = KtBuilder.dCurSql

    def __init__(self, conf, caseds, ordermode):
        KtBuilder.__init__(self, conf, caseds, ordermode)
        # self.psId = None
        # self.doneCode = None
        # self.tradId = None
        # self.regionCode = None

    def openDs(self):
        if self.caseDs is not None: return self.caseDs
        caseSql = KtTableCase.dCurSql['curCaseDs'] % self.caseDsName
        logging.info('opends sql:%s', caseSql)
        client = self.getClient()
        cur = client.getCurbyName('curCaseDs', caseSql)
        client.executeCur(cur)
        logging.debug('open ds,get cursor ok')
        self.caseDs = cur
        return self.caseDs

    def closeDs(self):
        if self.caseDs is not None:
            self.caseDs.close()
        logging.info('close db cursor.')

    def loadNext(self):
        if self.caseDs is None:
            logging.info('no open cursor')
            return None
        row = self.caseDs.fetchone()
        if row is None:
            return None
        logging.debug('load from table: %s', (row))
        self.caseNum += 1
        case = KtTableCase(*row[:9])
        case.psId = row[1]
        case.doneCode = row[7]
        case.regionCode = row[9]
        logging.debug('load case: %s', vars(case))
        return case


class BatTableBuilder(KtTableBuilder):
    def __init__(self, conf, caseds, ordermode):
        super(self.__class__, self).__init__(conf, caseds, ordermode)
        self.size = 1

    def openDs(self):
        super(self.__class__, self).openDs()
        self.caseDs.arraysize = self.size

    def openResultDs(self):
        super(self.__class__, self).openResultDs()
        self.resultDs.arraysize = self.size

    def openCheckDs(self):
        super(self.__class__, self).openCheckDs()
        self.checkDs.arraysize = self.size

    def buildKtClient(self):
        logging.info('build kt client.')
        self.aKtClient = self.cfg.loadClient()
        for cli in self.aKtClient:
            self.dKtClient[cli.ktType] = cli
            cli.connDb()
            # cli.openAsyncSendCur()
            # cli.openBatAsyncStatusCur()
            # cli.openBatAsyncTaskCur()
            if self.orderMode.find('s') > -1:
                cli.connTcpServer()

    def loadNext(self):
        if self.caseDs is None:
            logging.info('no open cursor')
            self.openDs()
        logging.info('load next case')
        rows = self.caseDs.fetchmany()
        if len(rows) == 0:
            return None
        self.caseNum += 1
        logging.debug('load %d cases from table', (len(rows)))
        case = TableCaseGrp(len(rows), self.caseNum)
        case.setOriCase(rows)
        logging.debug('load next case group ok.')
        return case

    def loadNextRunningCase(self):
        logging.debug('load next running case.')
        if self.resultDs is None:
            self.openResultDs()
            # return None
        rows = self.resultDs.fetchmany()
        logging.debug('rows: %s', rows)
        if len(rows) == 0:
            return None
        self.caseNum += 1
        caseGrp = TableCaseGrp(self.size, self.caseNum)
        self.setRuningCase(caseGrp, rows)
        logging.debug('load next runing case: %s', vars(caseGrp))
        return caseGrp

    def loadNextCheckCase(self):
        logging.debug('load next check case.')
        if self.checkDs is None:
            self.openCheckDs()
            # return None
        rows = self.checkDs.fetchmany()
        logging.debug('rows: %s', rows)
        if len(rows) == 0:
            return None
        self.caseNum += 1
        caseGrp = TableCaseGrp(self.size, self.caseNum)
        self.setRuningCase(caseGrp, rows)
        logging.debug('load next runing case: %s', vars(caseGrp))
        return caseGrp

    def setRuningCase(self, casegrp, rows):
        logging.debug('load %d rows from table.', len(rows))
        for cli in self.aKtClient:
            orderGrp = CompareOrderGrp(casegrp, cli, self.orderMode)
            casegrp.dOrder[cli.ktName] = orderGrp
            # logging.debug('loaded order %s.', vars(orderGrp))

        i = 0
        for row in rows:
            i += 1
            caseNo = row[0]
            caseName = row[1]
            billId = row[2]
            createDate = row[3]
            regionCode_KT3 = row[4]
            regionCode_KT4 = row[5]
            psId_KT3 = row[6]
            psId_KT4 = row[7]
            tradId_KT3 = row[12]
            tradId_KT4 = row[13]
            case = KtCase(caseNo, caseName, None, None, None, billId)
            for cli in self.aKtClient:
                order = CompareKtOrder(case, cli, self.orderMode)
                order.createDate = createDate
                if cli.ktName == 'kt3':
                    order.psId = psId_KT3
                    order.regionCode = regionCode_KT3
                    order.tradId = tradId_KT3
                elif cli.ktName == 'kt4':
                    order.psId = psId_KT4
                    order.regionCode = regionCode_KT4
                    order.tradId = tradId_KT4
                case.dOrder[cli.ktName] = order
                casegrp.dOrder[cli.ktName].aOrder.append(order)
                casegrp.dOrder[cli.ktName].dOrder[order.psId] = order
            casegrp.aCase.append(case)
        if len(casegrp.aCase) > 0:
            casegrp.dTableMap = casegrp.aCase[0].dTableMap

        logging.debug('load %d casegrp', i)
        return casegrp

    def loadOrder(self, casegrp):
        logging.debug('load order of case %d %s', casegrp.caseNo, casegrp.psCaseName)
        for cli in self.aKtClient:
            orderGrp = CompareOrderGrp(casegrp, cli, self.orderMode)
            orderGrp.loadOrderInfo()
            casegrp.dOrder[cli.ktName] = orderGrp
            logging.debug('loaded order %s.', vars(orderGrp))

    def insertCase(self, case):
        logging.debug('insert case group %d %s', case.caseNo, case.psCaseName)
        insrt = case.insertCaseResult(self.curInsertCaseResult, self.insertCaseResultFields)
        if insrt.__str__().find('ORA-00942') > -1:
            logging.info('no result table, create the result table: %s', self.resultDsName)
            self.createResultTable()
            insrt = case.insertCaseResult(self.curInsertCaseResult, self.insertCaseResultFields)


class Director(object):
    def __init__(self, builder, processFlow):
        self.builder = builder
        self.processFlow = processFlow
        self.shutDown = False

    def start(self):
        self.builder.buildKtClient()
        self.builder.getClient()
        if self.processFlow == 's':
            self.doCase()
        elif self.processFlow == 'q':
            self.queryCaseStatus()
        elif self.processFlow == 'c':
            self.checkCaseStatus()
        elif self.processFlow == 'qc':
            self.sendCheckCaseStatus()
        self.builder.closeCaseCur()

    def doCase(self):
        logging.info('process all cases...')
        self.builder.openDs()
        self.builder.openInsertCaseResult()
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load case', time.time())
            case = self.builder.loadNext()
            if case is None:
                logging.info('load all cases,')
                break
            i += 1
            logging.debug('timeer %f load order %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.loadOrder(case)
            logging.debug('timeer %f insert case %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.insertCase(case)
            logging.debug('timeer %f send case %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.sendCase(case)
            logging.debug('timeer %f done case %d %s', time.time(), case.caseNo, case.psCaseName)
        self.builder.commitAll()
        self.builder.closeDs()

    def queryCaseStatus(self):
        logging.info('query case status...')
        self.builder.openResultDs()
        self.builder.openUpdateAsyncStatus()
        self.builder.caseNum = 0
        while not self.shutDown:
            logging.debug('query timeer %f load case', time.time())
            case = self.builder.loadNextRunningCase()
            if case is None:
                logging.info('check all cases,')
                break
            logging.debug('query timeer %f query case %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.queryCaseStatus(case)
            logging.debug('query timeer %f save case status %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.saveCaseStatus(case)
            logging.debug('query timeer %f done case %d %s', time.time(), case.caseNo, case.psCaseName)

    def checkCaseStatus(self):
        logging.info('query case status...')
        self.builder.openResultDs()
        self.builder.openUpdateAsyncStatus()
        self.builder.caseNum = 0
        while not self.shutDown:
            case = self.builder.loadNextCheckCase()
            if case is None:
                logging.info('check all cases,')
                break
            logging.info('query case status...')
            self.builder.queryCaseStatus(case)
            logging.info('save case status...')
            self.builder.saveCaseStatus(case)
            logging.info('check case ...')
            self.builder.checkCase(case)
            logging.info('save case check result ...')
            self.builder.saveCaseCheck(case)

    def sendCheckCaseStatus(self):
        logging.info('process all cases...')
        self.builder.openDs()
        self.builder.openInsertCaseResult()
        self.builder.openResultDs()
        self.builder.openUpdateAsyncStatus()
        self.builder.openUpdateSyncStatus()
        self.builder.caseNum = 0
        i = 0
        while not self.shutDown:
            logging.debug('timeer %f load case', time.time())
            case = self.builder.loadNext()
            if case is None:
                logging.info('load all cases,')
                break
            i += 1
            logging.debug('timeer %f load order %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.loadOrder(case)
            logging.debug('timeer %f insert case %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.insertCase(case)
            logging.debug('timeer %f send case %d %s', time.time(), case.caseNo, case.psCaseName)
            self.builder.sendCase(case)
            logging.debug('timeer %f done case %d %s', time.time(), case.caseNo, case.psCaseName)
            logging.info('query case status...')
            self.builder.queryCaseStatus(case)
            logging.info('save case status...')
            self.builder.saveCaseStatus(case)
            logging.info('check case ...')
            self.builder.checkCase(case)
            logging.info('save case check result ...')
            self.builder.saveCaseCheck(case)
        self.builder.commitAll()
        self.builder.closeDs()


class SyncBuilder(KtBuilder):
    pass


class AsyncBuilder(KtBuilder):
    pass


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
            exit()
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
                 reqTpl="TRADE_ID=5352420;ACTION_ID=1;DISP_SUB=4;PS_SERVICE_TYPE=HLR;MSISDN=%s;IMSI=%s;"):
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


class Main(object):
    'main'

    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        self.cfgFile = '%s.cfg' % self.Name

    def checkArgv(self):
        if self.argc < 2:
            print 'need case file.'
            self.usage()
        self.checkopt()
        self.caseDs = self.argvs[0]
        self.logFile = '%s%s' % (self.caseDs, '.log')
        # self.outFile = '%s%s' % (self.caseDs, '.csv')

    def checkopt(self):
        argvs = sys.argv[1:]
        orderMode = ''
        try:
            opts, arvs = getopt.getopt(argvs, "d:m:p:")
            # print '%s %s' % (opts,arvs)
        except getopt.GetoptError, e:
            orderMode = 't'
            print 'get opt error:%s. %s' % (argvs,e)
            self.usage()
        orderMode = 't'
        caseDsType = 't'
        processFlow = 's'
        for opt, arg in opts:
            # print 'opt: %s' % opt
            if opt == '-d':
                caseDsType = arg
            if opt == '-m':
                orderMode = arg
            if opt == '-p':
                processFlow = arg
        self.orderMode = orderMode
        self.caseDsType = caseDsType
        self.processFlow = processFlow
        self.argvs = arvs

    def start(self):
        self.cfg = Conf(self.cfgFile)
        self.cfg.loadLogLevel()
        self.logLevel = self.cfg.logLevel
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%H%M%S')
        logging.info('%s starting...' % self.baseName)
        logging.info('order mode: %s.' % self.orderMode)
        # self.checkopt()
        if self.caseDsType == 'f':
            builder = KtBuilder(self.cfg, self.caseDs, self.orderMode)
        if self.caseDsType == 't':
            builder = BatTableBuilder(self.cfg, self.caseDs, self.orderMode)
            # builder = KtTableBuilder(self.cfg, self.caseDs, self.orderMode)
        # builder.orderMode = self.orderMode
        director = Director(builder, self.processFlow)
        director.start()
        # builder.startProc()

    def usage(self):
        print "Usage: %s -d f|t -m s|t|st -p s|q|c casefile" % self.baseName
        print "option:"
        print "   -d: case data source: default t"
        print "                  f: case file "
        print "                  t: case table;"
        print "   -m: order mode: default t"
        print "                   s: synchrous orders(send to socket server);"
        print "                   t: asynchrous orders(send to database table);"
        print "   -p: process ;   default s"
        print "                   s : send case orders and query status;"
        print "                   q : only query status;"
        print "                   c : query and check orders status;"
        print "   case file format: Request Message of kt4"
        exit(1)


# main here
if __name__ == '__main__':
    main = Main()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
