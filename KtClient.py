#!/usr/bin/env python

"""kt client"""

import sys
import os
import time
import getopt
import random
import re
import signal
import logging
import multiprocessing
import cx_Oracle as orcl
from socket import *

ls = os.linesep

# ???????
class KtClient(object):
    def __init__(self):
        # self.cfgFile = cfgfile
        self.ktName = None
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

        self.caseGroup = None
        self.ordGroup = None

    def connDb(self):
        if self.conn: return self.conn
        try:
            connstr = '%s/%s@%s/%s' % (self.dbUser, self.dbPwd, self.dbHost, self.dbSid)
            self.conn = orcl.Connection(connstr)
            # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
        except Exception, e:
            logging.fatal( 'could not connect to oracle(%s:%s/%s), %s' , self.dbHost, self.dbUser, self.dbSid, e)
            exit()
        return self.conn

    def connTcpServer(self):
        if self.tcpClt: return self.tcpClt
        self.tcpClt = TcpClt(self.syncServer,int(self.sockPort))

    def setOrderTable(self, tablePre):
        self.orderTablePre = tablePre

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
        while i <  orderNum:
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
                logging.debug('recv order response ,tradId:%s, syncStatus:%d, syncDesc:%s, response:%s.',  tridId,order.syncStatus,order.syncDesc,order.response)
            # order.response = rspMsg
                logging.debug('receive %d of %d orders', i, orderNum)
            # logging.debug('receive response %s',rspMsg)
        logging.info('receive %s synchrous orders %d.',self.ktName, i)

    def syncSendOne(self,order):
        reqMsg = 'TRADE_ID=%d;ACTION_ID=%s;PS_SERVICE_TYPE=%s;MSISDN=%s;IMSI=%s;%s' % (
        order.tradId, order.ktCase.actionId, order.ktCase.psServiceType, order.ktCase.billId, order.ktCase.subBillId,
        order.ktCase.psParam)
        logging.debug('send order num:%d , order name:%s', order.ktCase.psNo, order.ktCase.psCaseName)
        logging.debug(reqMsg)
        self.tcpClt.send(reqMsg)
        # rspMsg = self.recvResp()
        # order.response = rspMsg
        # logging.debug('%s has response %s',order.ktCase.psCaseName, order.response)

    def recvResp(self):
        rspMsg = self.tcpClt.recv()
        # logging.debug(rspMsg)
        return rspMsg
        # aRspInfo = rspMsg.split(';')
        #
        # subInfo = ';'.join(aRspInfo[1:3])
        # if self.outPa:
        #     outPara = ['MSISDN1', 'IMSI1'] + self.outPa
        #     for para in outPara:
        #         key = '%s%s' % (para, '=')
        #         for val in aRspInfo:
        #             if key in val:
        #                 subInfo = '%s;%s' % (subInfo, val)
        #                 break
        # else:
        #     subInfo = ';'.join(aRspInfo[1:])
        # return subInfo

    def asyncSendOne(self,psOrder):
        params = {'psId': psOrder.psId, 'doneCode': psOrder.doneCode, 'psServiceType': psOrder.ktCase.psServiceType,
                  'billId': psOrder.ktCase.billId, 'subBillId': psOrder.ktCase.subBillId, 'actionId': psOrder.ktCase.actionId,
                  'psParam': psOrder.ktCase.psParam, 'regionCode': psOrder.regionCode}
        sql = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,sysdate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)' % (
        self.orderTablePre, psOrder.regionCode)
        logging.debug( '%s: %s' % (sql, params))
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            cur.connection.commit()
            cur.close()
        except Exception, e:
            logging.error( 'can not execute sql: %s : %s' , sql, e)

    def asyncSend(self):
        logging.info('send asyncronous orders to %s', self.ktName)
        i = 0
        cur = self.conn.cursor()
        for psOrder in self.ordGroup.aOrders:
            params = {'psId': psOrder.psId, 'doneCode': psOrder.doneCode, 'psServiceType': psOrder.ktCase.psServiceType,
                  'billId': psOrder.ktCase.billId, 'subBillId': psOrder.ktCase.subBillId, 'actionId': psOrder.ktCase.actionId,
                  'psParam': psOrder.ktCase.psParam, 'regionCode': psOrder.regionCode}
            sql = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,sysdate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)' % (
                self.orderTablePre, psOrder.regionCode)
            logging.debug( '%s: %s' % (sql, params))
            try:
                cur.execute(sql, params)
                cur.connection.commit()
                i += 1
            except Exception, e:
                logging.error( 'can not execute sql: %s : %s' , sql, e)
        cur.close()
        logging.info('insert %d orders', i)

    def getStsOne(self,psOrder):
        logging.debug( 'get %d status ' % (psOrder.psId))
        params = {'psId': psOrder.psId}
        sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (psOrder.regionCode, self.month)
        logging.debug('%s. %s' % (sql,params))
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            result = cur.fetchone()
        except Exception, e:
            logging.error( 'can not execute sql: %s :%s. %s' , sql, params, e)
            cur.close()
            return 0
        if result:
            logging.debug( 'get ps_id:%d status %d' , psOrder.psId, result[0])
            psOrder.psStatus = result[0]
            psOrder.failReason = result[1]
            cur.close()
            return 1
        else:
            cur.close()
            return 0

    def getSts(self):
        if self.ordGroup.doneNum == self.ordGroup.all:
            return self.ordGroup.all
        aOrders = self.ordGroup.aOrders
        logging.info('get %s asynchrous orders status...', self.ktName)
        cur = self.conn.cursor()
        i = 0
        for order in aOrders:
            if order.psStatus > 0:
                i += 1
                continue
            # logging.debug('get ps_id:%d status ' % (order.psId))
            params = {'psId': order.psId}
            sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (
            order.regionCode, self.month)
            logging.debug('%s. %s' % (sql, params))
            try:
                cur.execute(sql, params)
                result = cur.fetchone()
            except Exception, e:
                logging.error('can not execute sql: %s :%s. %s', sql, params, e)
                continue
            if result:
                status = result[0]
                i += 1
                logging.debug('get ps_id:%d status:%d failreason:%s', order.psId, status, result[1])
                order.psStatus = status
                order.failReason = result[1]
                if status == 9:
                    self.ordGroup.sucNum += 1
                else:
                    self.ordGroup.failNum += 1
        cur.close()
        logging.info('get %d orders status.', i)
        self.ordGroup.doneNum = i
        return i

    def getTasks(self):
        aOrders = self.ordGroup.aOrders
        for order in aOrders:
            if len(order.aTask) > 0: continue
            self.getTasksOne(order)

    def getTasksOne(self,psOrder):
        # self.aTask = []
        sql = 'select rownum,ps_id,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where old_ps_id=:psId and bill_id=:billId order by ps_id' % (
        psOrder.regionCode, self.month)
        argus = {'psId': psOrder.psId, 'billId': psOrder.ktCase.billId}
        cur = self.conn.cursor()
        try:
            cur.execute(sql, argus)
            rows = cur.fetchall()
        except Exception, e:
            logging.error( 'can not execute sql: %s :billid:%s psId:%s %s' , sql, psOrder.ktCase.billId, psOrder.psId, e)
            cur.close()
            return 0
        i = 0
        for item in rows:
            task = KtTask(psOrder.psId)
            task.setTask(item)
            logging.debug( '%d %d %d', psOrder.psId, item[0],item[1])
            logging.debug('psId:%s subPsId:%s billid:%s failLog:%s' , psOrder.psId, item[0], psOrder.ktCase.billId, task.fail_log)
            # print 'psId:%s billid:%s failLog:%s' % (self.psId, self.billId, task.fail_log)
            psOrder.aTask.append(task)
            i += 1
        cur.close()
        return i

    def validate(self,psOrder):
        pass


class KtCase(object):
    """provisioning synchronous order case"""

    def __init__(self, caseName, psServiceType, actionId, psParam, billId='', subBillId=''):
        self.psCaseName = caseName
        self.psServiceType = psServiceType
        self.billId = billId
        self.subBillId = subBillId
        self.actionId = actionId
        self.psParam = psParam

        self.aCmdKey = []  # keys of commandes send to ne
        # self.taskCount = 0
        self.comment = None

class CaseGrp(KtCase):
    def __init__(self, casefile):
        self.caseFile = casefile
        self.aCases = []
        self.caseNum = 0
        self.fCase = None

    def openCaseFile(self):
        if self.fCase is not None: return self.fCase
        try:
            self.fCase = open(self.caseFile, 'r')
        except IOError, e:
            logging.fatal( 'Can not open case file %s: %s' , self.caseFile, e)
            logging.fatal('exit.')
            exit()
        return self.fCase

    def closeCaseFile(self):
        self.fCase.close()
        logging.info('close case file.')

    def addCase(self,case):
        self.aCases.append(case)

    def buildCaseGrp(self):
        logging.info('create case group.')
        self.openCaseFile()
        case = self.getNextCase()
        while case:
            self.aCases.append(case)
            case = self.getNextCase()
        self.closeCaseFile()
        logging.info('created %d cases.', self.caseNum)

    def getNextCase(self):
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
        for line in self.fCase:
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
                myCase = KtCase(caseName, psServiceType, actionId, psParam, billId, subBillId)
                myCase.psNo = self.caseNum
                if comment: myCase.comment = comment
                if cmdCount: myCase.mmlCount = cmdCount
                if aCmd: myCase.aCmdKey = aCmd
                logging.debug('load case: %s', vars(myCase))
                return myCase
            elif param[0] == 'KT_REQUEST':
                continue
            else:
                logging.info('case file format error: %s', line)


class KtOrder(object):
    """provisioning synchronous order"""
    def __init__(self, ktcase):
        self.ktCase = ktcase
        self.psId = None
        self.doneCode = None
        self.tradId = None
        self.regionCode = None
        self.psStatus = 0
        self.failReason = None
        self.syncStatus = 0
        self.syncDesc = None
        self.response = None
        self.mode = None
        # self.sendTablePre = 'i_provision'
        self.aTask = []

    def getSyncOrderInfo(self,conn):
        cur = conn.cursor()
        sql = "select SEQ_PS_ID.NEXTVAL from dual"
        cur.execute(sql)
        result = cur.fetchone()
        if result:
            self.tradId = result[0]
        cur.close()
        logging.debug('load tradId: %d', self.tradId)

    def getAsyncOrderInfo(self, conn):
        params = {'billId': self.ktCase.billId}
        cur = conn.cursor()
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

    def getOrderInfo(self,conn):
        if self.mode.find('s') > -1:
            logging.debug('load %d %s synchroud orders info.', self.ktCase.psNo, self.ktCase.psCaseName)
            self.getSyncOrderInfo(conn)
        if self.mode.find('t') > -1:
            logging.debug('load %d %s asynchroud orders info.', self.ktCase.psNo, self.ktCase.psCaseName)
            self.getAsyncOrderInfo(conn)

class OrderGrp(KtOrder):
    def __init__(self, casegrp, client, ordermode):
        self.caseGrp = casegrp
        self.client = client
        # self.dClient = {}
        self.aOrders = []
        self.dOrders = {}
        self.orderNum = casegrp.caseNum
        self.doneNum = 0
        self.all = 0
        self.sucNum = 0
        self.failNum = 0
        self.syncFailNum = 0
        self.orderMode = ordermode

    def buildOrderGrp(self):
        logging.info('build %s orders.',self.client.ktName)
        i = 0
        for case in self.caseGrp.aCases:
            order = KtOrder(case)
            order.mode = self.orderMode
            order.client = self.client
            order.getOrderInfo(self.client.conn)
            self.aOrders.append(order)
            self.dOrders[order.psId] = order
            logging.debug('build order dic key %d .',order.psId)
            i += 1
        logging.info('build %d orders.', i)
        logging.info('build dic keys %s orders.', self.dOrders.keys())
        self.all = i

    def syncSend(self):
        logging.info('sending orders to %s socket server...', self.client.ktName)
        self.client.syncSend()

    def syncSendOne(self,index):
        logging.debug('sending order to %s socket server...', self.client.ktName)
        self.client.syncSendOne(self.aOrders[index])

    def asyncSend(self):
        logging.info('sending orders to %s table...', self.client.ktName)
        self.client.asyncSend()

    def qryStatus(self):
        logging.info('query order status...')
        for ordKey in self.dOrders:
            aOrder = self.dOrders[ordKey]
            client = self.dClient[ordKey]
            logging.debug('sending %s orders.',  ordKey)

    def asyncSendOne(self):
        logging.info('sending orders to kt table...')
        for i in range(self.orderNum):
            for ordKey in self.dOrders:
                aOrder = self.dOrders[ordKey]
                client = self.dClient[ordKey]
                logging.debug('sending %s to %s.', aOrder[i].ktCase.psCaseName, ordKey)
                client.asyncSendOne(aOrder[i])
                client.getSts(aOrder[i])
                logging.debug('%s status: %d, failreason: %s', aOrder[i].ktCase.psCaseName, aOrder[i].psStatus, aOrder[i].failReason)
        logging.info('sended %d orders to %d kt db', self.orderNum, len(self.dOrders))


class KtTask(object):
    'provisioning task sended to ne'
    def __init__(self, oldpsid):
        self.oldPsId = oldpsid

    def setTask(self, taskInfo):
        self.taskNum = taskInfo[0]
        self.ps_id = taskInfo[1]
        self.bill_id = taskInfo[2]
        self.sub_bill_id = taskInfo[3]
        self.action_id = taskInfo[4]
        self.ps_service_type = taskInfo[5]
        self.ps_param = taskInfo[6]
        self.ps_status = taskInfo[7]
        failReason = taskInfo[8]
        # failReason.replace('\r\n','')
        # failReason.replace('\r', '')
        # failReason.replace('\n', '')
        self.fail_reason = failReason
        failLog = taskInfo[9]
        # failLog.replace('\r\n','')
        # failLog.replace('\r', '')
        # failLog.replace('\n', '')
        self.fail_log = failLog
        self.ps_service_code = taskInfo[10]
        self.ps_net_code = taskInfo[11]

    def compare(self,task):
        logging.info('compare task:  %d %d %d : %d %d %d', self.oldPsId,self.taskNum,self.ps_id,task.oldPsId,task.taskNum,task.ps_id)
        if self.action_id != task.action_id:
            return 2 # action_id ???
        if self.ps_param != task.ps_param:
            return 3 # ps_param ???
        if self.ps_service_type != task.ps_service_type:
            return 7 # ps_service_type ???
        if self.ps_net_code != task.ps_net_code:
            return 6  # ps_net_code ???
        if self.ps_service_code != task.ps_service_code:
            return 5  # ps_service_code ???
        self.replaceVar()
        task.replaceVar()
        FailLog = str(self.fail_log)
        cmprFailLog = str(task.fail_log)
        if FailLog != cmprFailLog:
            logging.info( 'compare false: %d %d, %s' , self.oldPsId,self.taskNum,FailLog)
            logging.info( 'compare false: %d %d, %s' ,task.oldPsId,task.taskNum,cmprFailLog)
            return 4  # fail_log ???
        if self.ps_status != task.ps_status:
            return 9 # ps_status ???
        if self.fail_reason != task.fail_reason:
            return 10 # fail_reason ???
        return 0 # ????

    def replaceVar(self):
        failLog = str(self.fail_log)
        dReplaceVar = {r'<vol:MessageID>\d+</vol:MessageID>':'<vol:MessageID>999999</vol:MessageID>',
                       r'<serial>\d+</serial>' : '<serial>99999999</serial>',
                       r'<time>\d+</time>' : '<time>20990101010101</time>',
                       r'<reqTime>\d+</reqTime>' : '<reqTime>20990101010101</reqTime>',
                       r'<sequence>\d+</sequence>' : '<sequence>999999</sequence>',
                       r'<xsd:reqTime>\d+</xsd:reqTime>': '<xsd:reqTime>999999</xsd:reqTime>',
                       r'<xsd:sequence>\d+</xsd:sequence>': '<xsd:sequence>999999</xsd:sequence>',
                       r'<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">\d+</m:MessageID>' : '<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">999999</m:MessageID>',
                       r'<serialNO>\d+</serialNO>': '<serialNO>999999</serialNO>',
                       r'<vol:MessageID>\d+</vol:MessageID>' : '<vol:MessageID>99999999</vol:MessageID>',
                       r'<hss:CNTXID>\d+</hss:CNTXID>' : '<hss:CNTXID>999</hss:CNTXID>',
                       r'<time>\d+</time>' : '<time>209900101010101</time>',
                       r'<serial>\d+</serial>' : '<serial>99999999</serial>',
                       r'<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">\d</m:MessageID>' : '<m:MessageID xmlns:m="http://www.chinamobile.com/IMS/ENUM">999999</m:MessageID>',
                       r'<crm:tradeNo>\d+</crm:tradeNo>' : '<crm:tradeNo>9999999999</crm:tradeNo>',
                       r'<crm:issueTime>\d+</crm:issueTime>' : '<crm:issueTime>20990101010101</crm:issueTime>',
                       r'<ns1:time>\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}</ns1:time>' : '<ns1:time>2099-01-09 01:01:01</ns1:time>',
                       r'<ns1:serial>\d+</ns1:serial>' : '<ns1:serial>99999999</ns1:serial>',
                       r'<hlr:time>\d+-\d{2}-\d{2} \d{2}:\d{2}:\d{2}</hlr:time>' : '<hlr:time>2099-01-09 01:01:01</hlr:time>',
                       r'<hlr:serial>\d+</hlr:serial>' : '<hlr:serial>99999999</hlr:serial>'}
        for varKey in dReplaceVar:
            if re.search(varKey, failLog):
                varValue = dReplaceVar[varKey]
                failLog = re.sub(varKey, varValue, failLog)
        self.fail_log = failLog


class KtBuilder(object):
    def __init__(self, conf, casefile, outfile):
        self.cfg = conf
        self.caseFile = casefile
        self.outFile = outfile
        self.aKtClient = []
        self.dKtClient = {}
        self.dOrderGrp = {}
        self.all = 0
        self.orderMode = None

    def buildCaseGrp(self):
        logging.info('build case group')
        self.caseGrp = CaseGrp(self.caseFile)
        self.caseGrp.openCaseFile()
        self.caseGrp.buildCaseGrp()

    def buildKtClient(self):
        logging.info('build kt client.')
        self.aKtClient = self.cfg.loadClient()
        for cli in self.aKtClient:
            cli.connDb()
            if self.orderMode.find('s') > -1:
                cli.connTcpServer()
            cli.caseGroup = self.caseGrp
            # self.dKtClient[cli.ktName] = cli

    def buildOrderGrp(self):
        logging.info('build order group.')
        for cli in self.aKtClient:
            ordGrp = OrderGrp(self.caseGrp, cli, self.orderMode)
            # ordGrp.orderMode = self.orderMode
            ordGrp.buildOrderGrp()
            cli.ordGroup = ordGrp
            # self.dOrderGrp[cli.ktName] = ordGrp
            self.all += ordGrp.orderNum
        # self.orderGrp = OrderGrp(self.caseGrp, self.aKtClient)
        # self.orderGrp.buildOrderGrp()

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

    def getSleepTime(self,donenum):
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
            logging.info('total %d asynchronous orders, sleep %d secondes.', self.all,sleepSecondes)
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

    def startProc(self):
        self.buildCaseGrp()
        self.buildKtClient()
        self.buildOrderGrp()
        if self.orderMode.find('t') > -1:
            self.asyncSend()
        if self.orderMode.find('s') > -1:
            self.syncSend()

        logging.info('query status of all orders')
        # signal.signal(signal.SIGINT, self.terminate)
        # signal.signal(signal.SIGQUIT, self.terminate)
        # signal.signal(signal.SIGTERM, self.terminate)
        # signal.signal(signal.SIGTSTP, self.terminate)
        if self.orderMode.find('t') > -1:
            self.getOrderStatus()
        if self.orderMode.find('s') > -1:
            self.getSyncResp()
        self.validate()
        self.writeResult()

    def terminate(self, signum, frame):
        self.validate()
        self.writeResult()

    def validate(self):
        pass

    def writeResult(self):
        try:
            fOut = open(self.outFile,'w')
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
                        ord.ktCase.psNo, ord.psId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName, ord.ktCase.comment, ord.psStatus, ord.failReason))
                except IOError, e:
                    logging.error('can not write file: %s: %s' , self.outFile, e)

    def writeSyncResult(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s synchronous orders result',  cli.ktName)
            fOut.write('\n#%s synchronous order\n' %  cli.ktName)
            orderGrp = cli.ordGroup
            orderGrp.syncFailNum = 0
            for ord in orderGrp.aOrders:
                if ord.syncStatus == -1:
                    orderGrp.syncFailNum += 1
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,"%s","%s"\n' % (
                        ord.ktCase.psNo, ord.tradId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName, ord.ktCase.comment, ord.syncStatus, ord.syncDesc, ord.response))
                except IOError, e:
                    logging.error('can not write file: %s: %s' , self.outFile, e)

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
            fOut.write('\n%s asynchronous orders all:%d  fial %d :\n' % (cli.ktName,orderGrp.orderNum, orderGrp.asyncFailNum))
            for ord in orderGrp.aOrders:
                if ord.psStatus == 9: continue
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,%s\n' % (
                        ord.ktCase.psNo, ord.psId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName, ord.ktCase.comment, ord.psStatus, ord.failReason))
                except IOError, e:
                    logging.error('can not write file: %s: %s' , self.outFile, e)

    def writeSyncFail(self, fOut):
        for cli in self.aKtClient:
            logging.info('write %s synchronous fail orders', cli.ktName)
            orderGrp = cli.ordGroup
            fOut.write('\n%s synchronous orders all:%d  fial %d :\n' % (cli.ktName,orderGrp.orderNum, orderGrp.syncFailNum))
            for ord in orderGrp.aOrders:
                if ord.syncStatus == 9: continue
                try:
                    fOut.write('%d,%d,%s,%s,%s,%s,%s,"%s","%s"\n' % (
                        ord.ktCase.psNo, ord.tradId, ord.ktCase.billId, ord.ktCase.subBillId, ord.ktCase.psCaseName,
                        ord.ktCase.comment, ord.syncStatus, ord.syncDesc, ord.response))
                except IOError, e:
                    logging.error('can not write file: %s: %s' , self.outFile, e)

    def loadNextClient(self):
        pass

class SyncBuilder(KtBuilder):
    pass

class AsyncBuilder(KtBuilder):
    pass

class SyncBuilder(object):
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
            print( 'Can not open configuration file %s: %s' % (self.cfgFile, e))
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
        try:
            fCfg = open(self.cfgFile, 'r')
        except IOError,e:
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
            if line == '#provisioning client conf' :
                clientSection = 1
                client = KtClient()
                continue
            if clientSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if param[0] == 'prvnName': client.ktName = param[1]
            elif param[0] == 'dbusr': client.dbUser = param[1]
            elif param[0] == 'dbpwd': client.dbPwd = param[1]
            elif param[0] == 'dbhost': client.dbHost = param[1]
            elif param[0] == 'dbport': client.dbPort = param[1]
            elif param[0] == 'dbsid': client.dbSid = param[1]
            elif param[0] == 'table': client.orderTablePre = param[1]
            elif param[0] == 'server': client.syncServer = param[1]
            elif param[0] == 'sockPort': client.sockPort = param[1]
        fCfg.close()
        logging.info('load %d clients.',len(self.aClient))
        return self.aClient
        # logging.debug(())


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
        self.caseFile = self.argvs[0]
        self.logFile = '%s%s' % (self.caseFile, '.log')
        self.outFile = '%s%s' % (self.caseFile, '.csv')

    def checkopt(self):
        argvs = sys.argv[1:]
        orderMode = ''
        try:
            opts,arvs = getopt.getopt(argvs,"st")
            # print '%s %s' % (opts,arvs)
        except getopt.GetoptError,e:
            orderMode = 't'
            # print 'get opt error:%s. %s' % (argvs,e)
            # self.usage()
        for opt,arg in opts:
            # print 'opt: %s' % opt
            if opt == '-t':
                orderMode = '%st' % orderMode
            if opt == '-s':
                orderMode = '%ss' % orderMode
        if orderMode == '':
            orderMode = 't'
        self.orderMode = orderMode
        self.argvs = arvs

    def start(self):
        self.cfg = Conf(self.cfgFile)
        self.cfg.loadLogLevel()
        self.logLevel = self.cfg.logLevel
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)
        logging.info('order mode: %s.' % self.orderMode)
        # self.checkopt()
        builder = KtBuilder(self.cfg, self.caseFile, self.outFile)
        builder.orderMode = self.orderMode
        builder.startProc()

    def usage(self):
        print "Usage: %s -ts casefile" % self.baseName
        print "option: \n   -t send asynchrous orders(insert db table);"
        print "   -s send synchrous orders(send to socket server);"
        print "   case file format: Request Message of kt4"
        exit(1)


# main here
if __name__ == '__main__':
    main = Main()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
