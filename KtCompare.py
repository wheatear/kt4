#!/usr/bin/env python

"""kt compare"""

import sys
import os
import time
import random
import re
import logging
import multiprocessing
import cx_Oracle as orcl
from socket import *

from KtClient import *

class KtCompareBuilder(KtBuilder):
    def __init__(self, conf, casefile, outfile, cmprfile):
        super(self.__class__, self).__init__(conf, casefile, outfile)
        self.cmprFile = cmprfile
        self.aCmprFailOrders = []
        self.sourCli = None
        self.destCli = None

    def buildOrderGrp(self):
        logging.info('build order group.')
        for cli in self.aKtClient:
            ordGrp = CompareOrderGrp(self.caseGrp, cli, self.orderMode)
            ordGrp.buildOrderGrp()
            cli.ordGroup = ordGrp
            # self.dOrderGrp[cli.ktName] = ordGrp
            self.all += ordGrp.orderNum

    def buildKtClient(self):
        super(self.__class__,self).buildKtClient()
        for cli in self.aKtClient:
            if cli.version == 'source':
                self.sourCli = cli
            elif cli.version == 'destination':
                self.destCli = cli

    def validate(self):
        if self.orderMode.find('t') > -1:
            logging.info('query tasks of asynchronous orders')
            for cli in self.aKtClient:
                logging.info('query %s tasks of asynchronous orders', cli.ktName)
                cli.getTasks()
        logging.info('compare order of %s and %s', self.destCli.ktName, self.sourCli.ktName)
        self.destCli.compare(self.sourCli)

    def writeResult(self):
        super(self.__class__, self).writeResult()
        try:
            fOut = open(self.cmprFile,'w')
        except IOError, e:
            logging.fatal('can not open out file %s: %s', self.outFile, e)
            return -1
        logging.info('write compare result of all orderes.')
        # for cli in self.aKtClient:
        #     if cli.version == 'source':
        #         sourCli = cli
        #     elif cli.version == 'destination':
        #         destCli = cli
        if self.orderMode.find('t') > -1:
            self.writeCmprAsync(fOut)
        if self.orderMode.find('s') > -1:
            self.writeCmprSync(fOut)

        logging.info('write statistics')
        if self.orderMode.find('t') > -1:
            self.writeStatAsync(fOut)
        if self.orderMode.find('s') > -1:
            self.writeStatSync(fOut)
        fOut.close()

    def writeCmprAsync(self, fOut):
        logging.info('write %s asynchrous orders compare result', self.destCli.ktName)
        fOut.write('\n#%s asynchrous order comparision\n' % self.destCli.ktName)
        orderGrp = self.destCli.ordGroup
        sourOrdGrp = self.sourCli.ordGroup
        passNum = 0
        failNum = 0
        for i in range(orderGrp.orderNum):
            ord = orderGrp.aOrders[i]
            sourOrd = sourOrdGrp.aOrders[i]
            if ord.asyncComparision is not None and ord.asyncComparision == 0:
                passNum += 1
            else:
                failNum += 1
            try:
                fOut.write('%d,%s,%d,%d,%s,%s,%d,%d,%s,%d,%s\n' % (
                    ord.ktCase.psNo, ord.ktCase.psCaseName, sourOrd.psId, ord.psId, ord.ktCase.billId,
                    ord.ktCase.subBillId, ord.asyncComparision, sourOrd.psStatus,
                    sourOrd.failReason, ord.psStatus, ord.failReason))
            except IOError, e:
                self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))
        orderGrp.cmprAsyncPass = passNum
        orderGrp.cmprAsyncFail = failNum

    def writeCmprSync(self, fOut):
        logging.info('write %s synchrous orders compare result', self.destCli.ktName)
        fOut.write('\n#%s synchrous order comparision\n' % self.destCli.ktName)
        orderGrp = self.destCli.ordGroup
        sourOrdGrp = self.sourCli.ordGroup
        passNum = 0
        failNum = 0
        for i in range(orderGrp.orderNum):
            ord = orderGrp.aOrders[i]
            sourOrd = sourOrdGrp.aOrders[i]
            if ord.syncComparision is not None and ord.syncComparision == 0:
                passNum += 1
            else:
                failNum += 1
            try:
                fOut.write('%d,%s,%d,%d,%s,%s,%d,%d,%s,%d,%s,%s,%s\n' % (
                    ord.ktCase.psNo, ord.ktCase.psCaseName, sourOrd.tradId, ord.tradId, ord.ktCase.billId,
                    ord.ktCase.subBillId, ord.syncComparision, sourOrd.syncStatus,
                    sourOrd.syncDesc, ord.syncStatus, ord.syncDesc, sourOrd.response, ord.response))
            except IOError, e:
                self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))
        orderGrp.cmprSyncPass = passNum
        orderGrp.cmprSyncFail = failNum

    def writeStatAsync(self, fOut):
        try:
            fOut.write('\nasync orders all: %d, pass: %d, fail: %d.\n' % (self.destCli.ordGroup.orderNum, self.destCli.ordGroup.cmprAsyncPass, self.destCli.ordGroup.cmprAsyncFail))
            # fOut.write('sync pass: %d, fail: %d.\n' % (passSyncNum, failSyncNum))
        except IOError, e:
            self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))

        for i in range(self.destCli.ordGroup.orderNum):
            ord = self.destCli.ordGroup.aOrders[i]
            sourOrd = self.sourCli.ordGroup.aOrders[i]
            if ord.asyncComparision > 0:
                try:
                    fOut.write('%d,%s,%d,%d,%s,%s,%d,%d,%s,%d,%s\n' % (
                        ord.ktCase.psNo, ord.ktCase.psCaseName, sourOrd.psId, ord.psId, ord.ktCase.billId,
                        ord.ktCase.subBillId, ord.asyncComparision, sourOrd.psStatus,
                        sourOrd.failReason, ord.psStatus, ord.failReason))
                except IOError, e:
                    self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))

    def writeStatSync(self, fOut):
        try:
            fOut.write('\nsync orders all: %d, pass: %d, fail: %d.\n' % (self.destCli.ordGroup.orderNum, self.destCli.ordGroup.cmprSyncPass, self.destCli.ordGroup.cmprSyncFail))
            # fOut.write('sync pass: %d, fail: %d.\n' % (passSyncNum, failSyncNum))
        except IOError, e:
            self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))

        for i in range(self.destCli.ordGroup.orderNum):
            ord = self.destCli.ordGroup.aOrders[i]
            sourOrd = self.sourCli.ordGroup.aOrders[i]
            if ord.syncComparision > 0:
                try:
                    fOut.write('%d,%s,%d,%d,%s,%s,%d,%d,%s,%d,%s,%s,%s\n' % (
                        ord.ktCase.psNo, ord.ktCase.psCaseName, sourOrd.tradId, ord.tradId, ord.ktCase.billId,
                        ord.ktCase.subBillId, ord.syncComparision, sourOrd.syncStatus,
                        sourOrd.syncDesc, ord.syncStatus, ord.syncDesc, sourOrd.response, ord.response))
                except IOError, e:
                    self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outFile, e))
    # def writeCmprFail(self,orderGrp, fout):
    #     for i in range(orderGrp.orderNum):

class ktCompareClient(KtClient):
    def __init__(self):
        super(self.__class__, self).__init__()

    def compare(self,cli):
        logging.info('compare %s to %s', self.ktName, cli.ktName)
        self.ordGroup.compare(cli.ordGroup)


class CompareOrderGrp(OrderGrp):
    def __init__(self, casegrp, client, ordermode):
        super(self.__class__, self).__init__(casegrp, client, ordermode)
        self.cmprSyncPass = 0
        self.cmprSyncFail = 0
        self.cmprAsyncPass = 0
        self.cmprAsyncFail = 0
        self.cmprPass = 0
        self.cmprFail = 0

    def buildOrderGrp(self):
        logging.info('build %s orders.',self.client.ktName)
        i = 0
        for case in self.caseGrp.aCases:
            order = CompareKtOrder(case)
            order.mode = self.orderMode
            order.client = self.client
            order.getOrderInfo(self.client.conn)
            self.aOrders.append(order)
            self.dOrders[order.tradId] = order
            i += 1
        logging.info('build %d orders.', i)
        self.all = i

    def compare(self,orderGroup):
        if self.orderMode.find('t') > -1:
            self.compareAsync(orderGroup)
        if self.orderMode.find('s') > -1:
            self.compareSync(orderGroup)

    def compareAsync(self, orderGroup):
        logging.info('compare asynchrous orders')
        for i in range(self.orderNum):
            ord = self.aOrders[i]
            ordcmpr = orderGroup.aOrders[i]
            cmprAsync = ord.compareAsync(ordcmpr)
            if cmprAsync > 0: self.cmprAsyncFail += 1
            else: self.cmprSyncPass += 1

    def compareSync(self, orderGroup):
        logging.info('compare synchrous orders')
        for i in range(self.orderNum):
            ord = self.aOrders[i]
            ordcmpr = orderGroup.aOrders[i]
            cmprSync = ord.compareSync(ordcmpr)
            if cmprSync > 0:
                self.cmprSyncFail += 1
            else: self.cmprSyncPass += 1
    # def writeCpmrFail(self,fout):
    #     for i in range(self.orderNum):
    #         ord =

class CompareKtOrder(KtOrder):
    def __init__(self, ktcase):
        super(self.__class__, self).__init__(ktcase)
        self.asyncComparision = None
        self.syncComparision = None

    # def compareAsync(self, psOrder):
    #     self.compareAsync(psOrder)
    #
    # def compareSync(self, psOrder):
    #     self.compareSync(psOrder)

    def compareAsync(self,psOrder):
        logging.debug('compare asynchrous orders %s', self.client.ktName)
        myTaskNum = len(self.aTask)
        cmpaTaskNum = len(psOrder.aTask)
        if myTaskNum != cmpaTaskNum or myTaskNum == 0:
            logging.debug('%s: %d', self.client.ktName, myTaskNum)
            logging.debug('%s: %d', psOrder.client.ktName, cmpaTaskNum)
            time.sleep(3)
            self.client.getTasksOne(self)
            # self.getTasks(self.conn,self.month)
            psOrder.client.getTasksOne(psOrder)
            # psOrder.getTasks(psOrder.conn,psOrder.month)
            myTaskNum = len(self.aTask)
            cmpaTaskNum = len(psOrder.aTask)
            if myTaskNum != cmpaTaskNum:
                logging.debug('%s: %d' , self.client.ktName, myTaskNum)
                logging.debug('%s: %d' ,psOrder.client.ktName, cmpaTaskNum)
                self.asyncComparision = 1

                return self.asyncComparision  # ???????
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
            return 9 # ps_status ???
        if self.failReason != psOrder.failReason:
            logging.debug('psstatus:%d,%s : %d,%s', self.psId, self.failReason, psOrder.psId, psOrder.failReason)
            self.asyncComparision = 10
            return 10 #order fail_reason ???
        if self.psStatus == -1:
            self.asyncComparision = 14
        self.asyncComparision = 0
        logging.debug('compare %d to %d result:%d', self.psId, psOrder.psId, self.asyncComparision)
        return self.asyncComparision  #

    def compareSync(self,psOrder):
        response = re.sub(r'TRADE_ID=\d+;','TRADE_ID=99999999;', self.response)
        cmprResponse = re.sub(r'TRADE_ID=\d+;','TRADE_ID=99999999;', psOrder.response)
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
        # if response == cmprResponse:
        #     self.syncComparision = 0
        #     return 0
        # else:
        #     self.syncComparision = 8
        #     return 8 #???????

class MainCompare(Main):
    def __init__(self):
        super(self.__class__, self).__init__()

    def checkArgv(self):
        super(self.__class__, self).checkArgv()
        self.cmprFile = '%s_cmpr%s' % (self.caseFile, '.csv')

    def start(self):
        self.cfg = CompareConf(self.cfgFile)
        self.cfg.loadLogLevel()
        self.logLevel = self.cfg.logLevel
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.baseName)
        builder = KtCompareBuilder(self.cfg, self.caseFile, self.outFile, self.cmprFile)
        builder.orderMode = self.orderMode
        builder.startProc()

class CompareConf(Conf):
    def __init__(self, cfgfile):
        super(self.__class__, self).__init__(cfgfile)

    def loadClient(self):
        # super(self.__class__, self).__init__()
        # for cli in self.aClient:
        #     cfgFile = cli.
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
                continue
            if line == '#provisioning client conf' :
                clientSection = 1
                client = ktCompareClient()
                continue
            if clientSection < 1:
                continue
            logging.debug(line)
            param = line.split(' = ', 1)
            if param[0] == 'prvnName': client.ktName = param[1]
            elif param[0] == 'dbusr': client.dbUser = param[1]
            elif param[0] == 'version': client.version = param[1]
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


# main here
if __name__ == '__main__':
    main = MainCompare()
    main.checkArgv()
    main.start()
    logging.info('%s complete.', main.baseName)
