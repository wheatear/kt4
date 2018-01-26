#!/usr/bin/env python
'''send record to kt
'''
import sys
import os
import time
import random
import re
import cx_Oracle as orcl


# class definition part
class PsOrder(object):
    """provision service record"""
    def __init__(self, orderName, psServiceType, actionId, psParam, billId='', subBillId=''):
        self.psOrderName = orderName
        self.psServiceType = psServiceType
        self.billId = billId
        self.subBillId = subBillId
        self.actionId = actionId
        self.psParam = psParam
        # self.log = Log(None)

        self.psId = None
        self.doneCode = None
        self.regionCode = None
        self.psStatus = 0
        self.failReason = None
        self.sendTablePre = 'i_provision'

        self.aCmdKey = []  # keys of commandes send to ne
        self.taskCount = 0
        self.comment = None

        # self.aCmd = []  #
        self.aTask = []  # taskes sended to ne
        self.conn = None
        self.month = None

    def setPsId(self, psId):
        self.psId = psId

    def setDoneCode(self, doneCode):
        self.doneCode = doneCode

    def setRegionCode(self, regionCode):
        self.regionCode = regionCode

    def setNumber(self, billId, subBillId):
        self.billId = billId
        self.subBillId = subBillId

    def getSubInfo(self,conn):
        self.regionCode = None
        self.psId = None
        self.doneCode = None
        params = {'billId': self.billId}
        cur = conn.cursor()
        regionCode = '100'
        if re.match(r'\d{11}',self.billId ):
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
        cur.close()

        # if self.psServiceType == 'HLR' or self.psServiceType == 'VOLTE_AS' or self.psServiceType == 'VGOP' or self.psServiceType == 'IN':
        #     sql = 'select region_code,SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL from ps_net_number_area where :billId between start_number and end_number'
        #     cur.execute(sql, params)
        # else:
        #     sql = "select '100',SEQ_PS_ID.NEXTVAL,SEQ_PS_DONECODE.NEXTVAL from dual"
        #     cur.execute(sql)
        # result = cur.fetchone()
        # if result:
        #     self.regionCode = result[0]
        #     self.psId = result[1]
        #     self.doneCode = result[2]
        # cur.close()
        # if self.psServiceType == 'HLR' or self.psServiceType == 'VOLTE_AS' or self.psServiceType == 'VGOP' or self.psServiceType == 'IN':
        #     pass
        # else:
        #     self.regionCode = random.randrange(100, 200, 10)

    # def setKtDs(self, ds):
    #     conn = ds

    def sendKt(self,conn):
        log = Log(None)
        params = {'psId': self.psId, 'doneCode': self.doneCode, 'psServiceType': self.psServiceType,
                  'billId': self.billId, 'subBillId': self.subBillId, 'actionId': self.actionId,
                  'psParam': self.psParam, 'regionCode': self.regionCode}
        sql = 'insert into %s_%s (ps_id,busi_code,done_code,ps_type,prio_level,ps_service_type,bill_id,sub_bill_id,sub_valid_date,create_date,status_upd_date,action_id,ps_param,ps_status,op_id,region_code,service_id,sub_plan_no,RETRY_TIMES) values(:psId,0,:doneCode,0,80,:psServiceType,:billId,:subBillId,sysdate,sysdate,sysdate,:actionId,:psParam,0,530,:regionCode,100,0,5)' % (self.sendTablePre, self.regionCode)
        log.writeLog('DEBUG', '%s: %s' % (sql, params))
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            cur.close()
        except Exception,e:
            log.writeLog('ERROR', 'can not execute sql: %s : %s' % (sql, e))

    def getSts(self,conn, month):
        log = Log(None)
        log.writeLog('DEBUG', 'get %s status ' % (self.psId))
        params = {'psId': self.psId}
        sql = 'select ps_status,fail_reason from ps_provision_his_%s_%s where ps_id=:psId' % (self.regionCode, month)
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            result = cur.fetchone()
        except Exception, e:
            log.writeLog('ERROR', 'can not execute sql: %s : %s' % (sql, e))
            cur.close()
            return 0
        if result:
            log.writeLog('DEBUG', 'get %s status %s' % (self.psId, result[0]))
            self.psStatus = result[0]
            self.failReason = result[1]
            cur.close()
            return 1
        else:
            cur.close()
            return 0
        cur.close()

    def getTasks(self,conn, month):
        log = Log(None)
        self.conn = conn
        self.month = month
        self.aTask = []
        sql = 'select rownum,bill_id,sub_bill_id,action_id,ps_service_type,ps_param,ps_status,fail_reason,fail_log,ps_service_code,ps_net_code from ps_split_his_%s_%s where old_ps_id=:psId and bill_id=:billId order by ps_id' % (self.regionCode, month)
        argus = {'psId': self.psId, 'billId': self.billId}
        cur = conn.cursor()
        try:
            cur.execute(sql,argus)
            rows = cur.fetchall()
        except Exception, e:
            log.writeLog('ERROR', 'can not execute sql: %s :billid:%s psId:%s %s' % (sql, self.billId, self.psId, e))
            cur.close()
            return 0
        for item in rows:
            task = PsTask(self.psId)
            task.setTask(item)
            log.writeLog('DEBUG','%s %s' % (self.psId,item[0]))
            log.writeLog('DEBUG', 'psId:%s subPsId:%s billid:%s failLog:%s' % (self.psId,item[0], self.billId, task.fail_log))
            # print 'psId:%s billid:%s failLog:%s' % (self.psId, self.billId, task.fail_log)
            self.aTask.append(task)
        cur.close()
        return 1

    def compare(self,psOrder):
        log = Log(None)
        myTaskNum = len(self.aTask)
        cmpaTaskNum = len(psOrder.aTask)
        if myTaskNum != cmpaTaskNum:
            log.writeLog('DEBUG','kt3: %d' % myTaskNum)
            log.writeLog('DEBUG', 'kt4: %d' % cmpaTaskNum)
            time.sleep(3)
            self.getTasks(self.conn,self.month)
            psOrder.getTasks(psOrder.conn,psOrder.month)
            myTaskNum = len(self.aTask)
            cmpaTaskNum = len(psOrder.aTask)
            if myTaskNum != cmpaTaskNum:
                log.writeLog('DEBUG','kt3: %d' % myTaskNum)
                log.writeLog('DEBUG','kt4: %d' % cmpaTaskNum)
                return 1  # ???????
        for i in range(myTaskNum):
            myTask = self.aTask[i]
            cmpaTask = psOrder.aTask[i]
            cmpaCode = myTask.compare(cmpaTask)
            if cmpaCode > 0:
                return cmpaCode
        return 0 # ????


class PsTask(object):
    'provisioning task sended to ne'
    def __init__(self, oldpsid):
        self.oldPsId = oldpsid
        self.log = Log(None)

    def setTask(self, taskInfo):
        self.taskNum = taskInfo[0]
        self.bill_id = taskInfo[1]
        self.sub_bill_id = taskInfo[2]
        self.action_id = taskInfo[3]
        self.ps_service_type = taskInfo[4]
        self.ps_param = taskInfo[5]
        self.ps_status = taskInfo[6]
        failReason = taskInfo[7]
        failReason.replace('\r\n','')
        failReason.replace('\r', '')
        failReason.replace('\n', '')
        self.fail_reason = failReason
        failLog = taskInfo[8]
        # failLog.replace('\r\n','')
        # failLog.replace('\r', '')
        # failLog.replace('\n', '')
        self.fail_log = failLog
        self.ps_service_code = taskInfo[9]
        self.ps_net_code = taskInfo[10]

    def compare(self,task):
        self.log.writeLog('INFO', 'compare task: kt3: %d %d kt4: %d %d' % (self.oldPsId,self.taskNum,task.oldPsId,task.taskNum))
        if self.action_id != task.action_id:
            return 2 # action_id ???
        if self.ps_param != task.ps_param:
            return 3 # ps_param ???
        self.replaceVar(task)
        kt3FailLog = str(self.fail_log)
        kt4FailLog = str(task.fail_log)
        if kt3FailLog != kt4FailLog:
            self.log.writeLog('INFO', 'compare false: kt3:%d %d, %s' % (self.oldPsId,self.taskNum,kt3FailLog))
            self.log.writeLog('INFO', 'compare false: kt4:%d %d, %s' % (task.oldPsId,task.taskNum,kt4FailLog))
            return 4  # fail_log ???
        if self.ps_service_code != task.ps_service_code:
            return 5  # ps_service_code ???
        if self.ps_net_code != task.ps_net_code:
            return 6 # ps_net_code ???
        if self.ps_service_type != task.ps_service_type:
            return 7 # ps_service_type ???
        return 0 # ????

    def replaceVar(self,task):
        kt3FailLog = str(self.fail_log)
        kt4FailLog = str(task.fail_log)
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
                       r'<hss:CNTXID>\d+</hss:CNTXID>' : '<hss:CNTXID>999</hss:CNTXID>'}
        for varKey in dReplaceVar:
            if re.search(varKey, kt3FailLog):
                varValue = dReplaceVar[varKey]
                kt3FailLog = re.sub(varKey, varValue, kt3FailLog)
                kt4FailLog = re.sub(varKey, varValue, kt4FailLog)
        # if re.search(r'<vol:MessageID>\d+</vol:MessageID>', kt3FailLog):
        #     kt3FailLog = re.sub(r'<vol:MessageID>\d+</vol:MessageID>', '<vol:MessageID>999999</vol:MessageID>', kt3FailLog)
        #     kt4FailLog = re.sub(r'<vol:MessageID>\d+</vol:MessageID>', '<vol:MessageID>999999</vol:MessageID>', kt4FailLog)
        # if re.search(r'<serial>\d+</serial>',kt3FailLog):
        #     self.log.writeLog('INFO', 'find %s' % '<serial>\d+</serial>')
        #     kt3FailLog = re.sub(r'<serial>\d+</serial>', '<serial>99999999</serial>', kt3FailLog)
        #     kt4FailLog = re.sub(r'<serial>\d+</serial>', '<serial>99999999</serial>', kt4FailLog)
        # if re.search(r'<time>\d+</time>',kt3FailLog):
        #     kt3FailLog = re.sub(r'<time>\d+</time>', '<time>20990101010101</time>', kt3FailLog)
        #     kt4FailLog = re.sub(r'<time>\d+</time>', '<time>20990101010101</time>', kt4FailLog)
        # if re.search(r'<reqTime>\d+</reqTime>',kt3FailLog):
        #     kt3FailLog = re.sub(r'<reqTime>\d+</reqTime>', '<reqTime>20990101010101</reqTime>', kt3FailLog)
        #     kt4FailLog = re.sub(r'<reqTime>\d+</reqTime>', '<reqTime>20990101010101</reqTime>', kt4FailLog)
        # if re.search(r'<sequence>\d+</sequence>',kt3FailLog):
        #     kt3FailLog = re.sub(r'<sequence>\d+</sequence>', '<sequence>999999</sequence>', kt3FailLog)
        #     kt4FailLog = re.sub(r'<sequence>\d+</sequence>', '<sequence>999999</sequence>', kt4FailLog)
        self.fail_log = kt3FailLog
        task.fail_log = kt4FailLog


class PsCreator(object):
    'ps control'
    def __init__(self, orderFile, ktDs):
        self.startTime = time.time()
        self.endTime = None
        self.orderFile = orderFile
        self.outName = '%s.csv' % orderFile
        self.orderDs = None
        self.ktDs = ktDs
        self.log = Log(None)
        self.psNum = 0
        self.aPs = []
        self.month = time.strftime("%Y%m", time.localtime())
        self.unDone = 0
        self.sucNum = 0
        self.failNum = 0

    def openFile(self, dsName, mode):
        try:
            fp = open(dsName, mode)
        except IOError, e:
            self.log.writeLog('FATAL', 'Can not open file %s: %s' % (dsName, e))
            exit()
        return fp

    def closeFile(self, fp):
        fp.close()

    def toOperate(self):
        self.log.writeLog('INFO', 'get ps')
        self.orderDs = self.openFile(self.orderFile, 'r')
        ps = self.getNextPs()
        while ps:
            if not ps:
                continue
            # ps.setKtDs(self.ktDs)
            ps.getSubInfo(self.ktDs)
            self.aPs.append(ps)
            if ps.regionCode:
                ps.sendKt(self.ktDs)
            ps = self.getNextPs()
        self.closeFile(self.orderDs)
        self.log.writeLog('INFO', 'sended %d ps to kt' % self.psNum)
        self.unDone = self.psNum

        while (self.unDone):
            sleepTime = self.getSleepTime(self.unDone)
            self.log.writeLog('INFO', 'all: %d, sucess: %d, fail: %d, undone: %d.\nsleep: %d' % (self.psNum, self.sucNum, self.failNum, self.unDone, sleepTime))
            time.sleep(self.unDone)
            self.qrySts(self.aPs)

        self.outDs = self.openFile(self.outName, 'w')
        self.writeRelt(self.aPs)
        self.closeFile(self.outDs)
        self.log.writeLog('INFO', 'write result completed')

    def getAllPs(self):
        self.log.writeLog('INFO', 'get all ps')
        self.orderDs = self.openFile(self.orderFile, 'r')
        ps = self.getNextPs()
        while ps:
            if not ps:
                continue
            # ps.setKtDs(self.ktDs)
            ps.getSubInfo(self.ktDs)
            self.aPs.append(ps)
            if ps.regionCode:
                ps.sendKt(self.ktDs)
            ps = self.getNextPs()
        self.closeFile(self.orderDs)
        self.log.writeLog('INFO', 'sended %d ps to kt' % self.psNum)
        self.unDone = self.psNum

    def getSleepTime(self, unDone):
        if unDone < 100:
            return unDone
        return unDone / len(str(unDone))

    def writeRelt(self, aPses):
        for ps in aPses:
            try:
                self.outDs.write('%d %d %s %s %s %s %s\n' % (
                    ps.psNo, ps.psId, ps.billId, ps.subBillId, ps.comment, ps.psStatus, ps.failReason))
            except IOError, e:
                self.log.writeLog('FATAL', 'can not write file: %s: %s' % (self.outName, e))

    def qrySts(self, aPses):
        unDone = 0
        for ps in aPses:
            if ps.psStatus != 0:
                continue
            if not ps.getSts(self.ktDs, self.month):
                unDone += 1
            else:
                if ps.psStatus == 9:
                    self.sucNum += 1
                else:
                    self.failNum += 1
        self.unDone = unDone

    def getNextPs(self):
        caseName = None
        comment = None
        mmlCount = None
        mmls = []
        regionCode = None
        psServiceType = None
        billId = None
        subBillId = None
        actionId = None
        psParam = None
        for line in self.orderDs:
            line = line.strip()
            self.log.writeLog('DEBUG', line)
            if len(line) == 0:
                continue
            if line[0] == '#':
                if len(line) < 3:
                    continue
                k2 = line[1]
                if k2 == '@':  # "#@" is mml command key  e.g. "#@Qry User:MSISDN=,Item=LOC"
                    mmls.append(line[2:])
                elif k2 == '#':  # "##" is comment,ignore.
                    comment = line[2:]
                elif k2 == '%':  # "#%" is count of mml command
                    mmlCount = line[2:]
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
                self.psNum += 1
                if caseName is None:
                    caseName = 'case%d' % self.psNum
                myPs = PsOrder(caseName, psServiceType, actionId, psParam, billId, subBillId)
                myPs.psNo = self.psNum
                if comment: myPs.comment = comment
                if mmlCount: myPs.mmlCount = mmlCount
                if mmls: myPs.aMml = mmls
                mmls = []
                self.log.writeLog('DEBUG', vars(myPs))
                return myPs
            elif param[0] == 'KT_REQUEST':
                pass
            else:
                self.log.writeLog('INFO', line)


class Main(object):
    'main'
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        if self.argc < 2:
            self.usage()
        self.cmdName = sys.argv[1]
        self.logFile = '%s%s' % (self.cmdName, '.log')
        self.cfgFile = '%s.cfg' % self.Name

    def getConf(self):
        cfg = Conf(self.cfgFile)
        cfg.setLogFile(self.logFile)
        log = Log(cfg)
        #		self.log = log
        log.writeLog('DEBUG', vars(cfg))
        self.ktDs = cfg.getDs()

    def startProc(self):
        myPsCreator = PsCreator(self.cmdName, self.ktDs)
        myPsCreator.toOperate()

    def usage(self):
        print "Usage: %s fileName" % self.baseName
        print "file format: Request Message of BST Extend API"
        exit(1)


class Conf(object):
    'data source configuration'

    def __init__(self, cfgFile):
        self.dbusr = None
        self.pawd = None
        self.sid = None
        self.host = None
        self.port = None
        self.dLevel = None
        self.level = 'INFO'
        self.logFile = None
        self.con = None

        #		execfile(cfgFile)
        fCfg = open(cfgFile, 'r')
        exec fCfg
        fCfg.close()
        if dbusr: self.dbusr = dbusr
        if pawd: self.pawd = pawd
        if sid: self.sid = sid
        if host: self.host = host
        if port: self.port = port
        if dLOGLEVEL: self.dLevel = dLOGLEVEL
        if LOGLEVEL: self.level = LOGLEVEL

    def setLogFile(self, log):
        self.logFile = log

    def getLogFile(self):
        return self.logFile

    def getDs(self):
        if self.con: return self.con
        try:
            dsn = orcl.makedsn(self.host, self.port, self.sid)
            dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            self.con = orcl.connect(self.dbusr, self.pawd, dsn)
        # cursor = con.cursor()
        except Exception, e:
            print 'could not connec to oracle(%s:%s/%s %s), %s' % (self.host, self.port, self.sid,self.dbusr, e)
            exit()
        return self.con


class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw)
        return cls._instance


class Log(Singleton):
    'Log class'

    def __init__(self, conf):
        if not hasattr(self, 'fLog'):
            self.dLevel = conf.dLevel
            self.level = self.dLevel[conf.level]
            self.logName = conf.logFile
            self.fLog = open(self.logName, 'a')

    def writeLog(self, logType, message):
        if self.dLevel[logType] < self.level:
            return
        sTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.fLog.write('%s %s %s%s' % (sTime, logType, message, '\n'))

    def close(self):
        self.fLog.close()


# main here
if __name__ == '__main__':
    'main'
    main = Main()
    main.getConf()
    log = Log(None)
    log.writeLog('INFO', '%s %s' % (main.baseName, 'Start process...'))
    main.startProc()

    log.writeLog('INFO', '%s %s' % (main.baseName, ' process completed.'))

    log.close()
