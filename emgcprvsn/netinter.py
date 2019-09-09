#!/usr/bin/env python

'''emergency provisioning network interface model'''

import sys
import os
import multiprocessing
import cx_Oracle as orcl
from socket import *
import psorder
from config import *

ls = os.linesep

# hss5
# $remote_host = "10.4.139.25";
# $hlrsn = 5;


class TcpClt(object):
    def __init__(self, bufSize=1024):
        self.bufSize = bufSize
        # try:
        #     tcpClt = socket(AF_INET, SOCK_STREAM)
        # except Exception, e:
        #     print 'Can not create socket : %s' % (e)
        #     exit(-1)
        # self.clt = tcpClt

    def connect(self, host, port):
        self.host = host
        self.port = port
        self.addr = (self.host, self.port)
        try:
            tcpClt = socket(AF_INET, SOCK_STREAM)
            self.clt = tcpClt
            self.clt.connect(self.addr)
        except Exception, e:
            print 'Can not connect socket server %s:%s. %s' % (self.host, self.port, e)
            return False
        return True

    def send(self, message):
        reqLen = len(message) + 12
        reqMsg = '%08d%s%s' % (reqLen, 'REQ:', message)
        self.clt.sendall(reqMsg)

    def recv(self):
        rspMsg = self.clt.recv(self.bufSize)
        rcvLen = len(rspMsg)
        rspLen = int(rspMsg[0:8])
        while rcvLen < rspLen:
            rspMsg = '%s%s' % (rspMsg, self.clt.recv(self.bufSize))
            rcvLen = len(rspMsg)
        return rspMsg


class HttpClt(TcpClt,multiprocessing.Process):
    def __init__(self, cfg, netname, quhss, bufSize=1024):
        multiprocessing.Process.__init__(self)
        TcpClt.__init__(self, bufSize)
        self.cfg = cfg
        self.netName = netname
        self.quHss = quhss
        self.host = None
        self.port = None
        self.user = None
        self.passwd = None
        self.remoteUrl = None
        self.hlrsn = None
        self.dTemplate = {}

    def run(self):
        self.cfg.logWriter.pid = '%s %s' % (os.getpid(),self.name)
        self.cfg.getDb()
        self.db = self.cfg.db
        self.getTemplate()
        self.getHostInfo()
        self.connect()
        self.setHttpCmd()
        self.setHead()
        if not self.login():
            msg = '%s exit.' % self.netName
            self.cfg.logWriter.writeLog('DEBUG', msg)
            return -1
        self.setHttpCmd()

        curUpdFail = self.db.cursor()
        curUpdSuc = self.db.cursor()
        curUpdFail.prepare('update EMPS_PROVISION set ps_status=:1,fail_reason=:2,fail_log=:3,end_date=sysdate,ret_date=sysdate where ps_id=:4')
        curUpdSuc.prepare('update EMPS_PROVISION set ps_status=:1,fail_reason=:2,fail_log=:3,end_date=sysdate,ret_date=sysdate where ps_id=:4')
        while True:
            msg = '%s Queue size: %d' % (self.netName, self.quHss.qsize())
            self.cfg.logWriter.writeLog('DEBUG', msg)
            try:
                order = self.quHss.get()
            except KeyboardInterrupt, e:
                msg = 'psQueue been KeyboardInterrupt, %s process exit: %s' %  (self.netName,e)
                self.cfg.logWriter.writeLog('INFO', msg)
                break
            if order is None:
                msg = '%s exit' % self.netName
                self.cfg.logWriter.writeLog('INFO', msg)
                break
            msg = '%s get order: %s' % (self.name, order.psId)
            self.cfg.logWriter.writeLog('DEBUG', msg)
            order.failLog = ''
            instKey = order.psServiceCode
            if instKey not in self.dTemplate:
                order.psStatus = -1
                order.failReason = 'can not find hss instruct for %s' % instKey
                msg = '%s; %s %s' % (curUpdFail.statement, order.psId,order.failReason)
                self.cfg.logWriter.writeLog('INFO', msg)
                curUpdFail.execute(None,(order.psStatus, order.failReason, order.failLog, order.psId))
                curUpdFail.connection.commit()
                self.moveHis(self.conn, order)
                continue
            instructes = self.makeInstruct(order)
            for instruct in instructes:
                order.failLog += instruct
                for i in range(3):
                    self.send(instruct)
                    try:
                        rsp = self.recv()
                    except Exception, e:
                        self.cfg.logWriter.writeLog('INFO', 'recv response failed,exception: %s' % e)
                    if rsp:
                        break
                    self.cfg.logWriter.writeLog('INFO', 'recv response fail,retry connect %d times' % i)
                    self.remoteUrl = self.cfgUrl
                    self.connect()
                    self.setHttpCmd()
                    if not self.login():
                        msg = '%s login failed, exit.' % self.netName
                        self.cfg.logWriter.writeLog('DEBUG', msg)
                        return -1
                    self.setHttpCmd()

                if self.parseResp(rsp):
                    order.psStatus = 9
                    order.failReason= 'success'
                    curUpdSuc.execute(None, (order.psStatus, order.failReason, order.failLog, order.psId))
                    curUpdSuc.connection.commit()
                    msg = '%s %s' % (curUpdSuc.statement, curUpdFail.bindvars)
                    self.cfg.logWriter.writeLog('INFO', msg)
                else:
                    order.psStatus = -1
                    order.failReason = 'hss failed'
                    curUpdFail.execute(None,(order.psStatus, order.failReason, order.failLog, order.psId))
                    curUpdSuc.connection.commit()
                    msg = '%s %s' % (curUpdFail.statement,curUpdFail.bindvars)
                    self.cfg.logWriter.writeLog('INFO', msg)
                    break
            msg = 'move ps to his: %s' % order.psId
            self.cfg.logWriter.writeLog('INFO', msg)
            self.moveHis(self.db,order)

    def connect(self):
        TcpClt.connect(self, self.host, self.port)

    def getHostInfo(self):
        sql = "select interface_desc,attribute from ps_net_config_tst where STATE=1 AND ps_net_code like :1 "
        nameLike = '%s%%' % self.netName
        cur = self.db.cursor()
        cur.execute(sql,(nameLike,))
        msg = 'get hostinfo:%s; %s' % (cur.statement, cur.bindvars)
        self.cfg.logWriter.writeLog('INFO', msg)
        row = cur.fetchone()
        cur.close()
        self.cfg.logWriter.writeLog('INFO', row)
        hostAddr = row[0]
        hostAttr = row[1]
        for item in hostAddr.split(';'):
            (pre,a,post) = item.partition('=')
            if pre == 'http:ServiceIp':
                self.host = post
            elif pre == 'ServicePort':
                self.port = int(post)
        for item in hostAttr.split('\n'):
            (pre,i,post) = item.partition('=')
            if pre == 'GLOBAL.USER':
                self.user = post
            elif pre == 'GLOBAL.PASSWD':
                self.passwd = post
            elif pre == 'GLOBAL.URL':
                self.remoteUrl = post
                self.cfgUrl = post
            elif pre == 'GLOBAL.HLRSN':
                self.hlrsn = post
        msg = 'host:%s port:%d user:%s passwd:%s url:%s hlrsn:%s' % (self.host,self.port ,self.user,self.passwd,self.remoteUrl,self.hlrsn)
        self.cfg.logWriter.writeLog('INFO', msg)

    def getTemplate(self):
        self.dTemplate['SUSPEND=3;'] = ('''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_LCK>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:IC>FALSE</hss:IC>
         <hss:OC>FALSE</hss:OC>
         <hss:GPRSLOCK>^<GPRS_FLAG^></hss:GPRSLOCK>
      </hss:MOD_LCK>
   </soapenv:Body>
</soapenv:Envelope>''',)
        # {'SUSPEND=3;': 'SUSPEND=3;', 'EPS=1;': 'EPS=1;', 'EPS=2;STATUS=GRANTED;': 'EPS=2;', 'GPRS=1;': 'GPRS=1;'}
        self.dTemplate['EPS=1;'] = ('''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:ADD_EPSSUB>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:EPSUSERTPLID>851</hss:EPSUSERTPLID>
         <hss:DIAMNODETPL_ID>950</hss:DIAMNODETPL_ID>
      </hss:ADD_EPSSUB>
   </soapenv:Body>
</soapenv:Envelope>''',)
        self.dTemplate['EPS=2;'] = ('''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_LCK>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:EPSLOCK>FALSE</hss:EPSLOCK>
      </hss:MOD_LCK>
   </soapenv:Body>
</soapenv:Envelope>''',)
        self.dTemplate['GPRS=1;'] = ('''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_LCK>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:GPRSLOCK>FALSE</hss:GPRSLOCK>
      </hss:MOD_LCK>
   </soapenv:Body>
</soapenv:Envelope>''','''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_CCGLOBAL>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:CHARGE_GLOBAL>PREPAID</hss:CHARGE_GLOBAL>
      </hss:MOD_CCGLOBAL>
   </soapenv:Body>
</soapenv:Envelope>''')

    def moveHis(self,conn,ps):
        msg = 'move to his: %s' % ps.psId
        self.cfg.logWriter.writeLog('DEBUG', msg)
        cur = conn.cursor()
        cur.execute("insert into EMPS_PROVISION_HIS select * from EMPS_PROVISION where ps_id=:1", (ps.psId,))
        cur.execute("delete from EMPS_PROVISION where ps_id=:1", (ps.psId,))
        conn.commit()
        cur.close()

    def makeInstruct(self,ps):
        insKey = ps.psServiceCode
        self.cfg.logWriter.writeLog('DEBUG', 'service code: %s' % insKey)
        reqMsges = self.dTemplate[insKey]
        newMsges = []
        for reqMsg in reqMsges:
            newMsg = reqMsg.replace('^<GLOBAL.PASSWD^>', self.passwd)
            newMsg = newMsg.replace('^<GLOBAL.USER^>', self.user)
            newMsg = newMsg.replace('^<GLOBAL.HLRSN^>', self.hlrsn)
            newMsg = newMsg.replace('^<IMSI^>', ps.subBillId)
            gprsFlag = '1'
            newMsg = newMsg.replace('^<GPRS_FLAG^>', gprsFlag)
            self.cfg.logWriter.writeLog('DEBUG', 'hss message: %s' % newMsg)
            newMsges.append(newMsg)
        return newMsges

    def setHttpCmd(self,cmd='POST %s HTTP/1.1\r\n'):
        self.cmd = cmd % self.remoteUrl

    def setHead(self,head='default'):
        if head == 'default':
            head = 'Accept: */*\r\nCache-Control: no-cache\r\nConnection: Keep-Alive\r\n' \
                   'Content-Type: text/xml;charset=utf-8\r\n' \
                   'Host: %s:%s\r\n' \
                   'Soapaction: \"\"\r\n' \
                   'User-Agent: Java/1.5.0_07\r\n' % (self.host,self.port)
            head += 'Content-Length: %d\r\n\r\n'
        self.cfg.logWriter.writeLog('DEBUG', 'httphead: %s' % head)
        self.head = head

    def login(self):
        return True

    def send(self,message):
        reqLen = len(message)
        reqHead = self.head % reqLen
        reqMsg = '%s%s%s' % (self.cmd,reqHead,message)
        msg = 'send message: %s' % reqMsg
        self.cfg.logWriter.writeLog('DEBUG', msg)
        self.clt.sendall(reqMsg)

    def recv(self):
        try:
            rspMsg = self.clt.recv(self.bufSize)
        except Exception , e:
            self.cfg.logWriter.writeLog('INFO', 'recv error: %s' % e)
            return None
        if not rspMsg:
            self.cfg.logWriter.writeLog('INFO', 'recv error')
            return None
        self.cfg.logWriter.writeLog('DEBUG', rspMsg)
        aHttpRsp = rspMsg.split('\r\n\r\n')
        httpHead = aHttpRsp[0]
        msg = 'recv head: %s' % httpHead
        self.cfg.logWriter.writeLog('DEBUG', msg)
        if len(aHttpRsp) < 2:
            self.cfg.logWriter.writeLog('INFO', 'http recv error')
            return None
        httpBody = aHttpRsp[1]
        if httpHead.find('HTTP/1.1') != 0:
            return('http parse error: %s' % rspMsg)
        self.httpRspHead = HttpRspHead(self.cfg, httpHead)
        lenBody = len(httpBody)
        if 'Content-Length' not in self.httpRspHead.dicHead:
            self.cfg.logWriter.writeLog('INFO', 'http response error, no Content-Length')
            return None
        while len(httpBody) < int(self.httpRspHead.dicHead['Content-Length']):
            httpBody += self.clt.recv(self.bufSize)
        msg = 'recv body: %s' % httpBody
        self.cfg.logWriter.writeLog('DEBUG', msg)
        return httpBody


class HttpRspHead(object):
    def __init__(self,cfg, headmsg):
        self.cfg = cfg
        headParas = headmsg.split('\r\n')
        self.dicHead = {}
        for param in headParas:
            (paName,div,paValue) = param.partition(':')
            self.dicHead[paName] = paValue
        msg = 'http resp head dic: %s' % self.dicHead
        self.cfg.logWriter.writeLog('DEBUG', msg)


class HssHw(HttpClt):
    def __init__(self, cfg, hssname, quhss):
        HttpClt.__init__(self, cfg, hssname, quhss)
        self.exit = None
        # self.quResult = quresult

    def login(self):
        loginBody = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:lgi="http://www.huawei.com/HLR9820/LGI">' \
                    '<soapenv:Header/>' \
                    '<soapenv:Body><lgi:LGI>' \
                    '<lgi:OPNAME>%s</lgi:OPNAME>' \
                    '<lgi:PWD>%s</lgi:PWD>' \
                    '<lgi:HLRSN>%s</lgi:HLRSN>' \
                    '</lgi:LGI></soapenv:Body></soapenv:Envelope>' % (self.user,self.passwd,self.hlrsn)
        msg = 'login %s: %s' % (self.netName,loginBody)
        self.cfg.logWriter.writeLog('INFO', msg)
        self.send(loginBody)
        try:
            rspMsg = self.recv()
        except Exception, e:
            msg = '%s login failed. %s' % (self.netName,e)
            self.cfg.logWriter.writeLog('DEBUG', msg)
            return False
        msg = 'login recv: %s' % (rspMsg)
        self.cfg.logWriter.writeLog('INFO', msg)
        if 'Location' not in self.httpRspHead.dicHead:
            self.cfg.logWriter.writeLog('INFO', 'http login failed, no Location')
            return False
        location = self.httpRspHead.dicHead['Location']
        self.remoteUrl = location[location.rfind('/'):]
        self.cfg.logWriter.writeLog('INFO', 'remoeUrl: %s' % self.remoteUrl)
        return True
        # aHttpRsp = rspMsg.split('\r\n\r\n')
        # print 'http head: %s' % aHttpRsp[0]
        # print 'http body: %s' % aHttpRsp[1]

    def parseResp(self,rsp):
        if rsp.find('<ResultCode>0</ResultCode>') > -1:
            return True
        else:
            return False
    # def send(self,msg):
    #     # self.setHttpCmd()
    #     super(self.__class__,self).send(msg)

class HssNsn(HttpClt):
    def __init__(self, cfg, hssname, quhss):
        HttpClt.__init__(self, cfg, hssname, quhss)
        self.exit = None
            # self.quResult = quresult
    def parseResp(self,rsp):
        if rsp.find('<ResultCode>0</ResultCode>') > -1:
            return True
        else:
            return False
# test part
def testHssNsn():
    # hssnsn2 13810536523 460000333145846
    msisdn = '13810536523'
    imsi = '460000333145846'
    hlrsn = '2'
    # host = '10.7.5.164'
    host = '10.4.136.151'
    port = 7777
    remoteUrl = '/'
    user = 'boss1'
    passwd = 'boss@123'
    hssclt = HttpClt(host, port, remoteUrl, user, passwd)
    hssclt.setHttpCmd()
    hssclt.setHead()
    print hssclt.cmd, hssclt.head
    reqMsg = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_LCK>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:IC>FALSE</hss:IC>
         <hss:OC>FALSE</hss:OC>
         <hss:GPRSLOCK>^<GPRS_FLAG^></hss:GPRSLOCK>
      </hss:MOD_LCK>
   </soapenv:Body>
</soapenv:Envelope>'''
    reqMsg = reqMsg.replace('^<GLOBAL.PASSWD^>',passwd)
    reqMsg = reqMsg.replace('^<GLOBAL.USER^>', user)
    reqMsg = reqMsg.replace('^<GLOBAL.HLRSN^>', hlrsn)
    reqMsg = reqMsg.replace('^<IMSI^>', imsi)
    gprsFlag = '1'
    reqMsg = reqMsg.replace('^<GPRS_FLAG^>', gprsFlag)
    print reqMsg
    hssclt.send(reqMsg)
    hssclt.recv()

def testHssHw():
    # hsshw8 15801147472 460028010685931
    msisdn = '15801147472'
    imsi = '460028010685931'
    hlrsn = '8'
    # host = '10.7.5.164'
    host = '10.4.137.207'
    port = 8002
    remoteUrl = '/'
    user = 'boss'
    passwd = 'cnp200@HW'
    quOrder = multiprocessing.Queue(20)
    quResult = multiprocessing.Queue(20)
    hssHw1 = HssHw(host, port, remoteUrl, user, passwd, quOrder, quResult)
    hssHw1.setHttpCmd()
    hssHw1.setHead()
    print hssHw1.cmd, hssHw1.head
    hssHw1.login(hlrsn)
    reqMsg = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:hss="http://www.chinamobile.com/HSS/">
   <soapenv:Header>
      <hss:PassWord>^<GLOBAL.PASSWD^></hss:PassWord>
      <hss:UserName>^<GLOBAL.USER^></hss:UserName>
   </soapenv:Header>
   <soapenv:Body>
      <hss:MOD_LCK>
         <hss:HLRSN>^<GLOBAL.HLRSN^></hss:HLRSN>
         <hss:IMSI>^<IMSI^></hss:IMSI>
         <hss:IC>FALSE</hss:IC>
         <hss:OC>FALSE</hss:OC>
         <hss:GPRSLOCK>^<GPRS_FLAG^></hss:GPRSLOCK>
      </hss:MOD_LCK>
   </soapenv:Body>
</soapenv:Envelope>'''
    reqMsg = reqMsg.replace('^<GLOBAL.PASSWD^>',passwd)
    reqMsg = reqMsg.replace('^<GLOBAL.USER^>', user)
    reqMsg = reqMsg.replace('^<GLOBAL.HLRSN^>', hlrsn)
    reqMsg = reqMsg.replace('^<IMSI^>', imsi)
    gprsFlag = '1'
    reqMsg = reqMsg.replace('^<GPRS_FLAG^>', gprsFlag)
    print 'test hsshw msg: %s' % reqMsg
    hssHw1.start()
    quOrder.put(reqMsg)
    # hssHw1.send(reqMsg)
    # hssHw1.recv()
    quOrder.put(reqMsg)
    quOrder.put(None)
    hssHw1.join()


if __name__ == '__main__':
    # testHssNsn()
    testHssHw()


