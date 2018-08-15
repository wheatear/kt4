#!/usr/bin/env python

'''query subscribe infomation from hss'''

import sys
import os

from socket import *

ls = os.linesep


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
        self.tcpClt.sendall(reqMsg)

    def recv(self):
        rspMsg = self.tcpClt.recv(self.bufSize)
        rcvLen = len(rspMsg)
        rspLen = int(rspMsg[0:8])
        while rcvLen < rspLen:
            rspMsg = '%s%s' % (rspMsg, self.tcpClt.recv(self.bufSize))
            rcvLen = len(rspMsg)
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
        if self.argc < 2:
            self.usage()
        self.msisdn = sys.argv[1]
        self.imsi = sys.argv[1]
        if self.argc > 2: self.imsi = sys.argv[2]
        self.outPa = None
        if self.argc > 3:
            self.outPa = sys.argv[3:]

    #		self.logFile = '%s%s' % (self.cmdName,'.log')
    #		self.cfgFile = '%s.cfg' % self.Name

    #	def getConf(self):
    #		cfg = Conf(self.cfgFile)
    #		cfg.setLogFile(self.logFile)
    #		log = Log(cfg)
    ##		self.log = log
    #		log.writeLog('DEBUG',vars(cfg))
    #		self.ktDs = cfg.getDs()

    def startProc(self):
        HOST = '10.4.72.74'
        PORT = 16801
        BUFSIZ = 1024
        tcpClt = TcpClt(HOST, PORT, BUFSIZ)
        qSub = QSub(tcpClt, self.outPa)
        message = qSub.makeReqMsg(self.msisdn, self.imsi)

        qSub.sendReq(message)
        rspInfo = qSub.recvRsp()
        print rspInfo

    def usage(self):
        print "Usage: %s fileName" % self.baseName
        print "file format: Request Message of BST Extend API"
        exit(1)


# main here
if __name__ == '__main__':
    main = Main()
    main.startProc()
