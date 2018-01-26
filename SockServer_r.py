#!/usr/bin/env python

"""socket server"""

import sys
import os
import time
import getopt
import random
import re
import signal
import struct
import logging
import multiprocessing
from socket import *
import SocketServer


HOST = ''
PORT = 8786
ADDR = (HOST,PORT)



loginFmt = '!3IH12s12s'
loginRspFmt = '!3IH12sB'
cmdFmt = '!3IH%ds'
cmdRspFmt = '!3IHB'

headFmt = '!3IH'

loginBodyFmt = '!12s12s'
loginRspBodyFmt = '!12sB'
cmdBodyFmt = '!%ds'
cmdRspBodyFmt = '!B'

class NeHander(SocketServer.StreamRequestHandler):
    def handle(self):
        self.login()
        self.doCmd()
        self.logout()

    def login(self):
        print '...connected from:', self.client_address
        # reqMsg = self.rfile.readline()
        buff = self.request.recv(38)
        print 'login:%s' % repr(buff)
        reqLen = len(buff)
        print 'login package length:%d ' % reqLen
        fLength, fMsgType, fSquence, fClientId, fUser, fPasswd = struct.unpack(loginFmt, buff)
        print 'login:%d %d %d %d %s %s' % (fLength, fMsgType, fSquence, fClientId, fUser, fPasswd)
        loginStatus = 0
        fSquence = 1
        fClientId = 2
        fRspLength = 27
        buffRsp = struct.pack(loginRspFmt, fRspLength, fMsgType, fSquence, fClientId, fUser, loginStatus)
        print 'login resp:%d %d %d %d %s %d' % (fRspLength, fMsgType, fSquence, fClientId, fUser, loginStatus)
        print 'login resp:%s' % repr(buffRsp)
        self.request.sendall(buffRsp)

    def doCmd(self):
        buff = self.request.recv(14)
        print 'cmd head:%s' % repr(buff)
        fLength, fMsgType, fSquence, fClientId = struct.unpack(headFmt, buff)
        print 'cmd head:%d %d %d %d' % (fLength, fMsgType, fSquence, fClientId)
        cmdLengtgh = fLength - 14
        cmdFmt = cmdBodyFmt % cmdLengtgh
        buff = self.request.recv(cmdLengtgh)
        print 'cmd:%s' % repr(buff)
        cmd = struct.unpack(cmdFmt, buff)
        print 'request:%d %d %d %d %s' % (fLength, fMsgType, fSquence, fClientId, cmd)
        rspBuff = struct.pack(cmdRspFmt, 15, fMsgType, fSquence, fClientId, 0)
        print 'response:%d %d %d %d %d' % (15, fMsgType, fSquence, fClientId, 0)
        print 'cmd resp:%s' % repr(rspBuff)
        self.request.sendall(rspBuff)

    def logout(self):
        buff = self.request.recv(14)
        print 'logout:%s' % repr(buff)
        fLength, fMsgType, fSquence, fClientId = struct.unpack(headFmt, buff)
        print 'request:%d %d %d %d' % (fLength, fMsgType, fSquence, fClientId)
        rspBuff = struct.pack(cmdRspFmt, 15, fMsgType, fSquence, fClientId, 0)
        print 'response:%d %d %d %d %d' % (15, fMsgType, fSquence, fClientId, 0)
        print 'logout resp:%s' % repr(rspBuff)
        self.request.sendall(rspBuff)



neServer = SocketServer.ThreadingTCPServer(ADDR, NeHander)
print 'waiting for connection...'
neServer.serve_forever()
