#!/usr/bin/env python

"""socket server"""

import sys
import os
import time
import getopt
import copy
import random
import re
import signal
import struct
import logging
import multiprocessing
from socket import *
import BaseHTTPServer
import urllib
import json
import base64
import SocketServer


# HOST = ''
# PORT = 8787
# ADDR = (HOST,PORT)


class MsHander(SocketServer.StreamRequestHandler):
    loginFmt = '<3IH12s12s'
    loginRspFmt = '<3IH12sB'
    cmdFmt = '<3IH%ds'
    cmdRspFmt = '<3IHB'

    headFmt = '<3IH'

    loginBodyFmt = '<12s12s'
    loginRspBodyFmt = '<12sB'
    cmdBodyFmt = '<%ds'
    cmdRspBodyFmt = '<B'
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
        fLength, fMsgType, fSquence, fClientId, fUser, fPasswd = struct.unpack(self.loginFmt, buff)
        print 'login:%d %d %d %d %s %s' % (fLength, fMsgType, fSquence, fClientId, fUser, fPasswd)
        loginStatus = 0
        fSquence = 1
        fClientId = 2
        fRspLength = 27
        buffRsp = struct.pack(self.loginRspFmt, fRspLength, fMsgType, fSquence, fClientId, fUser, loginStatus)
        print 'login resp:%d %d %d %d %s %d' % (fRspLength, fMsgType, fSquence, fClientId, fUser, loginStatus)
        print 'login resp:%s' % repr(buffRsp)
        self.request.sendall(buffRsp)

    def doCmd(self):
        buff = self.request.recv(14)
        print 'cmd head:%s' % repr(buff)
        fLength, fMsgType, fSquence, fClientId = struct.unpack(self.headFmt, buff)
        print 'cmd head:%d %d %d %d' % (fLength, fMsgType, fSquence, fClientId)
        cmdLengtgh = fLength - 14
        cmdFmt = self.cmdBodyFmt % cmdLengtgh
        buff = self.request.recv(cmdLengtgh)
        print 'cmd:%s' % repr(buff)
        cmd = struct.unpack(cmdFmt, buff)
        print 'request:%d %d %d %d %s' % (fLength, fMsgType, fSquence, fClientId, cmd)
        rspBuff = struct.pack(self.cmdRspFmt, 15, fMsgType, fSquence, fClientId, 0)
        print 'response:%d %d %d %d %d' % (15, fMsgType, fSquence, fClientId, 0)
        print 'cmd resp:%s' % repr(rspBuff)
        self.request.sendall(rspBuff)

    def logout(self):
        buff = self.request.recv(14)
        print 'logout:%s' % repr(buff)
        fLength, fMsgType, fSquence, fClientId = struct.unpack(self.headFmt, buff)
        print 'request:%d %d %d %d' % (fLength, fMsgType, fSquence, fClientId)
        rspBuff = struct.pack(self.cmdRspFmt, 15, fMsgType, fSquence, fClientId, 0)
        print 'response:%d %d %d %d %d' % (15, fMsgType, fSquence, fClientId, 0)
        print 'logout resp:%s' % repr(rspBuff)
        self.request.sendall(rspBuff)


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


# class DbConn(object):
#     def __init__(self, dbInfo):
#         self.dbInfo = dbInfo
#         self.conn = None
#         # self.connectServer()
#
#     def connectServer(self):
#         if self.conn: return self.conn
#         # if self.remoteServer: return self.remoteServer
#         connstr = '%s/%s@%s/%s' % (self.dbInfo['dbusr'], self.dbInfo['dbpwd'], self.dbInfo['dbhost'], self.dbInfo['dbsid'])
#         try:
#             self.conn = orcl.Connection(connstr)
#             # dsn = orcl.makedsn(self.dbHost, self.dbPort, self.dbSid)
#             # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
#             # self.conn = orcl.connect(self.dbUser, self.dbPwd, dsn)
#         except Exception, e:
#             logging.fatal('could not connect to oracle(%s:%s/%s), %s', self.cfg.dbinfo['dbhost'], self.cfg.dbinfo['dbusr'], self.cfg.dbinfo['dbsid'], e)
#             exit()
#         return self.conn
#
#     def prepareSql(self, sql):
#         logging.info('prepare sql: %s', sql)
#         cur = self.conn.cursor()
#         try:
#             cur.prepare(sql)
#         except orcl.DatabaseError, e:
#             logging.error('prepare sql err: %s', sql)
#             return None
#         return cur
#
#     def executemanyCur(self, cur, params):
#         logging.info('execute cur %s', cur.statement)
#         try:
#             cur.executemany(None, params)
#         except orcl.DatabaseError, e:
#             logging.error('execute sql err %s:%s ', e, cur.statement)
#             return None
#         return cur
#
#     def fetchmany(self, cur):
#         logging.debug('fetch %d rows from %s', cur.arraysize, cur.statement)
#         try:
#             rows = cur.fetchmany()
#         except orcl.DatabaseError, e:
#             logging.error('fetch sql err %s:%s ', e, cur.statement)
#             return None
#         return rows
#
#     def fetchone(self, cur):
#         logging.debug('fethone from %s', cur.statement)
#         try:
#             row = cur.fetchone()
#         except orcl.DatabaseError, e:
#             logging.error('execute sql err %s:%s ', e, cur.statement)
#             return None
#         return row
#
#     def fetchall(self, cur):
#         logging.debug('fethone from %s', cur.statement)
#         try:
#             rows = cur.fetchall()
#         except orcl.DatabaseError, e:
#             logging.error('execute sql err %s:%s ', e, cur.statement)
#             return None
#         return rows
#
#     def executeCur(self, cur, params=None):
#         logging.info('execute cur %s', cur.statement)
#         try:
#             if params is None:
#                 cur.execute(None)
#             else:
#                 cur.execute(None, params)
#         except orcl.DatabaseError, e:
#             logging.error('execute sql err %s:%s ', e, cur.statement)
#             return None
#         return cur


class Director(object):
    pass


class HttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    # pattCmdReq = r'<soapenv:Body>\W*<hss:(\w+)>'
    def _set_headers(self, headKey):
        dHeader = self.server.respHead[headKey]
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def _json_encode(self, data):
        array = data.split('&')
        json_data = {}
        for item in array:
            item = item.split('=', 1)
            json_data[item[0]] = item[1]
        return json_data

    def _get_handler(self, data):
        json_data = self._json_encode(data)

    def _post_handler(self, data):
        retVal = {}
        json_data = self._json_encode(data)
        file_name = json_data['FileName']
        file_data = base64.b64decode(json_data['FileData'])
        file_path = "%s/%s" % ('/tmp', file_name)
        fd = open(file_path, 'w')
        fd.write(file_data)
        fd.close()
        retVal["RetCode"] = 0
        return json.dumps(retVal)

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        self._set_headers()
        # get request params
        path = self.path
        query = urllib.splitquery(path)
        self._get_handler(query[1]);

    def do_POST(self):
        # get post data
        post_data = self.rfile.read(int(self.headers['content-length']))
        # post_data = urllib.unquote(post_data).decode("utf-8", 'ignore')
        logging.debug('get request from %s %s %d %s', self.client_address, self.request_version, self.close_connection, self.version_string())
        logging.debug(post_data)
        aPtCmd = self.server.servInfo['PCMD']
        for pt in aPtCmd:
            pt = eval(pt)
            logging.debug('cmd pattern: %s', pt)
            # pt = ':Body>\s*<m:(\w+?)[\s>]'
            m = re.search(pt, post_data)
            if m: break
        reqCmd = None
        if m:
            reqCmd = m.group(1)
        else:
            self.send_error(400, 'no find request cmd')
            return reqCmd
        logging.debug('get request cmd: %s', reqCmd)
        rspHeader = self.makeHeader(reqCmd)
        rspBody = self.makeRspBody(post_data, reqCmd)
        if not rspHeader:
            self.send_error(400)
            return
        if not rspBody:
            rspHeader['Content-Length'] = '%d' % 0
            logging.debug(rspHeader)
            self.sendHeader(rspHeader)
            return
        rspHeader['Content-Length'] = '%d' % len(rspBody)
        logging.debug(rspHeader)
        logging.debug(rspBody)
        msgRsp = self.makeRspMsg(rspHeader, rspBody)
        self.wfile.write(msgRsp)

    def makeRspMsg(self, dHeader, rspBody):
        rspCode = int(dHeader.pop('Resp-Code'))
        msgHeader = "%s %d %s\r\n" % (self.protocol_version, rspCode, self.responses[rspCode][0])
        for key in dHeader:
            msgHeader = '%s%s: %s\r\n' % (msgHeader, key, dHeader[key])
        msgRsp = '%s\r\n%s' % (msgHeader, rspBody)
        logging.debug((msgRsp))
        return msgRsp

    def sendHeader(self, dHeader):
        rspCode = dHeader.pop('Resp-Code')
        # rspCode = dHeader['Resp-Code']
        logging.debug('response %s', rspCode)
        self.send_response(int(rspCode))
        for key in dHeader:
            # if key == 'Resp-Code': continue
            logging.debug('%s: %s', key, dHeader[key])
            self.send_header('Server','Huawei web server')
            self.send_header(key, dHeader[key])
        self.send_header('Server', 'Huawei web server')
        self.end_headers()

    def getRspMap(self, reqCmd):
        if reqCmd in self.server.respMap:
            rspMap = self.server.respMap[reqCmd]
            logging.debug('rspMap: %s', rspMap)
            return rspMap
        reqCmd = 'OTHER'
        if reqCmd in self.server.respMap:
            rspMap = self.server.respMap[reqCmd]
            logging.debug('rspMap: %s', rspMap)
            return rspMap
        return None

    def makeHeader(self, reqCmd):
        rspMap = self.getRspMap(reqCmd)
        if rspMap:
            headKey = rspMap[0]
            logging.debug('head key %s', headKey)
        else:
            logging.info('no find rspMap')
            return None
        if headKey in self.server.respHead:
            dHeader = copy.deepcopy(self.server.respHead[headKey])
            logging.debug('header: %s', dHeader)
        else:
            logging.debug('no find header %s : %s', headKey, self.server.respHead)
            return None
        for headerKey in dHeader:
            if headerKey == 'Location':
                value = dHeader[headerKey] % self.client_address
                dHeader[headerKey] = value
        logging.debug(dHeader)
        return dHeader

    def makeRspBody(self, reqData, reqCmd):
        rspMap = self.getRspMap(reqCmd)
        if rspMap and len(rspMap) == 2:
            bodyKey = rspMap[1]
            logging.debug('bodyKey: %s', bodyKey)
        else:
            logging.debug('no find bodyKey')
            return None
        if bodyKey in self.server.respBody:
            rspBody = copy.deepcopy(self.server.respBody[bodyKey])
            logging.debug(rspBody)
        else:
            logging.debug('no find response body')
            return None
        aPattIsdnReq = self.server.servInfo['PISDNREQ']
        isdn = self.getValue(aPattIsdnReq, reqData)
        aPattIsdnRsp = self.server.servInfo['PISDNRSP']
        if isdn:
            rspBody = self.subValue(aPattIsdnRsp,isdn, rspBody)
        aPattImsiReq = self.server.servInfo['PIMSIREQ']
        imsi = self.getValue(aPattImsiReq, reqData)
        aPattImsiRsp = self.server.servInfo['PIMSIRSP']
        if imsi:
            rspBody = self.subValue(aPattImsiRsp, imsi, rspBody)
        logging.debug(rspBody)
        return rspBody

    def subValue(self, aPatt, val, data):
        for pt in aPatt:
            ptValue = pt.replace('(\d+)', val)
            rspValue = re.subn(pt, ptValue, data)
            if rspValue[1] > 0:
                return rspValue[0]
        return data

    def getValue(self, aPatt, data):
        m = None
        for pt in aPatt:
            m = re.search(pt, data)
            if m: return m.group(1)
        return m

        

class VirNetFac(object):
    def __init__(self, main, netfile):
        self.main = main
        self.netFile = netfile
        self.rows = []
        self.servInfo = {}
        self.respHead = {}
        self.respBody = {}
        self.respMap = {}

    def readNet(self):
        fNet = self.main.openFile(self.netFile, 'r')
        self.rows = fNet.readlines()
        fNet.close()

    def readNetInfo(self):
        section  = None
        respHeadKey = None
        for i in range(len(self.rows)):
            row = self.rows[i]
            row = row.strip()
            if len(row) < 1:
                # section = None
                continue
            if row == '#service_port':
                section = 'service_port'
                continue
            elif row == '#request command':
                section = 'request command'
                continue
            elif row == '#response head':
                section = 'response head'
                continue
            elif row == '#response body':
                section = 'response body'
                continue
            elif row == '#mapping of request and response':
                section = 'mapping of request and response'
                continue
            if section == 'service_port':
                aRow = row.split()
                if len(aRow) < 2:
                    logging.error('error server info: %s', row)
                else:
                    if aRow[0] in self.servInfo:
                        self.servInfo[aRow[0]].append(aRow[1])
                    else:
                        self.servInfo[aRow[0]] = []
                        self.servInfo[aRow[0]].append(aRow[1])
                continue
            if section == 'request command':
                pass
            if section == 'response head':
                aRow = row.split(': ')
                if aRow[0] == 'HEAD_KEY':
                    self.respHead[aRow[1]] = {}
                    respHeadKey = aRow[1]
                else:
                    self.respHead[respHeadKey][aRow[0]] = aRow[1]
                continue
            if section == 'response body':
                i += 1
                self.respBody[row] = self.rows[i]
                continue
            if section == 'mapping of request and response':
                aRow = row.split()
                if len(aRow) < 2:
                    logging.error('error request response map info: %s', row)
                self.respMap[aRow[0]] = aRow[1:]
                continue
        logging.info(self.servInfo)

    def makeServer(self):
        host = ''
        addr = (host, int(self.servInfo['PORT'][0]))
        serverType = self.servInfo['SERV'][0]
        if serverType == 'SOCKET':
            neServer = SocketServer.ThreadingTCPServer(addr, MsHander)
        elif serverType == 'HTTP':
            neServer = BaseHTTPServer.HTTPServer(addr, HttpHandler)
        neServer.servInfo = self.servInfo
        neServer.respHead = self.respHead
        neServer.respBody = self.respBody
        neServer.respMap = self.respMap
        return neServer


    def makeHandler(self):
        pass


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.fNet = None
        self.netFile = None

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
        logName = '%s_%s.log' % (self.netFile, self.today)
        logNamePre = '%s_%s' % (self.netFile, self.today)
        # outFileName = '%s_%s' % (os.path.basename(self.dsIn), self.today)
        self.cfgFile = os.path.join(self.dirCfg, cfgName)
        self.logFile = os.path.join(self.dirLog, logName)
        self.logPre = os.path.join(self.dirLog, logNamePre)
        # self.outFile = os.path.join(self.dirOutput, outFileName)

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.appName = appName
        if self.argc < 2:
            self.usage()
        self.netFile = sys.argv[1]

    def usage(self):
        print "Usage: %s netcfg" % self.appName
        print "example:   %s %s" % (self.appName,'hsshwcfg')
        exit(1)

    def openFile(self, fileName, mode):
        try:
            f = open(fileName, mode)
        except IOError, e:
            logging.fatal('open file %s error: %s', fileName, e)
            return None
        return f

    # def connectServer(self):
    #     if self.conn is not None: return self.conn
    #     self.conn = DbConn(self.cfg.dbinfo)
    #     self.conn.connectServer()
    #     return self.conn

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

    # def prepareSql(self, sql):
    #     logging.info('prepare sql: %s', sql)
    #     cur = self.conn.cursor()
    #     try:
    #         cur.prepare(sql)
    #     except orcl.DatabaseError, e:
    #         logging.error('prepare sql err: %s', sql)
    #         return None
    #     return cur

    def makeFactory(self):
        if self.facType == 't':
            return self.makeTableFactory()
        elif self.facType == 'f':
            return self.makeFileFactory()

    # def makeTableFactory(self):
    #     self.netType = 'KtPs'
    #     self.netCode = 'kt4'
    #     logging.info('net type: %s  net code: %s', self.netType, self.netCode)
    #     fac = TableFac(self)
    #     return fac

    # def makeFileFactory(self):
    #     if not self.fCmd:
    #         self.fCmd = self.openFile(self.cmdFile, 'r')
    #         logging.info('cmd file: %s', self.cmdFile)
    #         if not self.fCmd:
    #             logging.fatal('can not open command file %s. exit.', self.cmdFile)
    #             exit(2)
    #
    #     for line in self.fCmd:
    #         if line[:8] == '#NETTYPE':
    #             aType = line.split()
    #             self.netType = aType[2]
    #         if line[:8] == '#NETCODE':
    #             aCode = line.split()
    #             self.netCode = aCode[2]
    #         if self.netType and self.netCode:
    #             logging.info('net type: %s  net code: %s', self.netType, self.netCode)
    #             break
    #     logging.info('net type: %s  net code: %s', self.netType, self.netCode)
    #     if self.netType is None:
    #         logging.fatal('no find net type,exit.')
    #         exit(3)
    #     # facName = '%sFac' % self.netType
    #     # fac_meta = getattr(self.appNameBody, facName)
    #     # fac = fac_meta(self)
    #     fac = FileFac(self)
    #     return fac

    @staticmethod
    def createInstance(module_name, class_name, *args, **kwargs):
        module_meta = __import__(module_name, globals(), locals(), [class_name])
        class_meta = getattr(module_meta, class_name)
        obj = class_meta(*args, **kwargs)
        return obj

    def start(self):
        self.checkArgv()
        self.parseWorkEnv()

        # self.cfg = Conf(main, self.cfgFile)
        # self.logLevel = self.cfg.loadLogLevel()
        self.logLevel = logging.DEBUG
        logging.basicConfig(filename=self.logFile, level=self.logLevel, format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y%m%d%I%M%S')
        logging.info('%s starting...' % self.appName)
        print('logfile: %s' % self.logFile)

        # self.cfg.loadDbinfo()
        # self.connectServer()
        # self.cfg.loadNet()
        factory = VirNetFac(self, self.netFile)
        factory.readNet()
        factory.readNetInfo()
        server = factory.makeServer()
        server.serve_forever()



# neServer = SocketServer.ThreadingTCPServer(ADDR, NeHander)
# print 'waiting for connection...'
# neServer.serve_forever()

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