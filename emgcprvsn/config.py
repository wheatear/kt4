#!/usr/bin/env python
'''emergency provisioning ps control model'''

import sys
import os
import time
import multiprocessing
import cx_Oracle as orcl

import loader
import spliter
import netinter


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
        self.db = None
        self.logWriter = None

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
    def getDb(self):
        if self.db: return self.db
        try:
            connstr = '%s/%s@%s/%s' % (self.dbusr,self.pawd,self.host,self.sid)
            # dsn = orcl.makedsn(self.host, self.port, self.sid)
            # dsn = dsn.replace('SID=', 'SERVICE_NAME=')
            self.db = orcl.Connection(connstr)
        # cursor = con.cursor()
        except Exception, e:
            print 'could not connec to oracle(%s:%s/%s), %s' % (self.host, self.port, self.sid, e)
            exit()
        return self.db

    def closeDb(self):
        self.db.close()


class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kw)
        return cls._instance


class Log(multiprocessing.Process):
    'Log class'
    def __init__(self, conf,qulog):
        multiprocessing.Process.__init__(self)
        if not hasattr(self, 'fLog'):
            self.dLevel = conf.dLevel
            self.level = self.dLevel[conf.level]
            self.logName = conf.logFile
            self.fLog = open(self.logName, 'a')
            self.quLog = qulog
            self.exit = None
    def run(self):
        while not self.exit:
            # print('psqueue size: %d' % (self.quLog.qsize()))
            (pid,logType, message) = self.quLog.get()
            if self.dLevel[logType] < self.level:
                return
            sTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
            msg = '%s %s %s %s%s' % (sTime, pid, logType, message, '\n')
            self.fLog.write(msg)
            print msg
            self.fLog.flush()
            if message is None:
                print 'exit'
                self.close()
                break
    def close(self):
        self.fLog.close()


class LogWriter(object):
    def __init__(self,qlog):
        self.qLog = qlog
        self.pid = os.getpid()
    def writeLog(self,logtype, message):
         self.qLog.put((self.pid, logtype, message))
         print message

