#!/usr/bin/env python
'''emergency provisioning ps control model'''

import sys
import os
import time
import multiprocessing
import signal
import cx_Oracle as orcl

from config import *
from loader import Loader
from spliter import Spliter
import netinter


class Main(object):
    'main'
    def __init__(self):
        self.Name = sys.argv[0]
        self.baseName = os.path.basename(self.Name)
        self.argc = len(sys.argv)
        # if self.argc < 2:
        #     self.usage()
        # self.cmdName = sys.argv[1]
        self.sysName = os.path.splitext(self.baseName)[0]
        self.logFile = '%s/%s%s' % ('../log',self.sysName, '.log')
        self.cfgFile = '%s.cfg' % self.sysName
        self.cfg = Conf(self.cfgFile)
        self.cfg.logFile = self.logFile
        self.loaders = []
        self.spliters = []
        self.nets = []
    # def getConf(self):
    #     cfg = Conf(self.cfgFile)
    #     qLog = multiprocessing.Queue(200)
    #     log = Log(cfg,qLog)
    #     #		self.log = log
    #     log.writeLog('DEBUG', vars(cfg))
    #     self.ktDs = cfg.getDs()
    def startProc(self):
        quLog = multiprocessing.Queue(200)
        self.log = Log(self.cfg, quLog)
        self.log.name = 'EMGCPS LOG'
        self.log.start()
        logWriter = LogWriter(quLog)
        self.cfg.logWriter = logWriter

        msg = '%s starting...' % self.sysName
        logWriter.writeLog('INFO', msg)
        # logWriter.writeLog('DEBUG', vars(self.cfg))
        # self.cfg.getDb()
        quPs = multiprocessing.Queue(200)
        quHssnsn1 = multiprocessing.Queue(200)
        quHssnsn2 = multiprocessing.Queue(200)
        quHssnsn3 = multiprocessing.Queue(200)
        quHssnsn4 = multiprocessing.Queue(200)
        quHsshw5 = multiprocessing.Queue(200)
        quHsshw6 = multiprocessing.Queue(200)
        quHsshw7 = multiprocessing.Queue(200)
        quHsshw8 = multiprocessing.Queue(200)

        loader = Loader(self.cfg, quPs)
        loader.name = 'LOADER'
        self.loaders.append(loader)
        for ldr in self.loaders:
            ldr.start()

        spliter1 = Spliter(self.cfg, quPs, quHssnsn1,quHssnsn2,quHssnsn3,quHssnsn4,quHsshw5,quHsshw6,quHsshw7,quHsshw8)
        # spliter2 = Spliter(self.cfg, quPs, quHssnsn1,quHssnsn2,quHssnsn3,quHssnsn4,quHsshw5,quHsshw6,quHsshw7,quHsshw8)
        spliter1.name = 'SPLITER1'
        self.spliters.append(spliter1)

        hssNsn1 = netinter.HssNsn(self.cfg, 'HSSNSN1', quHssnsn1)
        hssNsn1.name = 'HSSNSN1'
        self.nets.append(hssNsn1)
        hssNsn2 = netinter.HssNsn(self.cfg, 'HSSNSN2', quHssnsn2)
        hssNsn2.name = 'HSSNSN2'
        self.nets.append(hssNsn2)
        hssNsn3 = netinter.HssNsn(self.cfg, 'HSSNSN3', quHssnsn3)
        hssNsn3.name = 'HSSNSN3'
        self.nets.append(hssNsn3)
        hssNsn4 = netinter.HssNsn(self.cfg, 'HSSNSN4', quHssnsn4)
        hssNsn4.name = 'HSSNSN4'
        self.nets.append(hssNsn4)
        hssHw5 = netinter.HssHw(self.cfg, 'HSSHW5', quHsshw5)
        hssHw5.name = 'HSSHW5'
        self.nets.append(hssHw5)
        hssHw6 = netinter.HssHw(self.cfg, 'HSSHW6', quHsshw6)
        hssHw6.name = 'HSSHW6'
        self.nets.append(hssHw6)
        hssHw7 = netinter.HssHw(self.cfg, 'HSSHW7', quHsshw7)
        hssHw7.name = 'HSSHW7'
        self.nets.append(hssHw7)
        hssHw8 = netinter.HssHw(self.cfg, 'HSSHW8', quHsshw8)
        hssHw8.name = 'HSSHW8'
        self.nets.append(hssHw8)

        for net in self.nets:
            net.start()
        for sp in self.spliters:
            sp.start()
        signal.signal(signal.SIGINT, self.sigHandler)
        signal.signal(signal.SIGQUIT, self.sigHandler)
        signal.signal(signal.SIGTSTP, self.sigHandler)
        signal.signal(signal.SIGTERM, self.sigHandler)

        for ldr in self.loaders:
            ldr.join()
        for spt in self.spliters:
            spt.join()
        for net in self.nets:
            net.join()
        quLog.put(None)
        self.log.join()

    def usage(self):
        print "Usage: %s fileName" % self.baseName
        print "file format: Request Message of BST Extend API"
        exit(1)

    def sigHandler(self,signum,frame):
        msg = '%s recevied signal: %d' % (self.sysName,signum)
        self.cfg.logWriter.writeLog('INFO', msg)
        for ldr in self.loaders:
            ldr.terminate()


if __name__ == '__main__':
    main = Main()

    main.startProc()

    main.cfg.logWriter.writeLog('INFO', '%s %s' % (main.baseName, ' process completed.'))

    # time.sleep(60)
    # main.loader.exit = True
    # main.log.exit = True


