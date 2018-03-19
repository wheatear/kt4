#!/usr/bin/env python
"""emergency provisioning ps control model"""

import sys
import os
import time
import multiprocessing
import pexpect
import pexpect.pxssh
import threading
import socket
import base64
import logging
import re

class Spread(object):
    def __init__(self,cfgfile):
        self.cfgfile = cfgfile
        self.aHost = []
        self.dLOGLEVEL = {'DEBUG': 1, 'INFO': 2, 'WARN': 3, 'ERROR': 4, 'FATAL': 5}
        self.LOGLEVEL = 'DEBUG'

    def spreadFile(self,remotedir,files):
        sucfile = []
        failfile = []
        # logging.info('put file %s to %s', files ,self.aHost)
        for h in self.aHost:
            if h[0] == self.localIp:
                continue
            logging.info('put to host: %s',h)
            logging.info('put files: %s', files)
            putfile = self.putFile(files, h[2], h[3], h[0], remotedir)
            if putfile:
                sucfile.append((h[0], files))
                logging.debug('success spread to %s file: %s', h[0], files)
            else:
                failfile.append((h[0], files))
                logging.debug('fail spread to %s file: %s', h[0], files)
        logging.info('success spread %d files: %s', len(sucfile), sucfile)
        logging.info('fail spread %d files: %s', len(failfile), failfile)

    def putFile(self,lfile,ruser,rpasswd,rhost,rpath):
        cmdUser = 'nms'
        cmdPwd = 'ailk,123'
        cmd = 'scp -pr %s %s@%s:%s' % (lfile,cmdUser,rhost,rpath)
        logging.info( cmd)
        try:
            pscp = pexpect.spawn(cmd)
            # flog = open('../log/spreadlog.log', 'w')
            pscp.logfile = sys.stdout
            index = pscp.expect(['assword:', pexpect.EOF])
            if index == 0:
                # pscp.sendline(base64.decodestring(rpasswd))
                pscp.sendline(cmdPwd)
            elif index == 1:
                return self.parseResp(pscp)
            # pscp.sendline('Tst,1234')
            fileBase = os.path.basename(lfile)
            index = pscp.expect(['assword:', pexpect.EOF])
            i = 0
            if index == 0:
                logging.warn('passward error for hotst: %s',rhost)
                return False
            elif index == 1:
                return self.parseResp(pscp)

            # elif index == 1:
            #     logging.info('put file %s to %s error:%s',lfile,rhost,pscp.match.group())
            #     return False
            # elif index == 2:
            #     logging.info('put file %s to %s error:%s',lfile,rhost,pscp.match.group())
            #     return False
        except (pexpect.TIMEOUT,pexpect.EOF),e:
            logging.info(pscp.buffer)
            logging.info('scp file %s:%s failed. %s' ,rhost,lfile,e)
            return False
        return True

    def parseResp(self,pscp):
        i = 0
        resp = pscp.before
        reg = re.compile(' \r\n\w(.+)')
        errres = re.findall(reg, resp)
        errnum = len(errres)
        if errnum > 0:
            for erf in errres:
                logging.info(erf)
            logging.info('%d files error', errnum)
            resp = re.sub(reg, '', resp)
        fileret = pscp.before.split('\r\n\r')
        for frt in fileret:
            frt = frt.strip()
            if frt == '':
                continue
            i += 1
            ret = frt.split('\r')
            if len(ret) > 1:
                logging.info(ret[1])
            else:
                logging.info(ret[0])
        logging.info('%d files ok', i)
        # logging.info(pscp.match.group())
        return True

    def getAllHost(self):
        try:
            fCfg = open(self.cfgfile,'r')
        except Exception,e:
            logging.error('can not open host cfg file %s: %s' ,self.cfgfile,e)
            exit(-1)
        for line in fCfg:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            aHostInfo = line.split(' ')
            self.aHost.append(aHostInfo)
        fCfg.close()

    def getLocalIp(self):
        self.hostname = socket.gethostname()
        logging.info('local host: %s' ,self.hostname)
        self.localIp = socket.gethostbyname(self.hostname)
        return self.localIp


def main():
    argc = len(sys.argv)
    if argc > 2:
        remoteHome = sys.argv[1]
        aSpreadDirs = sys.argv[2:]
    else:
        remoteHome = '/nms/scandir'
        aSpreadDirs = ['bin', 'config']

    callName = sys.argv[0]
    baseName = os.path.basename(callName)
    binDir = os.path.dirname(callName)
    if binDir == '':
        binDir = '.'
    if binDir == '.':
        appHome = '..'
    else:
        appHome = os.path.dirname(binDir)
    if appHome == '':
        appHome = '.'
    logDir = '%s/%s' % (appHome, 'log')
    binDir = '%s/%s' % (appHome, 'bin')
    # self.cfgDir = '%s/%s' % (self.appHome,'config')
    cfgDir = '%s/%s' % (appHome, 'config')
    cfgFile = '%s/%s' % (cfgDir, 'hostcluster.cfg')
    sysName = os.path.splitext(baseName)[0]
    logFile = '%s/%s.log' % (logDir, sysName)
    # print 'bindir: %s, apphome: %s, cfgdir: %s' % (binDir, appHome,cfgDir)

    logging.basicConfig(filename=logFile,level=logging.DEBUG,format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y%m%d%I%M%S')
    logging.info('spread file starting...')

    logging.info('put dirs : %s', aSpreadDirs)
    spreader = Spread(cfgFile)
    spreader.getAllHost()
    spreader.getLocalIp()
    logging.info('remote host: %s',spreader.aHost)
    # remoteHome = '/nms/scandir'
    spreaddirs = ' '.join(aSpreadDirs)
    os.chdir(remoteHome)
    spreader.spreadFile(remoteHome, spreaddirs)


    logging.info('spread file completed.')

if __name__ == '__main__':
    main()
