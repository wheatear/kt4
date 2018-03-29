#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""psh.py: python ssh """
######################################################################
## Filename:      psh.py
##
## Version:       1.0
## Author:        wangxintian <wangxt5@asiainfo.com>
## Created at:
##
## Description:
## 备注:
##
######################################################################

import sys
import os
import time
import datetime
import getopt
import re
import signal
import logging
import socket
import multiprocessing
import sqlite3
import pexpect


class ReHost(object):
    def __init__(self, hostName, hostIp):
        self.hostName = hostName
        self.hostIp = hostIp
        self.dUser = {}

    def setUser(self, user, passwd, prompt):
        self.dUser[user] = (passwd, prompt)


class Psh(object):
    def __init__(self, main):
        self.main = main
        self.hostName = main.host
        self.user = main.user
        self.passwd = None
        self.hostPre = main.hostPre
        self.fullHostName = '%s%s' % (self.hostPre, self.hostName)
        self.dHosts = {}

    def getAllHosts(self):
        dbfile = os.path.join(self.main.dirBin, self.main.dbFile)
        conn = sqlite3.connect(dbfile)
        # print('db: %s' % dbfile)
        cursor = conn.cursor()
        cursor.execute('SELECT hostname,hostip FROM kthosts')
        rows = cursor.fetchall()
        for row in rows:
            host = ReHost(*row)
            self.dHosts[row[0]] = host
        userSql = 'select hostname,user,passwd,prompt from hostuser'
        cursor.execute(userSql)
        rows = cursor.fetchall()
        for row in rows:
            hostName = row[0]
            user = row[1]
            passwd = row[2]
            prompt = row[3]
            self.dHosts[hostName].setUser(user, passwd, prompt)
        cursor.close()
        conn.close()
        return self.dHosts

    def login(self, original_prompt=r"[#$%]"):
        if self.hostName in self.dHosts:
            self.host = self.dHosts[self.hostName]
        elif self.fullHostName in self.dHosts:
            self.host = self.dHosts[self.fullHostName]
        else:
            print('host error: no host %s' % self.hostName)
            exit(1)
        if self.user in self.host.dUser:
            self.passwd = self.host.dUser[self.user][0]
        else:
            print('user error: no user %s in host %s' % (self.user, self.hostName))
        cmd = 'ssh %s@%s' % (self.user, self.host.hostIp)
        child = pexpect.spawn(cmd)
        i = child.expect(["(?i)are you sure you want to continue connecting", original_prompt, "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type", pexpect.TIMEOUT, "(?i)connection closed by remote host", pexpect.EOF])
        # First phase
        if i == 0:
            # New certificate -- always accept it.
            # This is what you get if SSH does not have the remote host's
            # public key stored in the 'known_hosts' cache.
            child.sendline("yes")
            i = child.expect(["(?i)are you sure you want to continue connecting", original_prompt,
                             "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                             pexpect.TIMEOUT])
        if i == 2:  # password or passphrase
            child.sendline(self.passwd)
            i = child.expect(["(?i)are you sure you want to continue connecting", original_prompt,
                             "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                             pexpect.TIMEOUT])
        if i == 4:
            child.sendline('ansi')
            i = child.expect(["(?i)are you sure you want to continue connecting", original_prompt,
                             "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                             pexpect.TIMEOUT])
        if i == 7:
            child.close()
            raise pexpect.ExceptionPxssh('Could not establish connection to host')

        # Second phase
        if i == 0:
            # This is weird. This should not happen twice in a row.
            child.close()
            raise pexpect.ExceptionPxssh('Weird error. Got "are you sure" prompt twice.')
        elif i == 1:  # can occur if you have a public key pair set to authenticate.
            ### TODO: May NOT be OK if expect() got tricked and matched a false prompt.
            # pass
            output = '%s%s' % (child.before, child.match.group())
            print(output)
        elif i == 2:  # password prompt again
            # For incorrect passwords, some ssh servers will
            # ask for the password again, others return 'denied' right away.
            # If we get the password prompt again then this means
            # we didn't get the password right the first time.
            child.close()
            raise pexpect.ExceptionPxssh('password refused')
        elif i == 3:  # permission denied -- password was bad.
            child.close()
            raise pexpect.ExceptionPxssh('permission denied')
        elif i == 4:  # terminal type again? WTF?
            child.close()
            raise pexpect.ExceptionPxssh('Weird error. Got "terminal type" prompt twice.')
        elif i == 5:  # Timeout
            # This is tricky... I presume that we are at the command-line prompt.
            # It may be that the shell prompt was so weird that we couldn't match
            # it. Or it may be that we couldn't log in for some other reason. I
            # can't be sure, but it's safe to guess that we did login because if
            # I presume wrong and we are not logged in then this should be caught
            # later when I try to set the shell prompt.
            pass
            # print(child.before)
        elif i == 6:  # Connection closed by remote host
            child.close()
            raise pexpect.ExceptionPxssh('connection closed')
        else:  # Unexpected
            child.close()
            raise pexpect.ExceptionPxssh('unexpected login response')

        print(child.buffer)
        child.interact()  # Give control of the child to the user.


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.host = None
        self.user = None
        self.hostPre = 'wb'
        self.dbFile = 'kthosts.db'

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        if self.argc < 2:
            self.usage()
        self.host = sys.argv[1]
        if self.argc > 2:
            self.user = sys.argv[2]
        else:
            self.user = 'ktrun'

    def usage(self):
        print "Usage: %s host [user]" % self.appName
        print "example:   %s %s %s" % (self.appName,'wbkt1', 'ktrun')
        exit(1)

    def start(self):
        self.checkArgv()
        psh = Psh(main)
        psh.getAllHosts()
        psh.login()

if __name__ == '__main__':
    main = Main()
    main.start()
    # logging.info('%s complete.', main.appName)