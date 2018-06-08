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


class Main(object):
    def __init__(self):
        self.Name = sys.argv[0]
        self.argc = len(sys.argv)
        self.host = None
        self.user = 'ktrun'
        self.passwd = 'passwd'
        self.original_prompt=r"[#$%]"

    def checkArgv(self):
        dirBin, appName = os.path.split(self.Name)
        self.dirBin = dirBin
        self.appName = appName
        if self.argc < 2:
            self.usage()
        self.host = sys.argv[1]
        if self.argc > 2:
            self.user = sys.argv[2]
        if self.argc > 3:
            self.passwd = sys.argv[3]

    def usage(self):
        print "Usage: %s host [user] [passwd]" % self.appName
        print "example:   %s %s %s" % (self.appName,'wbkt1', 'ktrun', 'passwd')
        exit(1)

    def start(self):
        self.checkArgv()
        self.login()

    def login(self):
        cmd = 'ssh %s@%s' % (self.user, self.host)
        child = pexpect.spawn(cmd)
        i = child.expect(["(?i)are you sure you want to continue connecting", self.original_prompt,
                          "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                          pexpect.TIMEOUT, "(?i)connection closed by remote host", pexpect.EOF])
        # First phase
        if i == 0:
            # New certificate -- always accept it.
            # This is what you get if SSH does not have the remote host's
            # public key stored in the 'known_hosts' cache.
            child.sendline("yes")
            i = child.expect(["(?i)are you sure you want to continue connecting", self.original_prompt,
                              "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                              pexpect.TIMEOUT])
        if i == 2:  # password or passphrase
            child.sendline(self.passwd)
            i = child.expect(["(?i)are you sure you want to continue connecting", self.original_prompt,
                              "(?i)(?:password)|(?:passphrase for key)", "(?i)permission denied", "(?i)terminal type",
                              pexpect.TIMEOUT])
        if i == 4:
            child.sendline('vt100')
            i = child.expect(["(?i)are you sure you want to continue connecting", self.original_prompt,
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


if __name__ == '__main__':
    main = Main()
    main.start()
    # logging.info('%s complete.', main.appName)