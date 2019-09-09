#!/usr/bin/env python
'''emergency provisioning ps control model'''

import sys
import os
import multiprocessing
import cx_Oracle as orcl

class PsOrder(object):
    def __init__(self,psid,billid,subbillid,psparam,psstatus,psnetcode=None,faillog=None,psservicecode=None):
        self.psId = psid
        self.billId = billid
        self.subBillId = subbillid
        self.psParam = psparam
        self.psStatus = psstatus
        self.psNetCode = psnetcode
        self.psServiceCode = psservicecode
        self.failLog = faillog
    def updateStatus(self,conn,status):
        upSql = ('update  EMPS_PROVISION set ps_status=:1 where ps_id=:2',(status,self.psId))
        cur = conn.cursor()
        cur.execute(*upSql)
        conn.commit()

    def loader(self):
        self.psId = None
