#!/usr/bin/env python
"""emergency provisioning ps control model"""

import sys
import os
import time
import multiprocessing
import signal
import cx_Oracle as orcl
# import psorder
# from config import *

class Loader(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.exit = None
        self.sleepTime = 1
        self.maxRowNum = 5
        self.db = orcl.connect('kt/ngboss4,123@10.7.5.164:1521/ngtst02')

    def run(self):

        curQry = self.db.cursor()
        sql = 'select to_char(ps_id),bill_id,sub_bill_id,ps_param,ps_status,ps_net_code,fail_log,ps_service_code from EMPS_PROVISION where ps_status=0 and rownum<%d order by create_date,ps_id' % self.maxRowNum

        i = 0
        j = 0
        while not self.exit:
            i += 1
            # if i > 200:
            #     curQry.close()
            #     curQry = self.db.cursor()

            print('loop %d: %s' % (i+j*100,sql))
            curQry.execute(sql)
            rows = curQry.fetchall()
            print( 'load %d rows' % curQry.rowcount)

            for porder in rows:
                # print porder
                print porder
                ss = porder[0]
                ss2 = porder[3]
                print '%s %s' % (ss, ss2)
                # curQry.execute('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2', (1, ss))
                # curQry.connection.commit()

            print( 'sleep %d' % self.sleepTime)




if __name__ == '__main__':
    loader = Loader()
    loader.start()
