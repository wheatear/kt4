#!/usr/bin/env python
"""emergency provisioning ps control model"""

import sys
import os
import time
import multiprocessing
import signal
import cx_Oracle as orcl

class Loadera(object):
    def __init__(self):

        self.exit = None
        self.sleepTime = 1
        self.maxRowNum = 5
        self.db = orcl.connect('kt/ngboss4,123@10.7.5.164:1521/ngtst02')

    def test(self):
        curQry = self.db.cursor()
        maxRowNum = 5
        sql = 'select to_char(ps_id),bill_id,sub_bill_id,ps_param,ps_status,ps_net_code,fail_log,ps_service_code from EMPS_PROVISION where ps_status=0 and rownum<%d order by create_date,ps_id' % self.maxRowNum
        i = 0
        while not self.exit:
            i += 1
            print('loop %d: %s' % (i, sql))
            if i > 200:
                curQry.close()
                curQry = self.db.cursor()
            curQry.execute(sql)
            rows = curQry.fetchall()
            print 'load %d rows' % curQry.rowcount
            for porder in rows:
                print porder
                ss = porder[0]
                ss2 = porder[3]
                print '%s %s' % (ss,ss2)
                curQry.execute('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2', (1, ss))
                curQry.connection.commit()
            print('sleep %d' % self.sleepTime)


class Loader(object):
    def __init__(self):
        # multiprocessing.Process.__init__(self)
        # self.cfg = cfg
        # self.conn = cfg.conn
        # self.quPs = qusplite
        self.exit = None
        self.sleepTime = 1
        self.maxRowNum = 5
        self.db = orcl.connect('kt/ngboss4,123@10.7.5.164:1521/ngtst02')

    def test(self):
        # self.cfg.logWriter.pid = '%s %s' % (os.getpid(),self.name)
        # self.cfg.getDb()
        # self.conn = self.cfg.db
        # self.conn.autocommit = 1

        # signal.signal(signal.SIGTERM,self.sigHandler)
        curQry = self.db.cursor()
        sql = 'select to_char(ps_id),bill_id,sub_bill_id,ps_param,ps_status,ps_net_code,fail_log,ps_service_code from EMPS_PROVISION where ps_status=0 and rownum<%d order by create_date,ps_id' % self.maxRowNum
        # curUpd = self.conn.cursor()
        # print cur,cur2
        # curQry.prepare(sql)
        # curUpd.prepare('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2')
        i = 0
        j = 0
        while not self.exit:
            i += 1
            # if i > 99:
            #     print( 'cursor close and reconn')
            #     curQry.close()
            #     # curUpd.close()
            #     curQry = self.db.cursor()
            #     # curUpd = self.conn.cursor()
            #
            #     # curUpd.prepare('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2')
            #     # curQry.prepare(sql)
            #     i = 0
            #     j += 1
            print('loop %d: %s' % (i+j*100,sql))
            # print( 'psQueue size: %d' % ( self.quPs.qsize()))
            # try:
            curQry.execute(sql)

            # except orcl.DatabaseError as exc:
            #     error, = exc.args
            #     print('ERROR', "Oracle-Error-Code: %d" %  error.code)
            #     print('ERROR', "Oracle-Error-Message:%s " %  error.message)
            #     print('ERROR', "Oracle-Error-offset:%d " % error.offset)
            #     print('ERROR', "Oracle-Error-context:%s " % error.context)
            #     print('ERROR', "Oracle-Error-isrecoverable:%s " % error.isrecoverable)
            #     print('ERROR','cursor: %s' %  curQry)
            #     print('ERROR','cache size: %d' %  curQry.connection.stmtcachesize)
            #     print('ERROR', 'cur description: %s' % curQry.description)
            #     print('ERROR', 'cur rowcount: %d' % curQry.rowcount)
            #     curQry.close()
            #     # self.conn.close()
            #     # self.cfg.getDb()
            #     # self.conn = self.cfg.db
            #     curQry = self.conn.cursor()
            #     # curQry.prepare(sql)
            #     time.sleep(self.sleepTime)
            #     continue
            curQry.fetchall()
            print( 'load %d rows' % curQry.rowcount)
            # if rows is None:
            #     msg = 'load 0 rows. sleep 10'
            #     print( msg)
            #     time.sleep(self.sleepTime)
            #     continue
            # rowCount = len(rows)
            # msg = 'loading %d rows.' % rowCount
            # print( msg)
            for porder in curQry:
                # print porder
                print( porder)
                ss = porder[0]
                # ps = psorder.PsOrder(*porder)
                # ps.psStatus = 1
                # msg = 'load ps:%s, psQueue size: %d' % (ps.psId, self.quPs.qsize())
                # print( msg)
                # msg = '%s : %d, %s' % (curUpd.statement, ps.psStatus, ps.psId)
                # print( msg)
                curQry.execute('update  EMPS_PROVISION set ps_status=:1,start_date=sysdate where ps_id=:2', (1, ss))
                curQry.connection.commit()
                # self.quPs.put(ps)
            # rows = None
            print( 'sleep %d' % self.sleepTime)
            # time.sleep(self.sleepTime)
        # self.quPs.put(None)

    def sigHandler(self,signum,fram):
        msg = 'I received signal: %d' % signum
        print( msg)
        self.exit = True
        self.quPs.put(None)


if __name__ == '__main__':
    # loader = Loader()
    # loader.test()

    loader = Loadera()
    loader.test()