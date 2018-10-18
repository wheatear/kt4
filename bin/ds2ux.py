#!/usr/bin/env python
import sys,os

if len(sys.argv) < 2:
    print('no input file')
    exit()
pId = os.getpid()
pName = sys.argv[0]

for fl in sys.argv[1:]:
    # if fl == pName:
    #     print('no deal %s' % pName)
    #     continue
    print('deal %s' % fl)
    wkFile = '%s.%s_ing' % (pId,fl)
    fIn = open(fl, 'r')
    fOut = open(wkFile, 'w')
    for line in fIn:
        fOut.write('%s%s' % (line.rstrip('\n').rstrip('\r'), os.linesep))
    fIn.close()
    fOut.close()
    os.chmod(wkFile, os.stat(fl).st_mode)
    os.utime(wkFile, os.stat(fl)[7:9])
    os.rename(wkFile, fl)
