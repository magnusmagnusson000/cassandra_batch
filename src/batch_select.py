import gc
import os.path
import sys
import json
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

if len(sys.argv) < 7:
   print 'Usage:\narguments: <db username> <db password> <name of keyspace> <key-filename> <tablename> <primary-key-column> <batchsize>'
   sys.exit(1)
   
keyfilename = sys.argv[4]
tablename = sys.argv[5]
keycolumn = sys.argv[6]
batchsize = int(sys.argv[7])

auth_prov = PlainTextAuthProvider(username=sys.argv[1], password=sys.argv[2])
cluster = Cluster(auth_provider=auth_prov)
session = cluster.connect(sys.argv[3])

# open file with list of keys to select
f = open(keyfilename)

start_index = 0
# Check if there exists a progress file, this can be used to continue reading from the input file instead of
# starting from the beginning, very useful in case of failures or early exit
if os.path.isfile(keyfilename+'_progress.log'):
   # Get the last row
   ftmp = open(keyfilename+'_progress.log')
   tmpindex = 0
   for r in ftmp:
      if r is not None and r is not '':
         try:
            tmpindex = int(r)
         except:
            pass
   start_index = tmpindex
   ftmp.close()

print 'Starting to read file from:', start_index

rawdata = []

t1 = time.time()
for l in f:
   if l is not None and l != '':
      rawdata.append("\'"+l.strip()+"\'")
   else:
      print 'empty row found:', l
print 'time to read file:', keyfilename, time.time() - t1, 'sec'

print 'nr rows in file:', len(rawdata)

# fout can be used for writing the results to a file, its opened in append mode to allow continuing the execution after failure or normal exit
fout = open(keyfilename+'_out.log', 'a+')
fprogress = open(keyfilename+'_progress.log', 'a+')

progress = start_index

starttime = time.time()

keybatch = []
batchstr = ''
cntr = start_index
for l in rawdata[start_index:]:
   keybatch.append(l)
   cntr += 1
   if cntr % batchsize == 0:
      batchstr = ','.join(keybatch)
      print 'len(keybatch):', len(keybatch)
      sys.stdout.flush()
      del keybatch
      keybatch = []
      cqlstr = "select * from " + tablename + " where " + keycolumn +" IN (" + batchstr + ");"
      batchstr = None
      del batchstr
      rows = session.execute(cqlstr)
      progress += batchsize
      if progress % batchsize == 0:
         t2 = time.time()
         exectime = t2 - starttime
         starttime = time.time()
         print 'Exectime for batchsize:', exectime, ' progress =', progress
         print starttime
      for elem in rows:
         TODO Add your own code here
      fout.flush()
      fprogress.write(str(cntr) + '\n')
      fprogress.flush()
      gc.collect()

# This part handles the leftover keys that did not fill a complete batch
if len(keybatch) > 0:
   batchstr = ''
   batchstr = ','.join(keybatch)
   print 'len(keybatch):', len(keybatch)
   keybatch = []
   cqlstr = "select * from " + tablename + " where " + keycolumn +" IN (" + batchstr + ");"
   batchstr = None
   del batchstr
   rows = session.execute(cqlstr)
   for elem in rows:
      TODO Add your own code here
   fout.flush()

fprogress.write(str(cntr) + '\n')
fprogress.flush()

fout.close()
fprogress.close()
