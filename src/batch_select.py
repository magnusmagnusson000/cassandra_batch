import gc
import os.path
import sys
import json
import time
import traceback
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

"""
Date: 2018-11-12
Author: Magnus Magnusson

Description:
Python script for selecting batches of keys from a Cassandra cluster

If you need more speed, run multiple instances of this script in parallel with different input file-names
One way is to split the original input file in multiple with the split command: 
split -l <number of rows in each new file> <filename> <new filename>
Example: split -l 300 file.txt new
Results in new files starting with 'new' and containing each 300 lines and one last file with the remaining rows
Then use each new file as input to the parallel executions

Usage:

Running this script without arguments prints the required input arguments

Output:
0 - successful execution
1 - execution interrupted by user, ie ctrl-c
2 - unexpected exception caught during execution
3 - missing input arguments

"""

def logic(session, fout, fprogress, starttime, tablename, keycolumn, keybatch, progress, cntr):
   batchstr = ','.join(keybatch)
   print 'Ready to execute batch of length:', len(keybatch)
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
      print 'Execution time for last batch:', exectime, 'sec, total progress:', progress
      print 'Currentime:', starttime
   for elem in rows:
      TODO ADD YOUR OWN LOGIC HERE
   fout.flush()
   fprogress.write(str(cntr) + '\n')
   fprogress.flush()
   gc.collect()
   
   return starttime, progress

if len(sys.argv) < 7:
   print 'Usage:\narguments: <db username> <db password> <name of keyspace> <key-filename> <tablename> <primary-key-column> <batchsize>'
   sys.exit(3)

cluster = None
session = None
fout = None
fprogress = None

try:
   keyfilename = sys.argv[4]
   tablename = sys.argv[5]
   keycolumn = sys.argv[6]
   batchsize = int(sys.argv[7])
   
   auth_prov = PlainTextAuthProvider(username=sys.argv[1], password=sys.argv[2])
   cluster = Cluster(auth_provider=auth_prov)
   session = cluster.connect(sys.argv[3])
   
   # open file containing list of keys to use for batch select
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
   
   
   rawdata = []
   t1 = time.time()
   for l in f:
      if l is not None and l != '':
         rawdata.append("\'"+l.strip()+"\'")
      else:
         print 'empty row found:', l
   print 'time to read file:', keyfilename, time.time() - t1, 'sec'
   print 'Total number of rows in file:', len(rawdata)
   print 'Starting to read file from:', start_index
   
   # fout is used for writing the results to a file, its opened in append mode to allow continuing the execution after failure or normal exit
   fout = open(keyfilename+'_out.log', 'a+')
   # fprogress is used for keeping track of how many lines have been successfully executed
   fprogress = open(keyfilename+'_progress.log', 'a+')
   
   progress = start_index
   
   starttime = time.time()
   
   keybatch = []
   batchstr = ''
   cntr = start_index
   for l in rawdata[start_index:]:
      sys.stdout.flush()
      keybatch.append(l)
      cntr += 1
      if cntr % batchsize == 0:
         starttime, progress = logic(session, fout, fprogress, starttime, tablename, keycolumn, keybatch, progress, cntr)
   if len(keybatch) > 0:
      logic(session, fout, fprogress, starttime, tablename, keycolumn, keybatch, progress, cntr)
   
   fout.close()
   fprogress.close()
   
   print 'All keys from input file have been processed and system will now exit'
   sys.stdout.flush()
   session.shutdown()
   cluster.shutdown()
   sys.exit(0)
except KeyboardInterrupt:
   if fprogress is not None and fout is not None:
      fprogress.flush()
      fprogress.close()
      fout.close()
   if session is not None and cluster is not None:
      session.shutdown()
      cluster.shutdown()
   print "Shutdown requested...exiting"
   sys.exit(1)
except Exception:
   traceback.print_exc(file=sys.stdout)
   sys.exit(2)
