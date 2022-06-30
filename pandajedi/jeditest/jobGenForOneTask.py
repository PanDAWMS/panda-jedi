# logger
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface

from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jedicore.ThreadUtils import ThreadPool, ListWithLock
from pandajedi.jediorder.TaskSetupper import TaskSetupper

import os
import sys
import socket

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.getSiteMapper()

ddmIF = DDMInterface()
ddmIF.setupInterface()

jediTaskID = int(sys.argv[1])

# get task attributes
s, taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID)
pid = '{0}-{1}_{2}-sgen'.format(socket.getfqdn().split('.')[0], os.getpid(), os.getpgrp())
vo = taskSpec.vo
prodSourceLabel = taskSpec.prodSourceLabel
workQueue = tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)

# get inputs
tmpList = tbIF.getTasksToBeProcessed_JEDI(pid, None, workQueue, None, None, nFiles=1000,
                                              target_tasks=[jediTaskID])
inputList = ListWithLock(tmpList)

# create thread
threadPool = ThreadPool()
taskSetupper = TaskSetupper(vo, prodSourceLabel)
taskSetupper.initializeMods(tbIF, ddmIF)
gen = JobGeneratorThread(inputList, threadPool, tbIF, ddmIF, siteMapper, True, taskSetupper, pid,
                         workQueue, 'sgen', None, None, None, False)
gen.start()
gen.join()
