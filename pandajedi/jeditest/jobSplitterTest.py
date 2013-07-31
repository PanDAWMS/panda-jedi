# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger('JobGenerator')
from pandajedi.jedicore.MsgWrapper import MsgWrapper
tmpLog = MsgWrapper(logger)

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.getSiteMapper()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

from pandajedi.jediorder.JobBroker import JobBroker
from pandajedi.jediorder.JobSplitter import JobSplitter
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jedicore.ThreadUtils import ThreadPool
from pandajedi.jediorder.TaskSetupper import TaskSetupper

import sys
jediTaskID = int(sys.argv[1])

s,taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID)

cloudName = taskSpec.cloud
vo = taskSpec.vo
prodSourceLabel = taskSpec.prodSourceLabel 
queueID = taskSpec.workQueue_ID

workQueue = tbIF.getWorkQueueMap().getQueueWithID(queueID)

threadPool = ThreadPool()

tmpList = tbIF.getTasksToBeProcessed_JEDI(None,vo,workQueue,
                                          prodSourceLabel,
                                          cloudName,nFiles=10,simTasks=[jediTaskID],
                                          fullSimulation=True)

taskSetupper = TaskSetupper(vo,prodSourceLabel)
taskSetupper.initializeMods(tbIF,ddmIF)

for taskSpec,cloudName,inputChunk in tmpList:
    jobBroker = JobBroker(taskSpec.vo,taskSpec.prodSourceLabel)
    tmpStat = jobBroker.initializeMods(ddmIF.getInterface(vo),tbIF)
    tmpStat,inputChunk = jobBroker.doBrokerage(taskSpec,cloudName,inputChunk)

    splitter = JobSplitter()
    tmpStat,subChunks = splitter.doSplit(taskSpec,inputChunk,siteMapper)

    gen = JobGeneratorThread(None,threadPool,tbIF,ddmIF,siteMapper,False,taskSetupper)
    newJobs = gen.doGenerate(taskSpec,cloudName,subChunks,inputChunk,tmpLog,True)
