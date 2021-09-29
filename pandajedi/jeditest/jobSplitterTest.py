# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface

from pandajedi.jediorder.JobBroker import JobBroker
from pandajedi.jediorder.JobSplitter import JobSplitter
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jedicore.ThreadUtils import ThreadPool,ListWithLock
from pandajedi.jediorder.TaskSetupper import TaskSetupper
from pandajedi.jedicore import Interaction

import sys

logger = PandaLogger().getLogger('JobGenerator')
tmpLog = MsgWrapper(logger)


tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.getSiteMapper()


ddmIF = DDMInterface()
ddmIF.setupInterface()

jediTaskID = int(sys.argv[1])
try:
    datasetID = [int(sys.argv[2])]
except Exception:
    datasetID = None

s,taskSpec = tbIF.getTaskWithID_JEDI(jediTaskID)

cloudName = taskSpec.cloud
vo = taskSpec.vo
prodSourceLabel = taskSpec.prodSourceLabel
queueID = taskSpec.workQueue_ID
gshare_name = taskSpec.gshare

workQueue = tbIF.getWorkQueueMap().getQueueWithIDGshare(queueID, gshare_name)

brokerageLockIDs = ListWithLock([])

threadPool = ThreadPool()

# get typical number of files
typicalNumFilesMap = tbIF.getTypicalNumInput_JEDI(vo,prodSourceLabel,workQueue,
                                                  useResultCache=600)

tmpListList = tbIF.getTasksToBeProcessed_JEDI(None,vo,workQueue,
                                              prodSourceLabel,
                                              cloudName,nFiles=10,simTasks=[jediTaskID],
                                              fullSimulation=True,
                                              typicalNumFilesMap=typicalNumFilesMap,
                                              simDatasets=datasetID,
                                              numNewTaskWithJumbo=5,
                                              ignore_lock=True)

taskSetupper = TaskSetupper(vo,prodSourceLabel)
taskSetupper.initializeMods(tbIF,ddmIF)

for dummyID,tmpList in tmpListList:
    task_common = {}
    for taskSpec,cloudName,inputChunk in tmpList:
        jobBroker = JobBroker(taskSpec.vo,taskSpec.prodSourceLabel)
        tmpStat = jobBroker.initializeMods(ddmIF.getInterface(vo),tbIF)
        jobBrokerCore = jobBroker.getImpl(taskSpec.vo, taskSpec.prodSourceLabel)
        jobBrokerCore.setTestMode()
        jobBrokerCore.set_task_common_dict(task_common)
        splitter = JobSplitter()
        gen = JobGeneratorThread(None,threadPool,tbIF,ddmIF,siteMapper,False,taskSetupper,None,
                                 None,'dummy',None,None,brokerageLockIDs, False)

        taskParamMap = None
        if taskSpec.useLimitedSites():
            tmpStat,taskParamMap = gen.readTaskParams(taskSpec,taskParamMap,tmpLog)
        jobBroker.setLockID(taskSpec.vo,taskSpec.prodSourceLabel,123,0)
        tmpStat,inputChunk = jobBroker.doBrokerage(taskSpec,cloudName,inputChunk,taskParamMap)
        brokerageLockID = jobBroker.getBaseLockID(taskSpec.vo,taskSpec.prodSourceLabel)
        if brokerageLockID is not None:
            brokerageLockIDs.append(brokerageLockID)
        for brokeragelockID in brokerageLockIDs:
            tbIF.unlockProcessWithPID_JEDI(taskSpec.vo,taskSpec.prodSourceLabel,workQueue.queue_id,
                                           brokeragelockID,True)
        tmpStat, subChunks, isSkipped = splitter.doSplit(taskSpec, inputChunk, siteMapper,
                                                         allow_chunk_size_limit=True)
        if tmpStat == Interaction.SC_SUCCEEDED and isSkipped:
            # run again without chunk size limit to generate jobs for skipped snippet
            tmpStat, tmpChunks, isSkipped = splitter.doSplit(taskSpec, inputChunk,
                                                             siteMapper,
                                                             allow_chunk_size_limit=False)
            if tmpStat == Interaction.SC_SUCCEEDED:
                subChunks += tmpChunks
        tmpStat,pandaJobs,datasetToRegister,oldPandaIDs,parallelOutMap,outDsMap = gen.doGenerate(taskSpec,cloudName,subChunks,inputChunk,tmpLog,True,
                                                                                                 splitter=splitter)
