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
from pandajedi.jedicore.ThreadUtils import ThreadPool,ListWithLock
from pandajedi.jediorder.TaskSetupper import TaskSetupper

import sys
jediTaskID = int(sys.argv[1])
try:
    datasetID = [int(sys.argv[2])]
except:
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
                                              numNewTaskWithJumbo=5)

taskSetupper = TaskSetupper(vo,prodSourceLabel)
taskSetupper.initializeMods(tbIF,ddmIF)

for dummyID,tmpList in tmpListList:
    for taskSpec,cloudName,inputChunk in tmpList:
        jobBroker = JobBroker(taskSpec.vo,taskSpec.prodSourceLabel)
        tmpStat = jobBroker.initializeMods(ddmIF.getInterface(vo),tbIF)
        jobBroker.setTestMode(taskSpec.vo,taskSpec.prodSourceLabel)
        splitter = JobSplitter()
        gen = JobGeneratorThread(None,threadPool,tbIF,ddmIF,siteMapper,False,taskSetupper,None,
                                 None,'dummy',None,None,brokerageLockIDs, False)

        taskParamMap = None
        if taskSpec.useLimitedSites():
            tmpStat,taskParamMap = gen.readTaskParams(taskSpec,taskParamMap,tmpLog)
        jobBroker.setLockID(taskSpec.vo,taskSpec.prodSourceLabel,123,0)
        tmpStat,inputChunk = jobBroker.doBrokerage(taskSpec,cloudName,inputChunk,taskParamMap)
        brokerageLockID = jobBroker.getBaseLockID(taskSpec.vo,taskSpec.prodSourceLabel)
        if brokerageLockID != None:
            brokerageLockIDs.append(brokerageLockID)
        for brokeragelockID in brokerageLockIDs:
            tbIF.unlockProcessWithPID_JEDI(taskSpec.vo,taskSpec.prodSourceLabel,workQueue.queue_id,
                                           brokeragelockID,True)
        tmpStat,subChunks = splitter.doSplit(taskSpec,inputChunk,siteMapper)
        tmpStat,pandaJobs,datasetToRegister,oldPandaIDs,parallelOutMap,outDsMap = gen.doGenerate(taskSpec,cloudName,subChunks,inputChunk,tmpLog,True,
                                                                                                 splitter=splitter)

