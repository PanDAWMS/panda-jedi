# DB API for JEDI

import datetime

from pandajedi.jediconfig import jedi_config

from pandaserver.taskbuffer import TaskBuffer
from pandaserver.brokerage.SiteMapper import SiteMapper
import JediDBProxyPool
from Interaction import CommandReceiveInterface

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# use customized proxy pool
TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool


class JediTaskBuffer(TaskBuffer.TaskBuffer, CommandReceiveInterface):

    # constructor
    def __init__(self,conn):
        CommandReceiveInterface.__init__(self,conn)
        TaskBuffer.TaskBuffer.__init__(self)
        TaskBuffer.TaskBuffer.init(self, jedi_config.db.dbhost,
                                   jedi_config.db.dbpasswd,
                                   nDBConnection=1)
        # site mapper
        self.siteMapper = SiteMapper(self)
        # update time for site mapper
        self.dateTimeForSM = datetime.datetime.utcnow()
        logger.debug('__init__')



    # get SiteMapper
    def getSiteMapper(self):
        timeNow = datetime.datetime.utcnow()
        if datetime.datetime.utcnow()-self.dateTimeForSM > datetime.timedelta(minutes=10):
            self.siteMapper = SiteMapper(self)
            self.dateTimeForSM = timeNow
        return self.siteMapper

    

    # get work queue map
    def getWorkQueueMap(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        retVal = proxy.getWorkQueueMap()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self,vo=None,prodSourceLabel=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        retVal = proxy.getDatasetsToFeedContents_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap,datasetState,stateUpdateTime,
                                   nEventsPerFile,nEventsPerJob,maxAttempt,firstEventNumber,
                                   nMaxFiles,nMaxEvents,useScout,fileList,useFilesWithNewAttemptNr,
                                   nFilesPerJob,nEventsPerRange,nChunksForScout,includePatt,
                                   excludePatt,xmlConfig,noWaitParent,parent_tid,pid,maxFailure,
                                   useRealNumEvents,respectLB,tgtNumEventsPerJob,skipFilesUsedBy,
                                   ramCount,taskSpec,skipShortInput):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDataset_JEDI(datasetSpec,fileMap,datasetState,stateUpdateTime,
                                                  nEventsPerFile,nEventsPerJob,maxAttempt,
                                                  firstEventNumber,nMaxFiles,nMaxEvents,
                                                  useScout,fileList,useFilesWithNewAttemptNr,
                                                  nFilesPerJob,nEventsPerRange,nChunksForScout,
                                                  includePatt,excludePatt,xmlConfig,
                                                  noWaitParent,parent_tid,pid,maxFailure,
                                                  useRealNumEvents,respectLB,
                                                  tgtNumEventsPerJob,skipFilesUsedBy,
                                                  ramCount,taskSpec,skipShortInput)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get files from the JEDI contents table with jediTaskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self,jediTaskID=None,datasetID=None,nFiles=None,status=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getFilesInDatasetWithID_JEDI(jediTaskID,datasetID,nFiles,status)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self,datasetSpec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertDataset_JEDI(datasetSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update JEDI dataset
    def updateDataset_JEDI(self,datasetSpec,criteria,lockTask=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateDataset_JEDI(datasetSpec,criteria,lockTask)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self,jediTaskID,datasetID,attributes):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateDatasetAttributes_JEDI(jediTaskID,datasetID,attributes)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self,jediTaskID,datasetID,attributes):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getDatasetAttributes_JEDI(jediTaskID,datasetID,attributes)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self,jediTaskID,criteria,attributes):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getDatasetAttributesWithMap_JEDI(jediTaskID,criteria,attributes)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI dataset with jediTaskID and datasetID
    def getDatasetWithID_JEDI(self,jediTaskID,datasetID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getDatasetWithID_JEDI(jediTaskID,datasetID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(self,jediTaskID,datasetTypes=None,getFiles=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retStat,datasetSpecList = proxy.getDatasetsWithJediTaskID_JEDI(jediTaskID,datasetTypes=datasetTypes)
        if retStat == True and getFiles == True:
            for datasetSpec in datasetSpecList:
                # read files
                retStat,fileSpecList = proxy.getFilesInDatasetWithID_JEDI(jediTaskID,datasetSpec.datasetID,None,None)
                if retStat == False:
                    break
                for fileSpec in fileSpecList:
                    datasetSpec.addFile(fileSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retStat,datasetSpecList



    # insert task to the JEDI tasks table
    def insertTask_JEDI(self,taskSpec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertTask_JEDI(taskSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update JEDI task
    def updateTask_JEDI(self,taskSpec,criteria,oldStatus=None,updateDEFT=False,insertUnknown=None,
                        setFrozenTime=True,setOldModTime=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTask_JEDI(taskSpec,criteria,oldStatus,updateDEFT,insertUnknown,
                                       setFrozenTime,setOldModTime)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update JEDI task lock
    def updateTaskLock_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskLock_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self,jediTaskID,taskSpec=None,getTaskStatus=False,pid=None,
                                          setFrozenTime=True,useWorldCloud=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,getTaskStatus,
                                                         pid,setFrozenTime,useWorldCloud)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI task with jediTaskID
    def getTaskWithID_JEDI(self,jediTaskID,fullFlag=False,lockTask=False,pid=None,lockInterval=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskWithID_JEDI(jediTaskID,fullFlag,lockTask,pid,lockInterval)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI task and tasks with ID and lock it
    def getTaskDatasetsWithID_JEDI(self,jediTaskID,pid,lockTask=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskDatasetsWithID_JEDI(jediTaskID,pid,lockTask)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI tasks with selection criteria
    def getTaskIDsWithCriteria_JEDI(self,criteria,nTasks=50):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskIDsWithCriteria_JEDI(criteria,nTasks)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI tasks to be finished
    def getTasksToBeFinished_JEDI(self,vo,prodSourceLabel,pid,nTasks=50):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeFinished_JEDI(vo,prodSourceLabel,pid,nTasks)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(self,pid,vo,workQueue,prodSourceLabel,cloudName,
                                   nTasks=50,nFiles=100,simTasks=None,minPriority=None,
                                   maxNumJobs=None,typicalNumFilesMap=None,
                                   fullSimulation=False,simDatasets=None,
                                   mergeUnThrottled=None,readMinFiles=False,
                                   numNewTaskWithJumbo=0, resource_name=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(pid,vo,workQueue,prodSourceLabel,cloudName,nTasks,nFiles,
                                                  simTasks=simTasks,
                                                  minPriority=minPriority,
                                                  maxNumJobs=maxNumJobs,
                                                  typicalNumFilesMap=typicalNumFilesMap,
                                                  fullSimulation=fullSimulation,
                                                  simDatasets=simDatasets,
                                                  mergeUnThrottled=mergeUnThrottled,
                                                  readMinFiles=readMinFiles,
                                                  numNewTaskWithJumbo=numNewTaskWithJumbo,
                                                  resource_name=resource_name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be processed
    def checkWaitingTaskPrio_JEDI(self,vo,workQueue,prodSourceLabel,cloudName,resource_name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(None,vo,workQueue,prodSourceLabel,
                                                  cloudName,isPeeking=True,resource_name=resource_name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


        
    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self,vo,prodSourceLabel,minPriority=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getJobStatisticsWithWorkQueue_JEDI(vo,prodSourceLabel,minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get job statistics by global share
    def getJobStatisticsByGlobalShare(self, vo, exclude_rwq=False):
        proxy = self.proxyPool.getProxy()
        retVal = proxy.getJobStatisticsByGlobalShare(vo, exclude_rwq)
        self.proxyPool.putProxy(proxy)
        return retVal


    # get job statistics by global share
    def getJobStatisticsByResourceType(self, workqueue):
        proxy = self.proxyPool.getProxy()
        retVal = proxy.getJobStatisticsByResourceType(workqueue)
        self.proxyPool.putProxy(proxy)
        return retVal


    # generate output files for task
    def getOutputFiles_JEDI(self,jediTaskID,provenanceID,simul,instantiateTmpl=False,instantiatedSite=None,
                            isUnMerging=False,isPrePro=False,xmlConfigJob=None,siteDsMap=None,middleName='',
                            registerDatasets=False,parallelOutMap=None,fileIDPool=[]):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getOutputFiles_JEDI(jediTaskID,provenanceID,simul,instantiateTmpl,instantiatedSite,
                                           isUnMerging,isPrePro,xmlConfigJob,siteDsMap,middleName,
                                           registerDatasets,parallelOutMap,fileIDPool)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert output file templates
    def insertOutputTemplate_JEDI(self,templates):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertOutputTemplate_JEDI(templates)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert JobParamsTemplate
    def insertJobParamsTemplate_JEDI(self,jediTaskID,templ):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertJobParamsTemplate_JEDI(jediTaskID,templ)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert TaskParams
    def insertTaskParams_JEDI(self,vo,prodSourceLabel,userName,taskName,taskParams,parent_tid=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertTaskParams_JEDI(vo,prodSourceLabel,userName,taskName,taskParams,parent_tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # reset unused files
    def resetUnusedFiles_JEDI(self,jediTaskID,inputChunk):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.resetUnusedFiles_JEDI(jediTaskID,inputChunk)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert TaskParams
    def insertUpdateTaskParams_JEDI(self,jediTaskID,vo,prodSourceLabel,updateTaskParams,insertTaskParamsList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertUpdateTaskParams_JEDI(jediTaskID,vo,prodSourceLabel,updateTaskParams,insertTaskParamsList)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set missing files
    def setMissingFiles_JEDI(self,jediTaskID,datasetID,fileIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.setMissingFiles_JEDI(jediTaskID,datasetID,fileIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # rescue picked files
    def rescuePickedFiles_JEDI(self,vo,prodSourceLabel,waitTime):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.rescuePickedFiles_JEDI(vo,prodSourceLabel,waitTime)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # rescue unlocked tasks with picked files
    def rescueUnLockedTasksWithPicked_JEDI(self,vo,prodSourceLabel,waitTime,pid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.rescueUnLockedTasksWithPicked_JEDI(vo,prodSourceLabel,waitTime,pid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # unlock tasks
    def unlockTasks_JEDI(self,vo,prodSourceLabel,waitTime,hostName=None,pgid=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.unlockTasks_JEDI(vo,prodSourceLabel,waitTime,hostName,pgid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the size of input files which will be copied to the site
    def getMovingInputSize_JEDI(self,siteName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getMovingInputSize_JEDI(siteName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get typical number of input files for each workQueue+processingType
    def getTypicalNumInput_JEDI(self,vo,prodSourceLabel,workQueue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTypicalNumInput_JEDI(vo,prodSourceLabel,workQueue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get highest prio jobs with workQueueID
    def getHighestPrioJobStat_JEDI(self, prodSourceLabel, cloudName, workQueue, resource_name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getHighestPrioJobStat_JEDI(prodSourceLabel, cloudName, workQueue, resource_name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the list of tasks to refine
    def getTasksToRefine_JEDI(self,vo=None,prodSourceLabel=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToRefine_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get task parameters with jediTaskID
    def getTaskParamsWithID_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskParamsWithID_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(self,jediTaskID,taskSpec,inMasterDatasetSpec,
                                   inSecDatasetSpecList,outDatasetSpecList,
                                   outputTemplateMap,jobParamsTemplate,taskParams,
                                   unmergeMasterDatasetSpec,unmergeDatasetSpecMap,
                                   uniqueTaskName,oldTaskStatus):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.registerTaskInOneShot_JEDI(jediTaskID,taskSpec,inMasterDatasetSpec,
                                                  inSecDatasetSpecList,outDatasetSpecList,
                                                  outputTemplateMap,jobParamsTemplate,taskParams,
                                                  unmergeMasterDatasetSpec,unmergeDatasetSpecMap,
                                                  uniqueTaskName,oldTaskStatus)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set tasks to be assigned
    def setScoutJobDataToTasks_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.setScoutJobDataToTasks_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self,vo,prodSourceLabel,nTasks=50,simTasks=None,pid='lock',noBroken=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.prepareTasksToBeFinished_JEDI(vo,prodSourceLabel,nTasks,simTasks,pid,noBroken)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be assigned
    def getTasksToAssign_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToAssign_JEDI(vo, prodSourceLabel, workQueue, resource_name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self, vo, prodSourceLabel, workQueue, resource_name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToCheckAssignment_JEDI(vo, prodSourceLabel, workQueue, resource_name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # calculate RW with a priority
    def calculateRWwithPrio_JEDI(self,vo,prodSourceLabel,workQueue,priority):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.calculateRWwithPrio_JEDI(vo,prodSourceLabel,workQueue,priority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # calculate RW for tasks
    def calculateTaskRW_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.calculateTaskRW_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # calculate WORLD RW with a priority
    def calculateWorldRWwithPrio_JEDI(self,vo,prodSourceLabel,workQueue,priority):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.calculateWorldRWwithPrio_JEDI(vo,prodSourceLabel,workQueue,priority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # calculate WORLD RW for tasks
    def calculateTaskWorldRW_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.calculateTaskWorldRW_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set cloud to tasks
    def setCloudToTasks_JEDI(self,taskCloudMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.setCloudToTasks_JEDI(taskCloudMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the list of tasks to exec command
    def getTasksToExecCommand_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToExecCommand_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the list of PandaIDs for a task
    def getPandaIDsWithTask_JEDI(self,jediTaskID,onlyActive):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getPandaIDsWithTask_JEDI(jediTaskID,onlyActive)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get the list of queued PandaIDs for a task
    def getQueuedPandaIDsWithTask_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getQueuedPandaIDsWithTask_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get jediTaskID/datasetID/FileID with dataset and file names
    def getIDsWithFileDataset_JEDI(self,datasetName,fileName,fileType):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getIDsWithFileDataset_JEDI(datasetName,fileName,fileType)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get PandaID for a file
    def getPandaIDWithFileID_JEDI(self,jediTaskID,datasetID,fileID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getPandaIDWithFileID_JEDI(jediTaskID,datasetID,fileID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI files for a job
    def getFilesWithPandaID_JEDI(self,pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getFilesWithPandaID_JEDI(pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # update task parameters
    def updateTaskParams_JEDI(self,jediTaskID,taskParams):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskParams_JEDI(jediTaskID,taskParams)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # reactivate pending tasks
    def reactivatePendingTasks_JEDI(self,vo,prodSourceLabel,timeLimit,timeoutLimit=None,minPriority=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.reactivatePendingTasks_JEDI(vo,prodSourceLabel,timeLimit,timeoutLimit,minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # restart contents update
    def restartTasksForContentsUpdate_JEDI(self,vo,prodSourceLabel,timeLimit=30):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.restartTasksForContentsUpdate_JEDI(vo,prodSourceLabel,timeLimit=timeLimit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # kick exhausted tasks
    def kickExhaustedTasks_JEDI(self,vo,prodSourceLabel,timeLimit):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.kickExhaustedTasks_JEDI(vo,prodSourceLabel,timeLimit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get file spec of lib.tgz
    def getBuildFileSpec_JEDI(self,jediTaskID,siteName,associatedSites):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getBuildFileSpec_JEDI(jediTaskID,siteName,associatedSites)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get file spec of old lib.tgz
    def getOldBuildFileSpec_JEDI(self,jediTaskID,datasetID,fileID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getOldBuildFileSpec_JEDI(jediTaskID,datasetID,fileID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert lib dataset and files
    def insertBuildFileSpec_JEDI(self,jobSpec,reusedDatasetID,simul):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertBuildFileSpec_JEDI(jobSpec,reusedDatasetID,simul)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get sites used by a task
    def getSitesUsedByTask_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getSitesUsedByTask_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get random seed
    def getRandomSeed_JEDI(self,jediTaskID,simul):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getRandomSeed_JEDI(jediTaskID,simul)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get preprocess metadata
    def getPreprocessMetadata_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getPreprocessMetadata_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get log dataset for preprocessing
    def getPreproLog_JEDI(self,jediTaskID,simul):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getPreproLog_JEDI(jediTaskID,simul)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get sites with best connections to source
    def getBestNNetworkSites_JEDI(self,source,protocol,nSites,threshold,cutoff,maxWeight):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getBestNNetworkSites_JEDI(source,protocol,nSites,threshold,
                                                 cutoff,maxWeight)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal




    # get jobsetID
    def getUserJobsetID_JEDI(self,userName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        tmpJobID,tmpDummy,tmpStat = proxy.getUserParameter(userName,1,None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return tmpStat,tmpJobID



    # retry or incrementally execute a task
    def retryTask_JEDI(self,jediTaskID,commStr,maxAttempt=5):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.retryTask_JEDI(jediTaskID,commStr,maxAttempt)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # append input datasets for incremental execution
    def appendDatasets_JEDI(self,jediTaskID,inMasterDatasetSpecList,inSecDatasetSpecList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.appendDatasets_JEDI(jediTaskID,inMasterDatasetSpecList,inSecDatasetSpecList)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # record retry history
    def recordRetryHistory_JEDI(self,jediTaskID,oldNewPandaIDs,relationType):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.recordRetryHistory_JEDI(jediTaskID,oldNewPandaIDs,relationType)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI tasks with a selection criteria
    def getTasksWithCriteria_JEDI(self,vo,prodSourceLabel,taskStatusList,taskCriteria={},datasetCriteria={},
                                  taskParamList=[],datasetParamList=[]):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksWithCriteria_JEDI(vo,prodSourceLabel,taskStatusList,taskCriteria,datasetCriteria,
                                                 taskParamList,datasetParamList)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # check parent task status 
    def checkParentTask_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.checkParentTask_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get task status 
    def getTaskStatus_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskStatus_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get lib.tgz for waiting jobs
    def getLibForWaitingRunJob_JEDI(self,vo,prodSourceLabel,checkInterval):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getLibForWaitingRunJob_JEDI(vo,prodSourceLabel,checkInterval)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to get reassigned
    def getTasksToReassign_JEDI(self,vo=None,prodSourceLabel=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToReassign_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # kill child tasks
    def killChildTasks_JEDI(self,jediTaskID,taskStatus):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.killChildTasks_JEDI(jediTaskID,taskStatus)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # kick child tasks
    def kickChildTasks_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.kickChildTasks_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal




    # lock task
    def lockTask_JEDI(self,jediTaskID,pid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.lockTask_JEDI(jediTaskID,pid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get successful files
    def getSuccessfulFiles_JEDI(self,jediTaskID,datasetID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getSuccessfulFiles_JEDI(jediTaskID,datasetID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # unlock a single task
    def unlockSingleTask_JEDI(self,jediTaskID,pid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.unlockSingleTask_JEDI(jediTaskID,pid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # throttle JEDI tasks
    def throttleTasks_JEDI(self,vo,prodSourceLabel,waitTime):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.throttleTasks_JEDI(vo,prodSourceLabel,waitTime)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # throttle a JEDI task
    def throttleTask_JEDI(self,jediTaskID,waitTime,errorDialog):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.throttleTask_JEDI(jediTaskID,waitTime,errorDialog)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # release throttled tasks
    def releaseThrottledTasks_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.releaseThrottledTasks_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # release throttled task
    def releaseThrottledTask_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.releaseThrottledTask_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get throttled users
    def getThrottledUsersTasks_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getThrottledUsersTasks_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # lock process
    def lockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid, forceOption=False, timeLimit=5):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.lockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid, forceOption, timeLimit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # unlock process
    def unlockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.unlockProcess_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # unlock process with PID
    def unlockProcessWithPID_JEDI(self, vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.unlockProcessWithPID_JEDI(vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # check process lock
    def checkProcessLock_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid, checkBase):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.checkProcessLock_JEDI(vo, prodSourceLabel, cloud, workqueue_id, resource_name, pid, checkBase)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get JEDI tasks to be assessed
    def getAchievedTasks_JEDI(self,vo,prodSourceLabel,timeLimit=60,nTasks=50):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getAchievedTasks_JEDI(vo,prodSourceLabel,timeLimit,nTasks)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get inactive sites
    def getInactiveSites_JEDI(self,flag,timeLimit):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getInactiveSites_JEDI(flag,timeLimit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get total walltime
    def getTotalWallTime_JEDI(self, vo, prodSourceLabel, workQueue, resource_name, cloud=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTotalWallTime_JEDI(vo, prodSourceLabel, workQueue, resource_name, cloud)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal




    # check duplication with internal merge
    def checkDuplication_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.checkDuplication_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get network metrics for brokerage
    def getNetworkMetrics(self, dst, keyList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getNetworkMetrics(dst, keyList)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal

    # get nuclei that have built up a long backlog
    def getBackloggedNuclei(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getBackloggedNuclei()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal

    # get network metrics for brokerage
    def getPandaSiteToOutputStorageSiteMapping(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getPandaSiteToOutputStorageSiteMapping()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal

    # get failure counts for a task
    def getFailureCountsForTask_JEDI(self,jediTaskID,timeWindow):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getFailureCountsForTask_JEDI(jediTaskID,timeWindow)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get old merge job PandaIDs
    def getOldMergeJobPandaIDs_JEDI(self,jediTaskID,pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getOldMergeJobPandaIDs_JEDI(jediTaskID,pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get active jumbo jobs for a task
    def getActiveJumboJobs_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getActiveJumboJobs_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get jobParms of the first job
    def getJobParamsOfFirstJob_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getJobParamsOfFirstJob_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # bulk fetch fileIDs
    def bulkFetchFileIDs_JEDI(self,jediTaskID,nIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.bulkFetchFileIDs_JEDI(jediTaskID,nIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set del flag to events
    def setDelFlagToEvents_JEDI(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.setDelFlagToEvents_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set del flag to events
    def removeFilesIndexInconsistent_JEDI(self,jediTaskID,datasetIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.removeFilesIndexInconsistent_JEDI(jediTaskID,datasetIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # throttle jobs in pauses tasks
    def throttleJobsInPausedTasks_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.throttleJobsInPausedTasks_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # set useJumbo flag
    def setUseJumboFlag_JEDI(self,jediTaskID,statusStr):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.setUseJumboFlag_JEDI(jediTaskID,statusStr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get number of tasks with running jumbo jobs
    def getNumTasksWithRunningJumbo_JEDI(self,vo,prodSourceLabel,cloudName,workqueue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getNumTasksWithRunningJumbo_JEDI(vo,prodSourceLabel,cloudName,workqueue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get number of unprocessed events
    def getNumUnprocessedEvents_JEDI(self, vo, prodSourceLabel, criteria):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getNumUnprocessedEvents_JEDI(vo, prodSourceLabel, criteria)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get number of jobs for a task
    def getNumJobsForTask_JEDI(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getNumJobsForTask_JEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get number map for standby jobs
    def getNumMapForStandbyJobs_JEDI(self, workqueue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getNumMapForStandbyJobs_JEDI(workqueue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal
