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


class JediTaskBuffer(TaskBuffer.TaskBuffer,CommandReceiveInterface):

    # constructor
    def __init__(self,conn):
        CommandReceiveInterface.__init__(self,conn)
        TaskBuffer.TaskBuffer.__init__(self)
        TaskBuffer.TaskBuffer.init(self,jedi_config.db.dbhost,
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
                                   excludePatt,xmlConfig,noWaitParent,parent_tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDataset_JEDI(datasetSpec,fileMap,datasetState,stateUpdateTime,
                                                  nEventsPerFile,nEventsPerJob,maxAttempt,
                                                  firstEventNumber,nMaxFiles,nMaxEvents,
                                                  useScout,fileList,useFilesWithNewAttemptNr,
                                                  nFilesPerJob,nEventsPerRange,nChunksForScout,
                                                  includePatt,excludePatt,xmlConfig,
                                                  noWaitParent,parent_tid)
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
    def updateTask_JEDI(self,taskSpec,criteria,oldStatus=None,updateDEFT=False,insertUnknown=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTask_JEDI(taskSpec,criteria,oldStatus,updateDEFT,insertUnknown)
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
    def updateTaskStatusByContFeeder_JEDI(self,jediTaskID,taskSpec=None,getTaskStatus=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,getTaskStatus)
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
    def getTaskDatasetsWithID_JEDI(self,jediTaskID,pid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskDatasetsWithID_JEDI(jediTaskID,pid)
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
                                   fullSimulation=False,simDatasets=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(pid,vo,workQueue,prodSourceLabel,cloudName,nTasks,nFiles,
                                                  simTasks=simTasks,
                                                  minPriority=minPriority,
                                                  maxNumJobs=maxNumJobs,
                                                  typicalNumFilesMap=typicalNumFilesMap,
                                                  fullSimulation=fullSimulation,
                                                  simDatasets=simDatasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be processed
    def checkWaitingTaskPrio_JEDI(self,vo,workQueue,prodSourceLabel,cloudName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(None,vo,workQueue,prodSourceLabel,
                                                  cloudName,isPeeking=True)
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



    # get job statistics with work queue per cloud
    def getJobStatWithWorkQueuePerCloud_JEDI(self,vo,prodSourceLabel):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getJobStatisticsWithWorkQueue_JEDI(vo,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        if retVal[0] == False:
            return retVal
        # make per-cloud map
        retMap = {}
        for computingSite,siteMap in retVal[1].iteritems():
            for cloud,cloudMap in siteMap.iteritems():
                # add cloud
                if not retMap.has_key(cloud):
                    retMap[cloud] = {}
                for workQueue_ID,workQueueMap in cloudMap.iteritems():
                    # add work queue
                    if not retMap[cloud].has_key(workQueue_ID):
                        retMap[cloud][workQueue_ID] = {}
                    for jobStatus,nCount in workQueueMap.iteritems():
                        # add job status
                        if not retMap[cloud][workQueue_ID].has_key(jobStatus):
                            retMap[cloud][workQueue_ID][jobStatus] = 0
                        # add
                        retMap[cloud][workQueue_ID][jobStatus] += nCount
        # return
        return retVal[0],retMap



    # generate output files for task
    def getOutputFiles_JEDI(self,jediTaskID,provenanceID,simul,instantiateTmpl=False,instantiatedSite=None,
                            isUnMerging=False,isPrePro=False,xmlConfigJob=None,siteDsMap=None,middleName=''):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getOutputFiles_JEDI(jediTaskID,provenanceID,simul,instantiateTmpl,instantiatedSite,
                                           isUnMerging,isPrePro,xmlConfigJob,siteDsMap,middleName)
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
    def getHighestPrioJobStat_JEDI(self,prodSourceLabel,cloudName,workQueueID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getHighestPrioJobStat_JEDI(prodSourceLabel,cloudName,workQueueID)
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



    # prepare tasks to be finished
    def prepareTasksToBeFinished_JEDI(self,vo,prodSourceLabel,nTasks=50,simTasks=None,pid='lock'):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.prepareTasksToBeFinished_JEDI(vo,prodSourceLabel,nTasks,simTasks,pid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be assigned
    def getTasksToAssign_JEDI(self,vo,prodSourceLabel,workQueue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToAssign_JEDI(vo,prodSourceLabel,workQueue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to check task assignment
    def getTasksToCheckAssignment_JEDI(self,vo,prodSourceLabel,workQueue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToCheckAssignment_JEDI(vo,prodSourceLabel,workQueue)
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
    def reactivatePendingTasks_JEDI(self,vo,prodSourceLabel,timeLimit,timeoutLimit=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.reactivatePendingTasks_JEDI(vo,prodSourceLabel,timeLimit,timeoutLimit)
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
