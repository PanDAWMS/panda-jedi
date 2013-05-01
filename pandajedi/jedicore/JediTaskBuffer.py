# DB API for JEDI

from pandajedi.jediconfig import jedi_config

from pandaserver.taskbuffer import TaskBuffer
from pandaserver.brokerage.SiteMapper import SiteMapper
import JediDBProxyPool
from Interaction import CommandReceiveInterface

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
        self.siteMapper = SiteMapper(self)


    # get SiteMapper
    def getSiteMapper(self):
        return self.siteMapper
    

    # get work queue map
    def getWrokQueueMap(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        retVal = proxy.getWrokQueueMap()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContents_JEDI(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        retVal = proxy.getDatasetsToFeedContents_JEDI()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # feed files to the JEDI contents table
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap,datasetState,stateUpdateTime):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDataset_JEDI(datasetSpec,fileMap,datasetState,stateUpdateTime)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get files from the JEDI contents table with taskID and/or datasetID
    def getFilesInDatasetWithID_JEDI(self,taskID=None,datasetID=None,nFiles=None,status=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getFilesInDatasetWithID_JEDI(taskID,datasetID,nFiles,status)
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


    # get JEDI dataset with datasetID
    def getDatasetWithID_JEDI(self,datasetID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getDatasetWithID_JEDI(datasetID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


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
    def updateTask_JEDI(self,taskSpec,criteria):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTask_JEDI(taskSpec,criteria)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # update JEDI task status by ContentsFeeder
    def updateTaskStatusByContFeeder_JEDI(self,taskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskStatusByContFeeder_JEDI(taskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get JEDI task with taskID
    def getTaskWithID_JEDI(self,taskID,fullFlag=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskWithID_JEDI(taskID,fullFlag)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # get tasks to be processed
    def getTasksToBeProcessed_JEDI(self,pid,vo,workQueue,prodSourceLabel,cloudName,nTasks=50,nFiles=100):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(pid,vo,workQueue,prodSourceLabel,cloudName,nTasks,nFiles)
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
    def getOutputFiles_JEDI(self,taskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getOutputFiles_JEDI(taskID)
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
    def insertJobParamsTemplate_JEDI(self,taskID,templ):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertJobParamsTemplate_JEDI(taskID,templ)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # insert TaskParams
    def insertTaskParams_JEDI(self,taskID,taskParams):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertTaskParams_JEDI(taskID,taskParams)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # rollback files
    def rollbackFiles_JEDI(self,taskID,inputChunk):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.rollbackFiles_JEDI(taskID,inputChunk)
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



    # get task parameters with taskID
    def getTaskParamsWithID_JEDI(self,taskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskParamsWithID_JEDI(taskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal



    # register task/dataset/templ/param in a single transaction
    def registerTaskInOneShot_JEDI(self,taskID,taskSpec,inMasterDatasetSpec,
                                   inSecDatasetSpecList,outDatasetSpecList,
                                   outputTemplateMap,jobParamsTemplate):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.registerTaskInOneShot_JEDI(taskID,taskSpec,inMasterDatasetSpec,
                                                  inSecDatasetSpecList,outDatasetSpecList,
                                                  outputTemplateMap,jobParamsTemplate)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal
