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
    def insertFilesForDataset_JEDI(self,datasetSpec,fileMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDataset_JEDI(datasetSpec,fileMap)
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
    def updateDataset_JEDI(self,datasetSpec,criteria):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateDataset_JEDI(datasetSpec,criteria)
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
    def getTasksToBeProcessed_JEDI(self,pid,vo,workQueue,prodSourceLabel,nTasks=50,nFiles=100):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTasksToBeProcessed_JEDI(pid,vo,workQueue,prodSourceLabel,nTasks,nFiles)
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
