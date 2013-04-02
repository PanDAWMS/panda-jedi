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
        TaskBuffer.TaskBuffer.init(self,jedi_config.dbhost,
                                   jedi_config.dbpasswd,
                                   nDBConnection=1)
        self.siteMapper = SiteMapper(self)


    # get SiteMapper
    def getSiteMapper(self):
        return self.siteMapper
    

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
    def insertFilesForDataset_JEDI(self,taskID,datasetID,fileMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDataset_JEDI(taskID,datasetID,fileMap)
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


    # get JEDI task with ID
    def getTaskWithID_JEDI(self,taskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getTaskWithID_JEDI(taskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self,prodSourceLabel,minPriority=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.getJobStatisticsWithWorkQueue_JEDI(prodSourceLabel,minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal
