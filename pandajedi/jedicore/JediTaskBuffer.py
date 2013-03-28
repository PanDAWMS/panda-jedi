# DB API for JEDI

from pandajedi.jediconfig import jedi_config

from pandaserver import taskbuffer
import taskbuffer.TaskBuffer
import JediDBProxyPool
from Interaction import CommandReceiveInterface

# use customized proxy pool
taskbuffer.TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool

class JediTaskBuffer(taskbuffer.TaskBuffer.TaskBuffer,CommandReceiveInterface):
    # constructor
    def __init__(self,conn):
        CommandReceiveInterface.__init__(self,conn)
        taskbuffer.TaskBuffer.TaskBuffer.__init__(self)
        taskbuffer.TaskBuffer.TaskBuffer.init(self,jedi_config.dbhost,
                                              jedi_config.dbpasswd,
                                              nDBConnection=1)


    # get the list of datasets to feed contents to DB
    def getDatasetsToFeedContentsJEDI(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        retVal = proxy.getDatasetsToFeedContentsJEDI()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # feed files to the JEDI contents table
    def insertFilesForDatasetJEDI(self,taskID,datasetID,fileMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertFilesForDatasetJEDI(taskID,datasetID,fileMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # insert dataset to the JEDI datasets table
    def insertDatasetJEDI(self,datasetSpec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertDatasetJEDI(datasetSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # update JEDI dataset
    def updateDatasetJEDI(self,datasetSpec,criteria):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateDatasetJEDI(datasetSpec,criteria)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # insert task to the JEDI tasks table
    def insertTaskJEDI(self,taskSpec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.insertTaskJEDI(taskSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # update JEDI task
    def updateTaskJEDI(self,taskSpec,criteria):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retVal = proxy.updateTaskJEDI(taskSpec,criteria)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal
