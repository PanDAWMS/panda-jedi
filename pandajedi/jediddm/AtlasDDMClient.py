import sys

from DDMClientBase import DDMClientBase

from dq2.clientapi.DQ2 import DQ2
from dq2.info import TiersOfATLAS

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# class to access to ATLAS DDM
class AtlasDDMClient(DDMClientBase):

    # constructor
    def __init__(self,con):
        # initialize base class
        DDMClientBase.__init__(self,con)
        # the list of fatal error
        from dq2.clientapi.DQ2 import *
        self.fatalErrors = [DQUnknownDatasetException]


    # get files in dataset
    def getFilesInDataset(self,datasetName):
        methodName = 'getFilesInDataset'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listFilesInDataset(datasetName)
            return self.SC_SUCCEEDED,tmpRet[0]
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)


    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        methodName = 'listDatasetReplicas'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listDatasetReplicas(datasetName,old=False)
            return self.SC_SUCCEEDED,tmpRet
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)


    # get site property
    def getSiteProperty(self,seName,attribute):
        methodName = 'getSiteProperty'
        try:
            return self.SC_SUCCEEDED,TiersOfATLAS.getSiteProperty(seName,attribute)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)


    # check error
    def checkError(self,errType):
        if errType in self.fatalErrors:
            # fatal error
            return self.SC_FATAL
        else:
            # temprary error
            return self.SC_FAILED
            
    
