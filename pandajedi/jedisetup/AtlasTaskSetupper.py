import re
import sys
import uuid
import json

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from TaskSetupperBase import TaskSetupperBase


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task setup for ATLAS
class AtlasTaskSetupper (TaskSetupperBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskSetupperBase.__init__(self,taskBufferIF,ddmIF)


    # main to setup task
    def doSetup(self,taskSpec):
        # make logger
        tmpLog = MsgWrapper(logger,"<jediTaskID={0}>".format(taskSpec.jediTaskID))
        tmpLog.info('start label={0} taskType={1}'.format(taskSpec.prodSourceLabel,taskSpec.taskType))
        # returns
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        retOK       = self.SC_SUCCEEDED
        try:
            # check prodSourceLabel
            if taskSpec.prodSourceLabel in ['managed','test']:
                # get output and log datasets
                tmpStat,datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,
                                                                                           ['output','log'])
                if not tmpStat:
                    tmpLog.error('failed to get output and log datasets')
                    return retFatal
                # get DDM I/F
                ddmIF = self.ddmIF.getInterface(taskSpec.vo)
                # loop over datasets
                avDatasetList = []
                cnDatasetMap  = []
                for datasetSpec in datasetSpecList:
                    tmpLog.info('checking datasetID={0}:Name={1}'.format(datasetSpec.datasetID,datasetSpec.datasetName)) 
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName,datasetSpec.containerName]:
                        if targetName == None:
                            continue
                        if not targetName in avDatasetList:
                            # check dataset/container in DDM
                            tmpList = ddmIF.listDatasets(targetName)
                            if tmpList == []:
                                # register dataset/container
                                tmpLog.info('registering {0}'.format(targetName))
                                tmpStat = ddmIF.registerNewDataset(targetName)
                                if not tmpStat:
                                    tmpLog.error('failed to register {0}'.format(targetName))
                                    return retFatal
                                avDatasetList.append(targetName)
                            else:
                                tmpLog.info('{0} already registered'.format(targetName))
                    # check if dataset is in the container
                    if datasetSpec.containerName != None and datasetSpec.containerName != datasetSpec.datasetName:
                        # get list of constituent datasets in the container
                        if not cnDatasetMap.has_key(datasetSpec.containerName):
                            cnDatasetMap[datasetSpec.containerName] = ddmIF.listDatasetsInContainer(datasetSpec.containerName)
                        # add dataset
                        if not cnDatasetMap[datasetSpec.containerName].has_key(datasetSpec.datasetName):
                            tmpLog.info('adding {0} to {1}'.format(datasetSpec.datasetName,datasetSpec.containerName)) 
                            tmpStat = ddmIF.addDatasetsToContainer(datasetSpec.containerName,datasetSpec.datasetName)
                            if not tmpStat:
                                tmpLog.error('failed to add {0} to {1}'.format(datasetSpec.datasetName,
                                                                               datasetSpec.containerName))
                                return retFatal
                            cnDatasetMap[datasetSpec.containerName].append(datasetSpec.datasetName)
                        else:
                            tmpLog.info('{0} already in {1}'.format(datasetSpec.datasetName,datasetSpec.containerName)) 
            # return
            tmpLog.info('done')        
            return retOK
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doSetup failed with {0}:{1}'.format(errtype.__name__,errvalue))
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retFatal
            
