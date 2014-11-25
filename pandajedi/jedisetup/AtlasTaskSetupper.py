import re
import sys
import uuid
import json

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from TaskSetupperBase import TaskSetupperBase

from pandaserver.dataservice import DataServiceUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task setup for ATLAS
class AtlasTaskSetupper (TaskSetupperBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskSetupperBase.__init__(self,taskBufferIF,ddmIF)


    # main to setup task
    def doSetup(self,taskSpec,datasetToRegister):
        # make logger
        tmpLog = MsgWrapper(logger,"<jediTaskID={0}>".format(taskSpec.jediTaskID))
        tmpLog.info('start label={0} taskType={1}'.format(taskSpec.prodSourceLabel,taskSpec.taskType))
        tmpLog.info('datasetToRegister={0}'.format(str(datasetToRegister)))
        # returns
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        retOK       = self.SC_SUCCEEDED
        try:
            if datasetToRegister != []:
                # prod vs anal
                userSetup = False
                if taskSpec.prodSourceLabel in ['user']:
                    userSetup = True
                # get DDM I/F
                ddmIF = self.ddmIF.getInterface(taskSpec.vo)
                # get site mapper
                siteMapper = self.taskBufferIF.getSiteMapper()
                # loop over all datasets
                avDatasetList = []
                cnDatasetMap  = {}
                for datasetID in datasetToRegister:
                    # get output and log datasets
                    tmpLog.info('getting datasetSpec with datasetID={0}'.format(datasetID))
                    tmpStat,datasetSpec = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID,
                                                                                  datasetID)
                    if not tmpStat:
                        tmpLog.error('failed to get output and log datasets')
                        return retFatal
                    # DDM backend
                    ddmBackEnd = taskSpec.getDdmBackEnd()
                    tmpLog.info('checking {0}'.format(datasetSpec.datasetName)) 
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName,datasetSpec.containerName]:
                        if targetName == None:
                            continue
                        if not targetName in avDatasetList:
                            # check dataset/container in DDM
                            tmpList = ddmIF.listDatasets(targetName)
                            if tmpList == []:
                                # get location
                                location = None
                                if targetName == datasetSpec.datasetName:
                                    # dataset
                                    if datasetSpec.site in ['',None]:
                                        if DataServiceUtils.getDestinationSE(datasetSpec.storageToken) != None:
                                            location = DataServiceUtils.getDestinationSE(datasetSpec.storageToken)
                                        elif taskSpec.cloud != None:
                                            # use T1 SE
                                            tmpT1Name = siteMapper.getCloud(taskSpec.cloud)['source']
                                            location = siteMapper.getDdmEndpoint(tmpT1Name,datasetSpec.storageToken)
                                    else:
                                        location = siteMapper.getDdmEndpoint(datasetSpec.site,datasetSpec.storageToken)
                                # register dataset/container
                                tmpLog.info('registering {0} with location={1} backend={2}'.format(targetName,
                                                                                                   location,
                                                                                                   ddmBackEnd))
                                tmpStat = ddmIF.registerNewDataset(targetName,backEnd=ddmBackEnd,location=location)
                                if not tmpStat:
                                    tmpLog.error('failed to register {0}'.format(targetName))
                                    return retFatal
                                # procedures for user 
                                if userSetup:
                                    # set owner
                                    tmpLog.info('setting owner={0}'.format(taskSpec.userName))
                                    tmpStat = ddmIF.setDatasetOwner(targetName,taskSpec.userName,backEnd=ddmBackEnd)
                                    if not tmpStat:
                                        tmpLog.error('failed to set ownership {0} with {1}'.format(targetName,
                                                                                                   taskSpec.userName))
                                        return retFatal
                                    # register location
                                    if targetName == datasetSpec.datasetName and not datasetSpec.site in ['',None]: 
                                        tmpLog.info('registring location={0}'.format(location))
                                        tmpStat = ddmIF.registerDatasetLocation(targetName,location,owner=taskSpec.userName,
                                                                                backEnd=ddmBackEnd)
                                        if not tmpStat:
                                            tmpLog.error('failed to register location {0} with {2} for {1}'.format(location,
                                                                                                                   targetName,
                                                                                                                   ddmBackEnd))
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
                        if not datasetSpec.datasetName in cnDatasetMap[datasetSpec.containerName]:
                            tmpLog.info('adding {0} to {1}'.format(datasetSpec.datasetName,datasetSpec.containerName)) 
                            tmpStat = ddmIF.addDatasetsToContainer(datasetSpec.containerName,[datasetSpec.datasetName],
                                                                   backEnd=ddmBackEnd)
                            if not tmpStat:
                                tmpLog.error('failed to add {0} to {1}'.format(datasetSpec.datasetName,
                                                                               datasetSpec.containerName))
                                return retFatal
                            cnDatasetMap[datasetSpec.containerName].append(datasetSpec.datasetName)
                        else:
                            tmpLog.info('{0} already in {1}'.format(datasetSpec.datasetName,datasetSpec.containerName)) 
                    # update dataset
                    datasetSpec.status = 'registered'
                    self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'jediTaskID':taskSpec.jediTaskID,
                                                                      'datasetID':datasetID})
            # return
            tmpLog.info('done')        
            return retOK
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doSetup failed with {0}:{1}'.format(errtype.__name__,errvalue))
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retFatal
            
