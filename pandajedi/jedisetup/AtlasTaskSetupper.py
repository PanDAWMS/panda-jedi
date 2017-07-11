import re
import sys
import uuid
import json

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from TaskSetupperBase import TaskSetupperBase

from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import EventServiceUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task setup for ATLAS
class AtlasTaskSetupper (TaskSetupperBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskSetupperBase.__init__(self,taskBufferIF,ddmIF)


    # main to setup task
    def doSetup(self,taskSpec,datasetToRegister,pandaJobs):
        # make logger
        tmpLog = MsgWrapper(logger,"<jediTaskID={0}>".format(taskSpec.jediTaskID))
        tmpLog.info('start label={0} taskType={1}'.format(taskSpec.prodSourceLabel,taskSpec.taskType))
        # returns
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        retOK       = self.SC_SUCCEEDED
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # register datasets
            if datasetToRegister != [] or taskSpec.prodSourceLabel in ['user']:
                # prod vs anal
                userSetup = False
                if taskSpec.prodSourceLabel in ['user']:
                    userSetup = True
                    # collect datasetID to register datasets/containers just in case
                    for tmpPandaJob in pandaJobs:
                        if not tmpPandaJob.produceUnMerge():
                            for tmpFileSpec in tmpPandaJob.Files:
                                if tmpFileSpec.type in ['output','log']:
                                    if not tmpFileSpec.datasetID in datasetToRegister:
                                        datasetToRegister.append(tmpFileSpec.datasetID)
                tmpLog.info('datasetToRegister={0}'.format(str(datasetToRegister)))
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
                    if datasetSpec.isPseudo():
                        tmpLog.info('skip pseudo dataset')
                        continue
                    # DDM backend
                    ddmBackEnd = taskSpec.getDdmBackEnd()
                    tmpLog.info('checking {0}'.format(datasetSpec.datasetName)) 
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName,datasetSpec.containerName]:
                        if targetName == None:
                            continue
                        if not targetName in avDatasetList:
                            # set lifetime
                            if targetName.startswith('panda'):
                                if datasetSpec.type == 'trn_log' and taskSpec.prodSourceLabel == 'managed':
                                    lifetime = 365
                                else:
                                    lifetime = 14
                            else:
                                lifetime = None
                            # check dataset/container in DDM
                            tmpList = ddmIF.listDatasets(targetName)
                            if tmpList == []:
                                # get location
                                location = None
                                locForRule = None
                                if targetName == datasetSpec.datasetName:
                                    # dataset
                                    if datasetSpec.site in ['',None]:
                                        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) != None:
                                            locForRule = datasetSpec.destination
                                        elif DataServiceUtils.getDestinationSE(datasetSpec.storageToken) != None:
                                            location = DataServiceUtils.getDestinationSE(datasetSpec.storageToken)
                                        elif taskSpec.cloud != None:
                                            # use T1 SE
                                            tmpT1Name = siteMapper.getCloud(taskSpec.cloud)['source']
                                            location = siteMapper.getDdmEndpoint(tmpT1Name,datasetSpec.storageToken)
                                    else:
                                        tmpLog.info('site={0} token='.format(datasetSpec.site,datasetSpec.storageToken))
                                        location = siteMapper.getDdmEndpoint(datasetSpec.site,datasetSpec.storageToken)
                                if locForRule == None:
                                    locForRule = location
                                # set metadata
                                if taskSpec.prodSourceLabel in ['managed','test'] and targetName == datasetSpec.datasetName:
                                    metaData = {}
                                    metaData['task_id'] = taskSpec.jediTaskID
                                    if not taskSpec.campaign in [None,'']:
                                        metaData['campaign'] = taskSpec.campaign 
                                    if datasetSpec.getTransient() != None:
                                        metaData['transient'] = datasetSpec.getTransient()
                                else:
                                    metaData = None
                                # register dataset/container
                                tmpLog.info('registering {0} with location={1} backend={2} lifetime={3} meta={4}'.format(targetName,
                                                                                                                         location,
                                                                                                                         ddmBackEnd,
                                                                                                                         lifetime,
                                                                                                                         str(metaData)))
                                tmpStat = ddmIF.registerNewDataset(targetName,backEnd=ddmBackEnd,location=location,
                                                                   lifetime=lifetime,metaData=metaData)
                                if not tmpStat:
                                    tmpLog.error('failed to register {0}'.format(targetName))
                                    return retFatal
                                # procedures for user 
                                if userSetup or DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) != None:
                                    # register location
                                    tmpToRegister = False
                                    if userSetup and targetName == datasetSpec.datasetName and not datasetSpec.site in ['',None]:
                                        userName = taskSpec.userName
                                        grouping = None
                                        tmpToRegister = True
                                    elif DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) != None:
                                        userName = None
                                        grouping = 'NONE'
                                        tmpToRegister = True
                                    if tmpToRegister:
                                        activity = DataServiceUtils.getActivityForOut(taskSpec.prodSourceLabel)
                                        tmpLog.info('registering location={0} lifetime={1}days activity={2} grouping={3}'.format(locForRule,lifetime,
                                                                                                                                 activity,grouping))
                                        tmpStat = ddmIF.registerDatasetLocation(targetName,locForRule,owner=userName,
                                                                                lifetime=lifetime,backEnd=ddmBackEnd,
                                                                                activity=activity,grouping=grouping)
                                        if not tmpStat:
                                            tmpLog.error('failed to register location {0} for {1}'.format(locForRule,
                                                                                                          targetName))
                                            return retFatal
                                        # double copy
                                        if userSetup and datasetSpec.type == 'output':
                                            if datasetSpec.destination != datasetSpec.site:
                                                tmpLog.info('skip making double copy as destination={0} is not site={1}'.format(datasetSpec.destination,
                                                                                                                                datasetSpec.site))
                                            else:
                                                locForDouble = 'type=SCRATCHDISK'
                                                tmpMsg  = 'registering double copy '
                                                tmpMsg += 'location="{0}" lifetime={1}days activity={2} for dataset={3}'.format(locForDouble,lifetime,
                                                                                                                                activity,targetName)
                                                tmpLog.info(tmpMsg)
                                                tmpStat = ddmIF.registerDatasetLocation(targetName,locForDouble,copies=2,owner=userName,
                                                                                        lifetime=lifetime,activity=activity,
                                                                                        grouping='NONE',weight='freespace')
                                                if not tmpStat:
                                                    tmpLog.error('failed to register double copylocation {0} for {1}'.format(locForDouble,
                                                                                                                           targetName))
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
                    # register ES datasets
                    if False: # FIXME taskSpec.useEventService() and not taskSpec.useJobCloning() and datasetSpec.type == 'output':
                        targetName = datasetSpec.datasetName +  EventServiceUtils.esSuffixDDM
                        location = None
                        metaData = {}
                        metaData['task_id'] = taskSpec.jediTaskID
                        metaData['hidden']  = True
                        tmpLog.info('registering ES dataset {0} with location={1} meta={2}'.format(targetName,
                                                                                                   location,
                                                                                                   str(metaData)))
                        tmpStat = ddmIF.registerNewDataset(targetName,location=location,metaData=metaData)
                        if not tmpStat:
                            tmpLog.error('failed to register ES dataset {0}'.format(targetName))
                            return retFatal
                        # register rule
                        location = 'type=ES' 
                        activity = DataServiceUtils.getActivityForOut(taskSpec.prodSourceLabel)
                        grouping = 'NONE'
                        tmpLog.info('registering location={0} activity={1} grouping={2}'.format(location,
                                                                                                activity,
                                                                                                grouping))
                        tmpStat = ddmIF.registerDatasetLocation(targetName,location,activity=activity,
                                                                grouping=grouping)
                        if not tmpStat:
                            tmpLog.error('failed to register location {0} with {2} for {1}'.format(location,
                                                                                                   targetName,
                                                                                                   activity))
                            return retFatal
            # open datasets
            if taskSpec.prodSourceLabel in ['managed','test']:
                # get the list of output/log datasets
                outDatasetList = []
                for tmpPandaJob in pandaJobs:
                    for tmpFileSpec in tmpPandaJob.Files:
                        if tmpFileSpec.type in ['output','log']:
                            if not tmpFileSpec.destinationDBlock in outDatasetList:
                                outDatasetList.append(tmpFileSpec.destinationDBlock)
                # open datasets
                for outDataset in outDatasetList:
                    tmpLog.info('open {0}'.format(outDataset))
                    ddmIF.openDataset(outDataset)
                    # unset lifetime
                    ddmIF.setDatasetMetadata(outDataset,'lifetime',None)
            # return
            tmpLog.info('done')        
            return retOK
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doSetup failed with {0}:{1}'.format(errtype.__name__,errvalue))
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retFatal
