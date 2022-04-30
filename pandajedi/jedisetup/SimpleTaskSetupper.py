import traceback

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from .TaskSetupperBase import TaskSetupperBase


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task setup for general purpose
class SimpleTaskSetupper (TaskSetupperBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskSetupperBase.__init__(self,taskBufferIF,ddmIF)


    # main to setup task
    def doSetup(self, taskSpec, datasetToRegister, pandaJobs):
        # make logger
        tmpLog = MsgWrapper(logger, "< jediTaskID={0} >".format(taskSpec.jediTaskID))
        tmpLog.info('start label={0} taskType={1}'.format(taskSpec.prodSourceLabel, taskSpec.taskType))
        # returns
        retFatal = self.SC_FATAL
        retOK = self.SC_SUCCEEDED
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud)
            # skip if DDM I/F is inactive
            if not ddmIF:
                tmpLog.info('skip due to inactive DDM I/F')
                return retOK
            # collect datasetID to register datasets/containers just in case
            for tmpPandaJob in pandaJobs:
                if not tmpPandaJob.produceUnMerge():
                    for tmpFileSpec in tmpPandaJob.Files:
                        if tmpFileSpec.type in ['output', 'log']:
                            if tmpFileSpec.datasetID not in datasetToRegister:
                                datasetToRegister.append(tmpFileSpec.datasetID)
            # register datasets
            if datasetToRegister:
                tmpLog.info('datasetToRegister={0}'.format(str(datasetToRegister)))
                # get site mapper
                siteMapper = self.taskBufferIF.getSiteMapper()

                # loop over all datasets
                avDatasetList = []
                cnDatasetMap  = {}
                ddmBackEnd = 'rucio'
                for datasetID in datasetToRegister:
                    # get output and log datasets
                    tmpLog.info('getting datasetSpec with datasetID={0}'.format(datasetID))
                    tmpStat, datasetSpec = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID,
                                                                                   datasetID)
                    if not tmpStat:
                        tmpLog.error('failed to get output and log datasets')
                        return retFatal
                    if datasetSpec.isPseudo():
                        tmpLog.info('skip pseudo dataset')
                        continue

                    tmpLog.info('checking {0}'.format(datasetSpec.datasetName))
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName, datasetSpec.containerName]:
                        if not targetName:
                            continue
                        if targetName in avDatasetList:
                            tmpLog.info('{0} already registered'.format(targetName))
                            continue
                        # set lifetime
                        lifetime = None
                        # check dataset/container in DDM
                        tmpList = ddmIF.listDatasets(targetName)
                        if not tmpList:
                            # get location
                            location = None
                            locForRule = None
                            if targetName == datasetSpec.datasetName:
                                # dataset
                                tmpLog.info('dest={0}'.format(datasetSpec.destination))
                                if datasetSpec.destination:
                                    if siteMapper.checkSite(datasetSpec.destination):
                                        location = siteMapper.getSite('BNL_OSG_SPHENIX').ddm_output['default']
                                    else:
                                        location = datasetSpec.destination
                            if locForRule is None:
                                locForRule = location
                            # set metadata
                            if targetName == datasetSpec.datasetName:
                                metaData = {}
                                metaData['task_id'] = taskSpec.jediTaskID
                                if taskSpec.campaign:
                                    metaData['campaign'] = taskSpec.campaign
                            else:
                                metaData = None
                            # register dataset/container
                            tmpLog.info('registering {0} with location={1} backend={2} lifetime={3} meta={4}'.format(targetName,
                                                                                                                     location,
                                                                                                                     ddmBackEnd,
                                                                                                                     lifetime,
                                                                                                                     str(metaData)))
                            tmpStat = ddmIF.registerNewDataset(targetName, backEnd=ddmBackEnd, location=location,
                                                               lifetime=lifetime, metaData=metaData)
                            if not tmpStat:
                                tmpLog.error('failed to register {0}'.format(targetName))
                                return retFatal
                            # register location
                            if locForRule:
                                """
                                if taskSpec.workingGroup:
                                    userName = taskSpec.workingGroup
                                else:
                                    userName = taskSpec.userName
                                """
                                userName = None
                                activity = None
                                grouping = None
                                tmpLog.info('registering location={} lifetime={} days activity={} grouping={} '
                                                    'owner={}'.format(locForRule, lifetime, activity, grouping,
                                                                      userName))
                                tmpStat = ddmIF.registerDatasetLocation(targetName, locForRule, owner=userName,
                                                                        lifetime=lifetime, backEnd=ddmBackEnd,
                                                                        activity=activity, grouping=grouping)
                                if not tmpStat:
                                    tmpLog.error('failed to register location {0} for {1}'.format(locForRule,
                                                                                                  targetName))
                                    return retFatal
                            avDatasetList.append(targetName)

                    # check if dataset is in the container
                    if datasetSpec.containerName and datasetSpec.containerName != datasetSpec.datasetName:
                        # get list of constituent datasets in the container
                        if datasetSpec.containerName not in cnDatasetMap:
                            cnDatasetMap[datasetSpec.containerName] = ddmIF.listDatasetsInContainer(datasetSpec.containerName)
                        # add dataset
                        if datasetSpec.datasetName not in cnDatasetMap[datasetSpec.containerName]:
                            tmpLog.info('adding {0} to {1}'.format(datasetSpec.datasetName, datasetSpec.containerName))
                            tmpStat = ddmIF.addDatasetsToContainer(datasetSpec.containerName, [datasetSpec.datasetName],
                                                                   backEnd=ddmBackEnd)
                            if not tmpStat:
                                tmpLog.error('failed to add {0} to {1}'.format(datasetSpec.datasetName,
                                                                               datasetSpec.containerName))
                                return retFatal
                            cnDatasetMap[datasetSpec.containerName].append(datasetSpec.datasetName)
                        else:
                            tmpLog.info('{0} already in {1}'.format(datasetSpec.datasetName, datasetSpec.containerName))
                    # update dataset
                    datasetSpec.status = 'registered'
                    self.taskBufferIF.updateDataset_JEDI(datasetSpec, {'jediTaskID': taskSpec.jediTaskID,
                                                                       'datasetID': datasetID})
            # return
            tmpLog.info('done')
            return retOK
        except Exception as e:
            errStr = 'doSetup failed with {}'.format(str(e))
            tmpLog.error(errStr + traceback.format_exc())
            taskSpec.setErrDiag(errStr)
            return retFatal
