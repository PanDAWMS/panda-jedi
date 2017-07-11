import re
import sys
import shlex
import random
import datetime

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase

from pandaserver.dataservice import DataServiceUtils


# brokerage for ATLAS production
class AtlasProdTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskRefinerBase.__init__(self, taskBufferIF, ddmIF)



    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule):
        tmpLog = self.tmpLog
        # set ddmBackEnd
        if not 'ddmBackEnd' in taskParamMap:
            taskParamMap['ddmBackEnd'] = 'rucio'
        # get number of unprocessed events for event service
        autoEsConversion = False
        if 'esConvertible' in taskParamMap and taskParamMap['esConvertible'] == True:
            maxPrio = self.taskBufferIF.getConfigValue('taskrefiner', 'AES_MAXTASKPRIORITY', 'jedi', 'atlas')
            minPrio = self.taskBufferIF.getConfigValue('taskrefiner', 'AES_MINTASKPRIORITY', 'jedi', 'atlas')
            if maxPrio is not None and maxPrio < taskParamMap['taskPriority']:
                pass
            elif minPrio is not None and minPrio > taskParamMap['taskPriority']:
                pass
            else:
                # get threshold
                minNumEvents = self.taskBufferIF.getConfigValue('taskrefiner', 'AES_EVENTPOOLSIZE', 'jedi', 'atlas')
                nEvents, lastTaskTime = self.taskBufferIF.getNumUnprocessedEvents_JEDI(taskParamMap['vo'],
                                                                                       taskParamMap['prodSourceLabel'],
                                                                                       {'eventService': 1})
                tmpLog.info('check for ES tot_num_unprocessed_events_AES={0} target_num_events_AES={1} last_AES_task_time={2}'.format(nEvents,
                                                                                                                                      minNumEvents,
                                                                                                                                      lastTaskTime))
                # not chane many tasks at once
                if lastTaskTime is None or (lastTaskTime < datetime.datetime.utcnow() - datetime.timedelta(minutes=5)):
                    if minNumEvents is not None and nEvents < minNumEvents:
                        autoEsConversion = True
                        tmpLog.info('converted to AES')
        # add ES paramsters
        if ('esFraction' in taskParamMap and taskParamMap['esFraction'] > 0) or autoEsConversion:
            tmpStr  = '<PANDA_ES_ONLY>--eventService=True</PANDA_ES_ONLY>'
            taskParamMap['jobParameters'].append({'type':'constant',
                                                  'value':tmpStr})
            if 'nEventsPerWorker' not in taskParamMap and \
                    (('esFraction' in taskParamMap and taskParamMap['esFraction'] > random.random()) or autoEsConversion):
                taskParamMap['nEventsPerWorker'] = 1
                if 'nEsConsumers' not in taskParamMap:
                    tmpVal = self.taskBufferIF.getConfigValue('taskrefiner', 'AES_NESCONSUMERS', 'jedi', 'atlas')
                    if tmpVal is None:
                        tmpVal = 1
                    taskParamMap['nEsConsumers'] = tmpVal
                if 'nSitesPerJob' not in taskParamMap:
                    tmpVal = self.taskBufferIF.getConfigValue('taskrefiner', 'AES_NSITESPERJOB', 'jedi', 'atlas')
                    if tmpVal is not None:
                        taskParamMap['nSitesPerJob'] = tmpVal
                if 'mergeEsOnOS' not in taskParamMap:
                    taskParamMap['mergeEsOnOS'] = True
                if 'maxAttemptES' not in taskParamMap:
                    taskParamMap['maxAttemptES'] = 10
                taskParamMap['coreCount'] = 0
        TaskRefinerBase.extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule)



    # main
    def doRefine(self,jediTaskID,taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug('start taskType={0}'.format(self.taskSpec.taskType))
        try:
            # basic refine    
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                if DataServiceUtils.isDBR(datasetSpec.datasetName):
                    datasetSpec.attributes = 'repeat,nosplit'
            # enable consistency check
            if not self.taskSpec.parent_tid in [None,self.taskSpec.jediTaskID]:
                for datasetSpec in self.inMasterDatasetSpec:
                    if datasetSpec.isMaster() and datasetSpec.type == 'input':
                        datasetSpec.enableCheckConsistency()
            # append attempt number
            for tmpKey,tmpOutTemplateMapList in self.outputTemplateMap.iteritems():
                for tmpOutTemplateMap in tmpOutTemplateMapList:
                    outFileTemplate = tmpOutTemplateMap['filenameTemplate']
                    if re.search('\.\d+$',outFileTemplate) == None and not outFileTemplate.endswith('.panda.um'):
                        tmpOutTemplateMap['filenameTemplate'] = outFileTemplate + '.1'
            # extract input datatype
            datasetTypeListIn = []
            for datasetSpec in self.inMasterDatasetSpec+self.inSecDatasetSpecList:
                datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                if not datasetType in ['',None]:
                    datasetTypeListIn.append(datasetType)
            # extract datatype and set destination if nessesary
            datasetTypeList = []
            for datasetSpec in self.outDatasetSpecList:
                datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                if not datasetType in ['',None]:
                    datasetTypeList.append(datasetType)
            # set numThrottled to use the task throttling mechanism
            if not 'noThrottle' in taskParamMap:
                self.taskSpec.numThrottled = 0
            # set to register datasets
            self.taskSpec.setToRegisterDatasets()
            # set transient to parent datasets
            if self.taskSpec.processingType in ['merge'] and not self.taskSpec.parent_tid in [None,self.taskSpec.jediTaskID]:
                # get parent
                tmpStat,parentTaskSpec = self.taskBufferIF.getTaskDatasetsWithID_JEDI(self.taskSpec.parent_tid,None,False)
                if tmpStat and parentTaskSpec != None:
                    # set transient to parent datasets
                    metaData = {'transient':True}
                    for datasetSpec in parentTaskSpec.datasetSpecList:
                        if datasetSpec.type in ['log','output']:
                            datasetType = DataServiceUtils.getDatasetType(datasetSpec.datasetName)
                            if not datasetType in ['',None] and datasetType in datasetTypeList and datasetType in datasetTypeListIn:
                                tmpLog.info('set metadata={0} to parent jediTaskID={1}:datasetID={2}:Name={3}'.format(str(metaData),
                                                                                                                      self.taskSpec.parent_tid,
                                                                                                                      datasetSpec.datasetID,
                                                                                                                      datasetSpec.datasetName))
                                for metadataName,metadaValue in metaData.iteritems():
                                    self.ddmIF.getInterface(self.taskSpec.vo).setDatasetMetadata(datasetSpec.datasetName,
                                                                                                 metadataName,metadaValue)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            raise errtype,errvalue
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            


    # insert string
    def insertString(self,paramName,tmpStr,origStr):
        items = shlex.split(origStr,posix=False)
        newStr = ''
        for item in items:
            if not paramName in item:
                newStr += item
            else:
                newStr += item[:-1]
                newStr += tmpStr
                newStr += item[-1]
            newStr += ' '
        return newStr


