import re
import sys
import uuid
import copy
import types
import datetime

import RefinerUtils
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediException
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jedicore.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer import EventServiceUtils

# base class for task refine
class TaskRefinerBase (object):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.initializeRefiner(None)
        self.refresh()



    # refresh
    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()



    # initialize
    def initializeRefiner(self,tmpLog):
        self.taskSpec = None
        self.inMasterDatasetSpec = []
        self.inSecDatasetSpecList = []        
        self.outDatasetSpecList = []
        self.outputTemplateMap = {}
        self.jobParamsTemplate = None
        self.cloudName = None
        self.siteName = None
        self.tmpLog = tmpLog
        self.updatedTaskParams = None
        self.unmergeMasterDatasetSpec = {}
        self.unmergeDatasetSpecMap = {}
        self.oldTaskStatus = None
        self.unknownDatasetList = [] 



    # set jobParamsTemplate
    def setJobParamsTemplate(self,jobParamsTemplate):
        self.jobParamsTemplate = jobParamsTemplate


    
    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule):
        # make task spec
        taskSpec = JediTaskSpec()
        taskSpec.jediTaskID = jediTaskID
        taskSpec.taskName = taskParamMap['taskName']
        taskSpec.userName = taskParamMap['userName']
        taskSpec.vo = taskParamMap['vo']     
        taskSpec.prodSourceLabel = taskParamMap['prodSourceLabel']
        taskSpec.taskPriority = taskParamMap['taskPriority']
        taskSpec.currentPriority = taskSpec.taskPriority
        taskSpec.architecture = taskParamMap['architecture']
        taskSpec.transUses = taskParamMap['transUses']
        taskSpec.transHome = taskParamMap['transHome']
        taskSpec.transPath = taskParamMap['transPath']
        taskSpec.processingType = taskParamMap['processingType']
        taskSpec.taskType = taskParamMap['taskType']
        taskSpec.splitRule = splitRule
        taskSpec.startTime = datetime.datetime.utcnow()
        if taskParamMap.has_key('workingGroup'):
            taskSpec.workingGroup = taskParamMap['workingGroup']
        if taskParamMap.has_key('countryGroup'):
            taskSpec.countryGroup = taskParamMap['countryGroup']
        if taskParamMap.has_key('ticketID'):
            taskSpec.ticketID = taskParamMap['ticketID']
        if taskParamMap.has_key('ticketSystemType'):
            taskSpec.ticketSystemType = taskParamMap['ticketSystemType']
        if taskParamMap.has_key('reqID'):
            taskSpec.reqID = taskParamMap['reqID']
        else:
            taskSpec.reqID = jediTaskID
        if taskParamMap.has_key('coreCount'):
            taskSpec.coreCount = taskParamMap['coreCount']
        else:
            taskSpec.coreCount = 1
        if taskParamMap.has_key('walltime'):
            taskSpec.walltime = taskParamMap['walltime']
        else:
            taskSpec.walltime = 0
        if not taskParamMap.has_key('walltimeUnit'):
            # force to set NULL so that retried tasks get data from scouts again
            taskSpec.forceUpdate('walltimeUnit')
        if taskParamMap.has_key('outDiskCount'):
            taskSpec.outDiskCount = taskParamMap['outDiskCount']
        else:
            taskSpec.outDiskCount = 0
        if 'outDiskUnit' in taskParamMap:
            taskSpec.outDiskUnit = taskParamMap['outDiskUnit']
        if taskParamMap.has_key('workDiskCount'):
            taskSpec.workDiskCount = taskParamMap['workDiskCount']
        else:
            taskSpec.workDiskCount = 0
        if taskParamMap.has_key('workDiskUnit'):
            taskSpec.workDiskUnit = taskParamMap['workDiskUnit']
        if taskParamMap.has_key('ramCount'):
            taskSpec.ramCount = taskParamMap['ramCount']
        else:
            taskSpec.ramCount = 0
        if taskParamMap.has_key('ramUnit'):
            taskSpec.ramUnit = taskParamMap['ramUnit']
        if taskParamMap.has_key('baseRamCount'):
            taskSpec.baseRamCount = taskParamMap['baseRamCount']
        else:
            taskSpec.baseRamCount = 0
        # HS06 stuff
        if 'cpuTimeUnit' in taskParamMap:
            taskSpec.cpuTimeUnit = taskParamMap['cpuTimeUnit']
        if 'cpuTime' in taskParamMap:
            taskSpec.cpuTime = taskParamMap['cpuTime']
        if 'cpuEfficiency' in taskParamMap:
            taskSpec.cpuEfficiency = taskParamMap['cpuEfficiency']
        else:
            # 90% of cpu efficiency by default
            taskSpec.cpuEfficiency = 90
        if 'baseWalltime' in taskParamMap:
            taskSpec.baseWalltime = taskParamMap['baseWalltime']
        else:
            # 10min of offset by default
            taskSpec.baseWalltime = 10*60
        # for merge
        if 'mergeRamCount' in taskParamMap:
            taskSpec.mergeRamCount = taskParamMap['mergeRamCount']
        if 'mergeCoreCount' in taskParamMap:
            taskSpec.mergeCoreCount = taskParamMap['mergeCoreCount']
        # scout
        if not taskParamMap.has_key('skipScout') and not taskSpec.isPostScout():
            taskSpec.setUseScout(True)
        # cloud
        if taskParamMap.has_key('cloud'):
            self.cloudName = taskParamMap['cloud']
            taskSpec.cloud = self.cloudName
        else:
            # set dummy to force update
            taskSpec.cloud = 'dummy'
            taskSpec.cloud = None
        # site
        if taskParamMap.has_key('site'):
            self.siteName = taskParamMap['site']
            taskSpec.site = self.siteName
        else:
            # set dummy to force update
            taskSpec.site = 'dummy'
            taskSpec.site = None
        # nucleus
        if 'nucleus' in taskParamMap:
            taskSpec.nucleus = taskParamMap['nucleus']
        # preset some parameters for job cloning
        if 'useJobCloning' in taskParamMap:
            # set implicit parameters
            if not 'nEventsPerWorker' in taskParamMap:
                taskParamMap['nEventsPerWorker'] = 1
            if not 'nSitesPerJob' in taskParamMap:
                taskParamMap['nSitesPerJob'] = 2
            if not 'nEsConsumers' in taskParamMap:
                taskParamMap['nEsConsumers'] = taskParamMap['nSitesPerJob']
        # minimum granularity
        if 'minGranularity' in taskParamMap:
            taskParamMap['nEventsPerRange'] = taskParamMap['minGranularity']
        # event service flag
        if 'useJobCloning' in taskParamMap:
            taskSpec.eventService = 2
        elif taskParamMap.has_key('nEventsPerWorker'):
            taskSpec.eventService = 1
        else:
            taskSpec.eventService = 0
        # ttcr: requested time to completion
        if taskParamMap.has_key('ttcrTimestamp'):
            try:
                # get rid of the +00:00 timezone string and parse the timestamp
                taskSpec.ttcRequested = datetime.datetime.strptime(taskParamMap['ttcrTimestamp'].split('+')[0], '%Y-%m-%d %H:%M:%S.%f')
            except (IndexError, ValueError):
                pass
        # goal
        if 'goal' in taskParamMap:
            try:
                taskSpec.goal = int(float(taskParamMap['goal'])*10)
                if taskSpec.goal > 1000:
                    taskSpec.goal = None
            except:
                pass
        # campaign
        if taskParamMap.has_key('campaign'):
            taskSpec.campaign = taskParamMap['campaign']
        # request type
        if 'requestType' in taskParamMap:
            taskSpec.requestType = taskParamMap['requestType']
        self.taskSpec = taskSpec
        # set split rule    
        if 'tgtNumEventsPerJob' in taskParamMap:
            # set nEventsPerJob not respect file boundaries when nFilesPerJob is not used
            if not 'nFilesPerJob' in taskParamMap:
                self.setSplitRule(None,taskParamMap['tgtNumEventsPerJob'],JediTaskSpec.splitRuleToken['nEventsPerJob'])
        self.setSplitRule(taskParamMap,'nFilesPerJob',     JediTaskSpec.splitRuleToken['nFilesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerJob',    JediTaskSpec.splitRuleToken['nEventsPerJob'])
        self.setSplitRule(taskParamMap,'nGBPerJob',        JediTaskSpec.splitRuleToken['nGBPerJob'])
        self.setSplitRule(taskParamMap,'nMaxFilesPerJob',  JediTaskSpec.splitRuleToken['nMaxFilesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerWorker', JediTaskSpec.splitRuleToken['nEventsPerWorker'])
        self.setSplitRule(taskParamMap,'useLocalIO',       JediTaskSpec.splitRuleToken['useLocalIO'])
        self.setSplitRule(taskParamMap,'disableAutoRetry', JediTaskSpec.splitRuleToken['disableAutoRetry'])
        self.setSplitRule(taskParamMap,'nEsConsumers',     JediTaskSpec.splitRuleToken['nEsConsumers'])
        self.setSplitRule(taskParamMap,'waitInput',        JediTaskSpec.splitRuleToken['waitInput'])
        self.setSplitRule(taskParamMap,'addNthFieldToLFN', JediTaskSpec.splitRuleToken['addNthFieldToLFN'])
        self.setSplitRule(taskParamMap,'scoutSuccessRate', JediTaskSpec.splitRuleToken['scoutSuccessRate'])
        self.setSplitRule(taskParamMap,'t1Weight',         JediTaskSpec.splitRuleToken['t1Weight'])
        self.setSplitRule(taskParamMap,'maxAttemptES',     JediTaskSpec.splitRuleToken['maxAttemptES'])
        self.setSplitRule(taskParamMap,'maxAttemptEsJob',  JediTaskSpec.splitRuleToken['maxAttemptEsJob'])
        self.setSplitRule(taskParamMap,'nSitesPerJob',     JediTaskSpec.splitRuleToken['nSitesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerMergeJob',   JediTaskSpec.splitRuleToken['nEventsPerMergeJob'])
        self.setSplitRule(taskParamMap,'nFilesPerMergeJob',    JediTaskSpec.splitRuleToken['nFilesPerMergeJob'])
        self.setSplitRule(taskParamMap,'nGBPerMergeJob',       JediTaskSpec.splitRuleToken['nGBPerMergeJob'])
        self.setSplitRule(taskParamMap,'nMaxFilesPerMergeJob', JediTaskSpec.splitRuleToken['nMaxFilesPerMergeJob'])
        if 'nJumboJobs' in taskParamMap:
            self.setSplitRule(taskParamMap,'nJumboJobs',JediTaskSpec.splitRuleToken['nJumboJobs'])
            taskSpec.useJumbo = JediTaskSpec.enum_useJumbo['waiting']
        if taskParamMap.has_key('loadXML'):
            self.setSplitRule(None,3,JediTaskSpec.splitRuleToken['loadXML'])
            self.setSplitRule(None,4,JediTaskSpec.splitRuleToken['groupBoundaryID'])
        if taskParamMap.has_key('pfnList'):
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['pfnList'])
        if taskParamMap.has_key('noWaitParent') and taskParamMap['noWaitParent'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['noWaitParent'])
        if 'respectLB' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['respectLB'])
        if 'respectSplitRule' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['respectSplitRule'])
        if taskParamMap.has_key('reuseSecOnDemand'):
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['reuseSecOnDemand'])
        if 'ddmBackEnd' in taskParamMap:
            self.taskSpec.setDdmBackEnd(taskParamMap['ddmBackEnd'])
        if 'disableReassign' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['disableReassign'])
        if 'allowPartialFinish' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['allowPartialFinish'])
        if 'useExhausted' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useExhausted'])
        if 'useRealNumEvents' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useRealNumEvents'])
        if 'ipConnectivity' in taskParamMap:
            self.taskSpec.setIpConnectivity(taskParamMap['ipConnectivity'])
        if 'altStageOut' in taskParamMap:
            self.taskSpec.setAltStageOut(taskParamMap['altStageOut'])
        if 'allowInputLAN' in taskParamMap:
            self.taskSpec.setAllowInputLAN(taskParamMap['allowInputLAN'])
        if 'runUntilClosed' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['runUntilClosed'])
        if 'stayOutputOnSite' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['stayOutputOnSite'])
        if 'useJobCloning' in taskParamMap:
            scValue = EventServiceUtils.getJobCloningValue(taskParamMap['useJobCloning'])
            self.setSplitRule(None,scValue,JediTaskSpec.splitRuleToken['useJobCloning'])
        if 'failWhenGoalUnreached' in taskParamMap and taskParamMap['failWhenGoalUnreached'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['failGoalUnreached'])
        if 'switchEStoNormal' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['switchEStoNormal'])
        if 'nEventsPerRange' in taskParamMap:
            self.setSplitRule(taskParamMap,'nEventsPerRange',JediTaskSpec.splitRuleToken['dynamicNumEvents'])
        if 'allowInputWAN' in taskParamMap and taskParamMap['allowInputWAN'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['allowInputWAN'])
        if 'putLogToOS' in taskParamMap and taskParamMap['putLogToOS'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['putLogToOS'])
        if 'mergeEsOnOS' in taskParamMap and taskParamMap['mergeEsOnOS'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['mergeEsOnOS'])
        if 'writeInputToFile' in taskParamMap and taskParamMap['writeInputToFile'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['writeInputToFile'])
        if 'useFileAsSourceLFN' in taskParamMap and taskParamMap['useFileAsSourceLFN'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useFileAsSourceLFN'])
        if 'ignoreMissingInDS' in taskParamMap and taskParamMap['ignoreMissingInDS'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['ignoreMissingInDS'])
        if 'noExecStrCnv' in taskParamMap and taskParamMap['noExecStrCnv'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['noExecStrCnv'])
        if 'inFilePosEvtNum' in taskParamMap and taskParamMap['inFilePosEvtNum'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['inFilePosEvtNum'])
        if self.taskSpec.useEventService() and not taskSpec.useJobCloning():
            if 'registerEsFiles' in taskParamMap and taskParamMap['registerEsFiles'] == True:
                self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['registerEsFiles'])
        if 'disableAutoFinish' in taskParamMap and taskParamMap['disableAutoFinish'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['disableAutoFinish'])
        if 'resurrectConsumers' in taskParamMap and taskParamMap['resurrectConsumers'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['resurrectConsumers'])
        if 'usePrefetcher' in taskParamMap and taskParamMap['usePrefetcher'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['usePrefetcher'])
        # work queue
        workQueue = None
        if 'workQueueName' in taskParamMap:
            # work queue is specified
            workQueue = workQueueMapper.getQueueByName(taskSpec.vo, taskSpec.prodSourceLabel, taskParamMap['workQueueName'])
        if workQueue is None:
            # get work queue based on task attributes
            workQueue,tmpStr = workQueueMapper.getQueueWithSelParams(taskSpec.vo,
                                                                     taskSpec.prodSourceLabel,
                                                                     prodSourceLabel=taskSpec.prodSourceLabel,
                                                                     processingType=taskSpec.processingType,
                                                                     workingGroup=taskSpec.workingGroup,
                                                                     coreCount=taskSpec.coreCount,
                                                                     site=taskSpec.site,
                                                                     eventService=taskSpec.eventService,
                                                                     splitRule=taskSpec.splitRule,
                                                                     campaign=taskSpec.campaign)
        if workQueue is None:
            errStr  = 'workqueue is undefined for vo={0} label={1} '.format(taskSpec.vo,taskSpec.prodSourceLabel)
            errStr += 'processingType={0} workingGroup={1} coreCount={2} eventService={3} '.format(taskSpec.processingType,
                                                                                                   taskSpec.workingGroup,
                                                                                                   taskSpec.coreCount,
                                                                                                   taskSpec.eventService)
            errStr += 'splitRule={0} campaign={1}'.format(taskSpec.splitRule,taskSpec.campaign)
            raise RuntimeError,errStr
        self.taskSpec.workQueue_ID = workQueue.queue_id

        # Initialize the global share
        gshare = None
        if 'gshare' in taskParamMap and self.taskBufferIF.is_valid_share(taskParamMap['gshare']):
            # work queue is specified
            gshare = taskParamMap['gshare']
        else:
            # get share based on definition
            gshare = self.taskBufferIF.get_share_for_task(self.taskSpec)
            if gshare is None:
                gshare = 'Undefined' # Should not happen. Undefined is set when no share is found
                # errStr  = 'share is undefined for vo={0} label={1} '.format(taskSpec.vo,taskSpec.prodSourceLabel)
                # errStr += 'workingGroup={0} campaign={1} '.format(taskSpec.workingGroup, taskSpec.campaign)
                # raise RuntimeError,errStr

            self.taskSpec.gshare = gshare

        # Initialize the resource type
        try:
            self.taskSpec.resource_type = self.taskBufferIF.get_resource_type_task(self.taskSpec)
        except:
            self.taskSpec.resource_type = 'Undefined'

        # return
        return
    


    # basic refinement procedure
    def doBasicRefine(self,taskParamMap):
        # get input/output/log dataset specs
        nIn  = 0
        nOutMap = {}
        if isinstance(taskParamMap['log'],dict):
            itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
        else:
            itemList = taskParamMap['jobParameters'] + taskParamMap['log']
        # pseudo input
        if taskParamMap.has_key('noInput') and taskParamMap['noInput'] == True:
            tmpItem = {}
            tmpItem['type']       = 'template'
            tmpItem['value']      = ''
            tmpItem['dataset']    = 'pseudo_dataset'
            tmpItem['param_type'] = 'pseudo_input'
            itemList = [tmpItem] + itemList
        # random seed
        if RefinerUtils.useRandomSeed(taskParamMap):
            tmpItem = {}
            tmpItem['type']       = 'template'
            tmpItem['value']      = ''
            tmpItem['dataset']    = 'RNDMSEED'
            tmpItem['param_type'] = 'random_seed'
            itemList.append(tmpItem)
        # loop over all items
        allDsList = []   
        for tmpItem in itemList:
            # look for datasets
            if tmpItem['type'] == 'template' and tmpItem.has_key('dataset'):
                # avoid duplication
                if not tmpItem['dataset'] in allDsList:
                    allDsList.append(tmpItem['dataset'])
                else:
                    continue
                datasetSpec = JediDatasetSpec()
                datasetSpec.datasetName = tmpItem['dataset']
                datasetSpec.jediTaskID = self.taskSpec.jediTaskID
                datasetSpec.type = tmpItem['param_type']
                if tmpItem.has_key('container'):
                    datasetSpec.containerName = tmpItem['container']
                if tmpItem.has_key('token'):
                    datasetSpec.storageToken = tmpItem['token']
                if tmpItem.has_key('destination'):
                    datasetSpec.destination = tmpItem['destination']
                if tmpItem.has_key('attributes'):
                    datasetSpec.setDatasetAttribute(tmpItem['attributes'])
                if tmpItem.has_key('ratio'):
                    datasetSpec.setDatasetAttribute('ratio={0}'.format(tmpItem['ratio']))
                if tmpItem.has_key('eventRatio'):
                    datasetSpec.setEventRatio(tmpItem['eventRatio'])
                if tmpItem.has_key('check'):
                    datasetSpec.setDatasetAttribute('cc')
                if tmpItem.has_key('usedup'):
                    datasetSpec.setDatasetAttribute('ud')
                if tmpItem.has_key('random'):
                    datasetSpec.setDatasetAttribute('rd')
                if tmpItem.has_key('reusable'):
                    datasetSpec.setDatasetAttribute('ru')
                if tmpItem.has_key('indexConsistent'):
                    datasetSpec.setDatasetAttributeWithLabel('indexConsistent')
                if tmpItem.has_key('offset'):
                    datasetSpec.setOffset(tmpItem['offset'])
                if tmpItem.has_key('allowNoOutput'):
                    datasetSpec.allowNoOutput()
                if tmpItem.has_key('nFilesPerJob'):
                    datasetSpec.setNumFilesPerJob(tmpItem['nFilesPerJob'])
                if tmpItem.has_key('num_records'):
                    datasetSpec.setNumRecords(tmpItem['num_records'])
                if 'transient' in tmpItem:
                    datasetSpec.setTransient(tmpItem['transient'])
                if 'pseudo' in tmpItem:
                    datasetSpec.setPseudo()
                datasetSpec.vo = self.taskSpec.vo
                datasetSpec.nFiles = 0
                datasetSpec.nFilesUsed = 0
                datasetSpec.nFilesFinished = 0
                datasetSpec.nFilesFailed = 0
                datasetSpec.nFilesOnHold = 0
                datasetSpec.nFilesWaiting = 0
                datasetSpec.nEvents = 0
                datasetSpec.nEventsUsed = 0
                datasetSpec.nEventsToBeUsed = 0
                datasetSpec.status = 'defined'
                if datasetSpec.type in JediDatasetSpec.getInputTypes() + ['random_seed']:
                    datasetSpec.streamName = RefinerUtils.extractStreamName(tmpItem['value'])
                    if not tmpItem.has_key('expandedList'):
                        tmpItem['expandedList'] = []
                    # dataset names could be comma-concatenated
                    datasetNameList = datasetSpec.datasetName.split(',')
                    # datasets could be added by incexec
                    incexecDS = 'dsFor{0}'.format(datasetSpec.streamName)
                    # remove /XYZ
                    incexecDS = incexecDS.split('/')[0]
                    if taskParamMap.has_key(incexecDS):
                        for tmpDatasetName in taskParamMap[incexecDS].split(','):
                            if not tmpDatasetName in datasetNameList:
                                datasetNameList.append(tmpDatasetName)
                    # loop over all dataset names
                    inDatasetSpecList = []
                    for datasetName in datasetNameList:
                        # skip empty
                        if datasetName == '':
                            continue
                        # expand
                        if datasetSpec.isPseudo() or datasetSpec.type in ['random_seed'] or datasetName == 'DBR_LATEST':
                            # pseudo input
                            tmpDatasetNameList = [datasetName]
                        elif tmpItem.has_key('expand') and tmpItem['expand'] == True:
                            # expand dataset container
                            tmpDatasetNameList = self.ddmIF.getInterface(self.taskSpec.vo).expandContainer(datasetName)
                        else:
                            # normal dataset name
                            tmpDatasetNameList = self.ddmIF.getInterface(self.taskSpec.vo).listDatasets(datasetName)
                        for elementDatasetName in tmpDatasetNameList:
                            if nIn > 0 or not elementDatasetName in tmpItem['expandedList']:
                                tmpItem['expandedList'].append(elementDatasetName)
                                inDatasetSpec = copy.copy(datasetSpec)
                                inDatasetSpec.datasetName = elementDatasetName
                                inDatasetSpec.containerName = datasetName
                                inDatasetSpecList.append(inDatasetSpec)
                    # empty input
                    if inDatasetSpecList == [] and self.oldTaskStatus != 'rerefine':
                        errStr = 'doBasicRefine : unknown input dataset "{0}"'.format(datasetSpec.datasetName)
                        self.taskSpec.setErrDiag(errStr)
                        if not datasetSpec.datasetName in self.unknownDatasetList:
                            self.unknownDatasetList.append(datasetSpec.datasetName)
                        raise JediException.UnknownDatasetError,errStr
                    # set master flag
                    for inDatasetSpec in inDatasetSpecList:    
                        if nIn == 0:
                            # master
                            self.inMasterDatasetSpec.append(inDatasetSpec)
                        else:
                            # secondary
                            self.inSecDatasetSpecList.append(inDatasetSpec)
                    nIn += 1    
                    continue
                if datasetSpec.type in ['output','log']:
                    if not nOutMap.has_key(datasetSpec.type):
                        nOutMap[datasetSpec.type] = 0
                    # make stream name
                    datasetSpec.streamName = "{0}{1}".format(datasetSpec.type.upper(),nOutMap[datasetSpec.type])
                    nOutMap[datasetSpec.type] += 1
                    # set attribute for event service
                    if self.taskSpec.useEventService() and taskParamMap.has_key('objectStore') and datasetSpec.type in ['output']:
                        datasetSpec.setObjectStore(taskParamMap['objectStore'])
                    # extract output filename template and change the value field
                    outFileTemplate,tmpItem['value'] = RefinerUtils.extractReplaceOutFileTemplate(tmpItem['value'],
                                                                                                  datasetSpec.streamName)
                    # make output template
                    if outFileTemplate != None:
                        if tmpItem.has_key('offset'):
                            offsetVal = 1 + tmpItem['offset']
                        else:
                            offsetVal = 1
                        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                          'serialNr' : offsetVal,
                                          'streamName' : datasetSpec.streamName,
                                          'filenameTemplate' : outFileTemplate,
                                          'outtype' : datasetSpec.type,
                                          }
                        if self.outputTemplateMap.has_key(datasetSpec.outputMapKey()):
                            # multiple files are associated to the same output datasets
                            self.outputTemplateMap[datasetSpec.outputMapKey()].append(outTemplateMap)
                            # don't insert the same output dataset
                            continue
                        self.outputTemplateMap[datasetSpec.outputMapKey()] = [outTemplateMap]
                    # append
                    self.outDatasetSpecList.append(datasetSpec)
                    # make unmerged dataset
                    if taskParamMap.has_key('mergeOutput') and taskParamMap['mergeOutput'] == True:
                        umDatasetSpec = JediDatasetSpec()
                        umDatasetSpec.datasetName = 'panda.um.' + datasetSpec.datasetName
                        umDatasetSpec.jediTaskID = self.taskSpec.jediTaskID
                        umDatasetSpec.storageToken = 'TOMERGE'
                        umDatasetSpec.vo = datasetSpec.vo
                        umDatasetSpec.type = "tmpl_trn_" + datasetSpec.type
                        umDatasetSpec.nFiles = 0
                        umDatasetSpec.nFilesUsed = 0
                        umDatasetSpec.nFilesToBeUsed = 0
                        umDatasetSpec.nFilesFinished = 0
                        umDatasetSpec.nFilesFailed = 0
                        umDatasetSpec.nFilesOnHold = 0
                        umDatasetSpec.status = 'defined'
                        umDatasetSpec.streamName = datasetSpec.streamName
                        if datasetSpec.isAllowedNoOutput():
                            umDatasetSpec.allowNoOutput()
                        # ratio
                        if datasetSpec.getRatioToMaster() > 1:
                            umDatasetSpec.setDatasetAttribute('ratio={0}'.format(datasetSpec.getRatioToMaster()))
                        # make unmerged output template 
                        if outFileTemplate != None:
                            umOutTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                                'serialNr' : 1,
                                                'streamName' : umDatasetSpec.streamName,
                                                'outtype' : datasetSpec.type,
                                                }
                            # append temporary name
                            if taskParamMap.has_key('umNameAtEnd') and taskParamMap['umNameAtEnd'] == True:
                                # append temporary name at the end
                                umOutTemplateMap['filenameTemplate'] = outFileTemplate + '.panda.um'
                            else:
                                umOutTemplateMap['filenameTemplate'] = 'panda.um.' + outFileTemplate
                            if self.outputTemplateMap.has_key(umDatasetSpec.outputMapKey()):
                                # multiple files are associated to the same output datasets
                                self.outputTemplateMap[umDatasetSpec.outputMapKey()].append(umOutTemplateMap)
                                # don't insert the same output dataset
                                continue
                            self.outputTemplateMap[umDatasetSpec.outputMapKey()] = [umOutTemplateMap]
                        # use log as master for merging
                        if datasetSpec.type == 'log':
                            self.unmergeMasterDatasetSpec[datasetSpec.outputMapKey()] = umDatasetSpec
                        else:
                            # append
                            self.unmergeDatasetSpecMap[datasetSpec.outputMapKey()] = umDatasetSpec
        # set attributes for merging
        if taskParamMap.has_key('mergeOutput') and taskParamMap['mergeOutput'] == True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['mergeOutput'])
        # make job parameters
        rndmSeedOffset = None
        firstEventOffset = None
        jobParameters = ''
        for tmpItem in taskParamMap['jobParameters']:
            if tmpItem.has_key('value'):
                # hidden parameter
                if tmpItem.has_key('hidden') and tmpItem['hidden'] == True:
                    continue
                # add tags for ES-only parameters
                esOnly = False
                if 'es_only' in tmpItem and tmpItem['es_only'] == True:
                    esOnly = True
                if esOnly:
                    jobParameters += '<PANDA_ES_ONLY>'
                jobParameters += '{0}'.format(tmpItem['value'])
                if esOnly:
                    jobParameters += '</PANDA_ES_ONLY>'
                # padding
                if tmpItem.has_key('padding') and tmpItem['padding'] == False:
                    pass
                else:
                    jobParameters += ' '
                # get offset for random seed and first event
                if tmpItem['type'] == 'template' and tmpItem['param_type'] == 'number':
                    if '${RNDMSEED}' in tmpItem['value']:
                        if tmpItem.has_key('offset'):
                            rndmSeedOffset = tmpItem['offset']
                        else:
                            rndmSeedOffset = 0
                    elif '${FIRSTEVENT}' in tmpItem['value']:
                        if tmpItem.has_key('offset'):
                            firstEventOffset = tmpItem['offset']    
        jobParameters = jobParameters[:-1]
        # append parameters for event service merging if necessary
        esmergeParams = self.getParamsForEventServiceMerging(taskParamMap)
        if esmergeParams != None:
            jobParameters += esmergeParams
        self.setJobParamsTemplate(jobParameters)
        # set random seed offset
        if rndmSeedOffset != None:
            self.setSplitRule(None,rndmSeedOffset,JediTaskSpec.splitRuleToken['randomSeed'])
        if firstEventOffset != None:
            self.setSplitRule(None,firstEventOffset,JediTaskSpec.splitRuleToken['firstEvent'])
        # return
        return


    
    # replace placeholder with dict provided by prepro job
    def replacePlaceHolders(self,paramItem,placeHolderName,newValue):
        if isinstance(paramItem,types.DictType):
            # loop over all dict params
            for tmpParName,tmpParVal in paramItem.iteritems():
                if tmpParVal == placeHolderName:
                    # replace placeholder
                    paramItem[tmpParName] = newValue
                elif isinstance(tmpParVal,types.DictType) or \
                        isinstance(tmpParVal,types.ListType):
                    # recursive execution
                    self.replacePlaceHolders(tmpParVal,placeHolderName,newValue)
        elif isinstance(paramItem,types.ListType):
            # loop over all list items
            for tmpItem in paramItem:
                self.replacePlaceHolders(tmpItem,placeHolderName,newValue)

    

    # refinement procedure for preprocessing
    def doPreProRefine(self,taskParamMap):
        # no preprocessing
        if not taskParamMap.has_key('preproSpec'):
            return None,taskParamMap
        # already preprocessed
        if self.taskSpec.checkPreProcessed():
            # get replaced task params
            tmpStat,tmpJsonStr = self.taskBufferIF.getPreprocessMetadata_JEDI(self.taskSpec.jediTaskID)
            try:
                # replace placeholders 
                replaceParams = RefinerUtils.decodeJSON(tmpJsonStr)
                self.tmpLog.debug("replace placeholders with "+str(replaceParams))
                for tmpKey,tmpVal in replaceParams.iteritems():
                    self.replacePlaceHolders(taskParamMap,tmpKey,tmpVal)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                self.tmpLog.error('{0} failed to get additional task params with {1}:{2}'.format(self.__class__.__name__,
                                                                                                 errtype.__name__,errvalue))
                return False,taskParamMap
            # succeeded
            self.updatedTaskParams = taskParamMap
            return None,taskParamMap
        # make dummy dataset to keep track of preprocessing
        datasetSpec = JediDatasetSpec()
        datasetSpec.datasetName = 'panda.pp.in.{0}.{1}'.format(uuid.uuid4(),self.taskSpec.jediTaskID)
        datasetSpec.jediTaskID = self.taskSpec.jediTaskID
        datasetSpec.type = 'pp_input'
        datasetSpec.vo = self.taskSpec.vo
        datasetSpec.nFiles = 1
        datasetSpec.nFilesUsed = 0
        datasetSpec.nFilesToBeUsed = 1
        datasetSpec.nFilesFinished = 0
        datasetSpec.nFilesFailed = 0
        datasetSpec.nFilesOnHold = 0
        datasetSpec.status = 'ready'
        self.inMasterDatasetSpec.append(datasetSpec)
        # make file 
        fileSpec = JediFileSpec()
        fileSpec.jediTaskID   = datasetSpec.jediTaskID
        fileSpec.type         = datasetSpec.type
        fileSpec.status       = 'ready'            
        fileSpec.lfn          = 'pseudo_lfn'
        fileSpec.attemptNr    = 0
        fileSpec.maxAttempt   = 3
        fileSpec.keepTrack    = 1
        datasetSpec.addFile(fileSpec)
        # make log dataset
        logDatasetSpec = JediDatasetSpec()
        logDatasetSpec.datasetName = 'panda.pp.log.{0}.{1}'.format(uuid.uuid4(),self.taskSpec.jediTaskID)
        logDatasetSpec.jediTaskID = self.taskSpec.jediTaskID
        logDatasetSpec.type = 'tmpl_pp_log'
        logDatasetSpec.streamName = 'PP_LOG'
        logDatasetSpec.vo = self.taskSpec.vo
        logDatasetSpec.nFiles = 0
        logDatasetSpec.nFilesUsed = 0
        logDatasetSpec.nFilesToBeUsed = 0
        logDatasetSpec.nFilesFinished = 0
        logDatasetSpec.nFilesFailed = 0
        logDatasetSpec.nFilesOnHold = 0
        logDatasetSpec.status = 'defined'
        self.outDatasetSpecList.append(logDatasetSpec)
        # make output template for log
        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                          'serialNr' : 1,
                          'streamName' : logDatasetSpec.streamName,
                          'filenameTemplate' : "{0}._${{SN}}.log.tgz".format(logDatasetSpec.datasetName),
                          'outtype' : re.sub('^tmpl_','',logDatasetSpec.type),
                          }
        self.outputTemplateMap[logDatasetSpec.outputMapKey()] = [outTemplateMap]
        # set split rule to use preprocessing
        self.taskSpec.setPrePro()
        # set task status
        self.taskSpec.status = 'topreprocess'
        # return
        return True,taskParamMap



    # set split rule
    def setSplitRule(self,taskParamMap,keyName,valName):
        if taskParamMap != None:
            if not taskParamMap.has_key(keyName):
                return
            tmpStr = '{0}={1}'.format(valName,taskParamMap[keyName])
        else:
            tmpStr = '{0}={1}'.format(valName,keyName)
        if self.taskSpec.splitRule in [None,'']:
            self.taskSpec.splitRule = tmpStr
        else:
            tmpMatch = re.search(valName+'=(-*\d+)(,-*\d+)*',self.taskSpec.splitRule)
            if tmpMatch == None:
                # append
                self.taskSpec.splitRule += ',{0}'.format(tmpStr)
            else:
                # replace
                self.taskSpec.splitRule = self.taskSpec.splitRule.replace(tmpMatch.group(0), tmpStr)
        return    



    # get parameters for event service merging
    def getParamsForEventServiceMerging(self,taskParamMap):
        # no event service
        if not self.taskSpec.useEventService():
            return None
        # extract parameters
        transPath = 'UnDefined'
        jobParameters = 'UnDefined'
        if taskParamMap.has_key('esmergeSpec'):
            if taskParamMap['esmergeSpec'].has_key('transPath'):
                transPath = taskParamMap['esmergeSpec']['transPath']
            if taskParamMap['esmergeSpec'].has_key('jobParameters'):
                jobParameters = taskParamMap['esmergeSpec']['jobParameters']
        # return
        return '<PANDA_ESMERGE_TRF>'+transPath+'</PANDA_ESMERGE_TRF>'+'<PANDA_ESMERGE_JOBP>'+jobParameters+'</PANDA_ESMERGE_JOBP>'

        
    
Interaction.installSC(TaskRefinerBase)
