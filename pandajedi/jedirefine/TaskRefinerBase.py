import re
import sys
import uuid
import copy
import datetime

from six import iteritems

from . import RefinerUtils
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediException
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jedicore.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer import EventServiceUtils

try:
    from idds.client.client import Client as iDDS_Client
    import idds.common.constants
    import idds.common.utils
except ImportError:
    pass


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
        # remove irrelevant
        if 'maxAttempt' in taskParamMap and not taskParamMap['maxAttempt']:
            del taskParamMap['maxAttempt']
        # make task spec
        taskSpec = JediTaskSpec()
        taskSpec.jediTaskID = jediTaskID
        taskSpec.attemptNr = 0
        taskSpec.taskName = taskParamMap['taskName']
        taskSpec.userName = taskParamMap['userName']
        taskSpec.vo = taskParamMap['vo']
        taskSpec.prodSourceLabel = taskParamMap['prodSourceLabel']
        taskSpec.taskPriority = taskParamMap['taskPriority']
        if taskSpec.taskPriority is None:
            taskSpec.taskPriority = 0
        if 'currentPriority' in taskParamMap:
            taskSpec.currentPriority = taskParamMap['currentPriority']
        else:
            taskSpec.currentPriority = taskSpec.taskPriority
        taskSpec.architecture = taskParamMap['architecture']
        taskSpec.transUses = taskParamMap['transUses']
        taskSpec.transHome = taskParamMap['transHome']
        if 'transPath' in taskParamMap:
            taskSpec.transPath = taskParamMap['transPath']
        taskSpec.processingType = taskParamMap['processingType']
        taskSpec.taskType = taskParamMap['taskType']
        taskSpec.splitRule = splitRule
        taskSpec.startTime = datetime.datetime.utcnow()
        if 'workingGroup' in taskParamMap:
            taskSpec.workingGroup = taskParamMap['workingGroup']
        if 'countryGroup' in taskParamMap:
            taskSpec.countryGroup = taskParamMap['countryGroup']
        if 'ticketID' in taskParamMap:
            taskSpec.ticketID = taskParamMap['ticketID']
        if 'ticketSystemType' in taskParamMap:
            taskSpec.ticketSystemType = taskParamMap['ticketSystemType']
        if 'reqID' in taskParamMap:
            taskSpec.reqID = taskParamMap['reqID']
        else:
            taskSpec.reqID = jediTaskID
        if 'coreCount' in taskParamMap:
            taskSpec.coreCount = taskParamMap['coreCount']
        else:
            taskSpec.coreCount = 1
        if 'walltime' in taskParamMap:
            taskSpec.walltime = taskParamMap['walltime']
        else:
            taskSpec.walltime = 0
        if 'walltimeUnit' not in taskParamMap:
            # force to set NULL so that retried tasks get data from scouts again
            taskSpec.forceUpdate('walltimeUnit')
        if 'outDiskCount' in taskParamMap:
            taskSpec.outDiskCount = taskParamMap['outDiskCount']
        else:
            taskSpec.outDiskCount = 0
        if 'outDiskUnit' in taskParamMap:
            taskSpec.outDiskUnit = taskParamMap['outDiskUnit']
        if 'workDiskCount' in taskParamMap:
            taskSpec.workDiskCount = taskParamMap['workDiskCount']
        else:
            taskSpec.workDiskCount = 0
        if 'workDiskUnit' in taskParamMap:
            taskSpec.workDiskUnit = taskParamMap['workDiskUnit']
        if 'ramCount' in taskParamMap:
            taskSpec.ramCount = taskParamMap['ramCount']
        else:
            taskSpec.ramCount = 0
        if 'ramUnit' in taskParamMap:
            taskSpec.ramUnit = taskParamMap['ramUnit']
        elif 'ramCountUnit' in taskParamMap:
            taskSpec.ramUnit = taskParamMap['ramCountUnit']
        if 'baseRamCount' in taskParamMap:
            taskSpec.baseRamCount = taskParamMap['baseRamCount']
        else:
            taskSpec.baseRamCount = 0
        # IO
        if 'ioIntensity' in taskParamMap:
            taskSpec.ioIntensity = taskParamMap['ioIntensity']
        if 'ioIntensityUnit' in taskParamMap:
            taskSpec.ioIntensityUnit = taskParamMap['ioIntensityUnit']
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
        if 'skipScout' not in taskParamMap and not taskSpec.isPostScout():
            taskSpec.setUseScout(True)
        # cloud
        if 'cloud' in taskParamMap:
            self.cloudName = taskParamMap['cloud']
            taskSpec.cloud = self.cloudName
        else:
            # set dummy to force update
            taskSpec.cloud = 'dummy'
            taskSpec.cloud = None
        # site
        if 'site' in taskParamMap:
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
            if 'nEventsPerWorker' not in taskParamMap:
                taskParamMap['nEventsPerWorker'] = 1
            if 'nSitesPerJob' not in taskParamMap:
                taskParamMap['nSitesPerJob'] = 2
            if 'nEsConsumers' not in taskParamMap:
                taskParamMap['nEsConsumers'] = taskParamMap['nSitesPerJob']
        # minimum granularity
        if 'minGranularity' in taskParamMap:
            taskParamMap['nEventsPerRange'] = taskParamMap['minGranularity']
        # event service flag
        if 'useJobCloning' in taskParamMap:
            taskSpec.eventService = 2
        elif 'nEventsPerWorker' in taskParamMap:
            taskSpec.eventService = 1
        else:
            taskSpec.eventService = 0
        # OS
        if 'osInfo' in taskParamMap:
            taskSpec.termCondition = taskParamMap['osInfo']
        # ttcr: requested time to completion
        if 'ttcrTimestamp' in taskParamMap:
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
            except Exception:
                pass
        # campaign
        if 'campaign' in taskParamMap:
            taskSpec.campaign = taskParamMap['campaign']
        # request type
        if 'requestType' in taskParamMap:
            taskSpec.requestType = taskParamMap['requestType']
        # image name
        if 'container_name' in taskParamMap:
            taskSpec.container_name = taskParamMap['container_name']
        self.taskSpec = taskSpec
        # set split rule
        if 'tgtNumEventsPerJob' in taskParamMap:
            # set nEventsPerJob not respect file boundaries when nFilesPerJob is not used
            if 'nFilesPerJob' not in taskParamMap:
                self.setSplitRule(None,taskParamMap['tgtNumEventsPerJob'],JediTaskSpec.splitRuleToken['nEventsPerJob'])
        self.setSplitRule(taskParamMap,'nFilesPerJob',     JediTaskSpec.splitRuleToken['nFilesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerJob',    JediTaskSpec.splitRuleToken['nEventsPerJob'])
        self.setSplitRule(taskParamMap,'nGBPerJob',        JediTaskSpec.splitRuleToken['nGBPerJob'])
        self.setSplitRule(taskParamMap,'nMaxFilesPerJob',  JediTaskSpec.splitRuleToken['nMaxFilesPerJob'])
        self.setSplitRule(taskParamMap,'nEventsPerWorker', JediTaskSpec.splitRuleToken['nEventsPerWorker'])
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
        self.setSplitRule(taskParamMap,'maxWalltime', JediTaskSpec.splitRuleToken['maxWalltime'])
        self.setSplitRule(taskParamMap,'tgtMaxOutputForNG', JediTaskSpec.splitRuleToken['tgtMaxOutputForNG'])
        self.setSplitRule(taskParamMap, 'maxNumJobs', JediTaskSpec.splitRuleToken['maxNumJobs'])
        self.setSplitRule(taskParamMap, 'totNumJobs', JediTaskSpec.splitRuleToken['totNumJobs'])
        self.setSplitRule(taskParamMap, 'nChunksToWait', JediTaskSpec.splitRuleToken['nChunksToWait'])
        if 'forceStaged' in taskParamMap:
            taskParamMap['useLocalIO'] = taskParamMap['forceStaged']
        if 'useLocalIO' in taskParamMap:
            if taskParamMap['useLocalIO']:
                self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['useLocalIO'])
            else:
                self.setSplitRule(None, 0, JediTaskSpec.splitRuleToken['useLocalIO'])
        if 'nJumboJobs' in taskParamMap:
            self.setSplitRule(taskParamMap,'nJumboJobs',JediTaskSpec.splitRuleToken['nJumboJobs'])
            taskSpec.useJumbo = JediTaskSpec.enum_useJumbo['waiting']
            if 'maxJumboPerSite' in taskParamMap:
                self.setSplitRule(taskParamMap,'maxJumboPerSite',JediTaskSpec.splitRuleToken['maxJumboPerSite'])
        if 'minCpuEfficiency' in taskParamMap:
            self.setSplitRule(taskParamMap,'minCpuEfficiency',JediTaskSpec.splitRuleToken['minCpuEfficiency'])
        if 'loadXML' in taskParamMap:
            self.setSplitRule(None,3,JediTaskSpec.splitRuleToken['loadXML'])
            self.setSplitRule(None,4,JediTaskSpec.splitRuleToken['groupBoundaryID'])
        if 'pfnList' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['pfnList'])
        if 'noWaitParent' in taskParamMap and taskParamMap['noWaitParent'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['noWaitParent'])
        if 'respectLB' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['respectLB'])
        if 'releasePerLB' in taskParamMap:
                self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['releasePerLB'])
        if 'orderByLB' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['orderByLB'])
        if 'respectSplitRule' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['respectSplitRule'])
        if 'reuseSecOnDemand' in taskParamMap:
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
        if 'failWhenGoalUnreached' in taskParamMap and taskParamMap['failWhenGoalUnreached'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['failGoalUnreached'])
        if 'switchEStoNormal' in taskParamMap:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['switchEStoNormal'])
        if 'nEventsPerRange' in taskParamMap:
            self.setSplitRule(taskParamMap,'nEventsPerRange',JediTaskSpec.splitRuleToken['dynamicNumEvents'])
        if 'allowInputWAN' in taskParamMap and taskParamMap['allowInputWAN'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['allowInputWAN'])
        if 'putLogToOS' in taskParamMap and taskParamMap['putLogToOS'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['putLogToOS'])
        if 'mergeEsOnOS' in taskParamMap and taskParamMap['mergeEsOnOS'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['mergeEsOnOS'])
        if 'writeInputToFile' in taskParamMap and taskParamMap['writeInputToFile'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['writeInputToFile'])
        if 'useFileAsSourceLFN' in taskParamMap and taskParamMap['useFileAsSourceLFN'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['useFileAsSourceLFN'])
        if 'ignoreMissingInDS' in taskParamMap and taskParamMap['ignoreMissingInDS'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['ignoreMissingInDS'])
        if 'noExecStrCnv' in taskParamMap and taskParamMap['noExecStrCnv'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['noExecStrCnv'])
        if 'inFilePosEvtNum' in taskParamMap and taskParamMap['inFilePosEvtNum'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['inFilePosEvtNum'])
        if self.taskSpec.useEventService() and not taskSpec.useJobCloning():
            if 'registerEsFiles' in taskParamMap and taskParamMap['registerEsFiles'] is True:
                self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['registerEsFiles'])
        if 'disableAutoFinish' in taskParamMap and taskParamMap['disableAutoFinish'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['disableAutoFinish'])
        if 'resurrectConsumers' in taskParamMap and taskParamMap['resurrectConsumers'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['resurrectConsumers'])
        if 'usePrefetcher' in taskParamMap and taskParamMap['usePrefetcher'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['usePrefetcher'])
        if 'notDiscardEvents' in taskParamMap and taskParamMap['notDiscardEvents'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['notDiscardEvents'])
        if 'decAttOnFailedES' in taskParamMap and taskParamMap['decAttOnFailedES'] is True:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['decAttOnFailedES'])
        if 'useZipToPin' in taskParamMap and taskParamMap['useZipToPin'] is True:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['useZipToPin'])
        if 'osMatching' in taskParamMap and taskParamMap['osMatching'] is True:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['osMatching'])
        if 'multiStepExec' in taskParamMap:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['multiStepExec'])
        if 'onlyTagsForFC' in taskParamMap:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['onlyTagsForFC'])
        if 'segmentedWork' in taskParamMap and 'segmentSpecs' in taskParamMap:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['segmentedWork'])
        if 'avoidVP' in taskParamMap:
                self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['avoidVP'])
        if 'inputPreStaging' in taskParamMap and taskParamMap['inputPreStaging'] is True:
            self.setSplitRule(None, JediTaskSpec.enum_inputPreStaging['use'],
                              JediTaskSpec.splitRuleToken['inputPreStaging'])
        if 'hpoWorkflow' in taskParamMap and taskParamMap['hpoWorkflow'] is True and 'hpoRequestData' in taskParamMap:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['hpoWorkflow'])
        if 'noLoopingCheck' in taskParamMap and taskParamMap['noLoopingCheck']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['noLoopingCheck'])
        if 'encJobParams' in taskParamMap and taskParamMap['encJobParams']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['encJobParams'])
        if 'useSecrets' in taskParamMap and taskParamMap['useSecrets']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['useSecrets'])
        if 'debugMode' in taskParamMap and taskParamMap['debugMode']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['debugMode'])
        if 'pushStatusChanges' in taskParamMap and taskParamMap['pushStatusChanges']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['pushStatusChanges'])
        if 'maxCoreCount' in taskParamMap:
            self.setSplitRule(taskParamMap, 'maxCoreCount', JediTaskSpec.splitRuleToken['maxCoreCount'])
        if 'cloudAsVO' in taskParamMap:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['cloudAsVO'])
        if 'pushJob' in taskParamMap and taskParamMap['pushJob']:
            self.setSplitRule(None, 1, JediTaskSpec.splitRuleToken['pushJob'])
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
            raise RuntimeError(errStr)
        self.taskSpec.workQueue_ID = workQueue.queue_id

        # Initialize the global share
        gshare = 'Undefined'
        if 'gshare' in taskParamMap and self.taskBufferIF.is_valid_share(taskParamMap['gshare']):
            # work queue is specified
            gshare = taskParamMap['gshare']
        else:
            # get share based on definition
            gshare = self.taskBufferIF.get_share_for_task(self.taskSpec)
            if gshare is None:
                gshare = 'Undefined'  # Should not happen. Undefined is set when no share is found
                # errStr  = 'share is undefined for vo={0} label={1} '.format(taskSpec.vo,taskSpec.prodSourceLabel)
                # errStr += 'workingGroup={0} campaign={1} '.format(taskSpec.workingGroup, taskSpec.campaign)
                # raise RuntimeError,errStr
        self.taskSpec.gshare = gshare

        # Initialize the resource type
        try:
            self.taskSpec.resource_type = self.taskBufferIF.get_resource_type_task(self.taskSpec)
        except Exception:
            self.taskSpec.resource_type = 'Undefined'

        # return
        return



    # basic refinement procedure
    def doBasicRefine(self,taskParamMap):
        # get input/output/log dataset specs
        nIn  = 0
        nOutMap = {}
        if 'log' not in taskParamMap:
            itemList = taskParamMap['jobParameters']
        elif isinstance(taskParamMap['log'],dict):
            itemList = taskParamMap['jobParameters'] + [taskParamMap['log']]
        else:
            itemList = taskParamMap['jobParameters'] + taskParamMap['log']
        if 'log_merge' in taskParamMap:
            itemList += [taskParamMap['log_merge']]
        # pseudo input
        if 'noInput' in taskParamMap and taskParamMap['noInput'] is True:
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
            if tmpItem['type'] == 'template' and 'dataset' in tmpItem:
                # avoid duplication
                if tmpItem['dataset'] not in allDsList:
                    allDsList.append(tmpItem['dataset'])
                else:
                    continue
                datasetSpec = JediDatasetSpec()
                datasetSpec.datasetName = tmpItem['dataset']
                datasetSpec.jediTaskID = self.taskSpec.jediTaskID
                datasetSpec.type = tmpItem['param_type']
                if 'container' in tmpItem:
                    datasetSpec.containerName = tmpItem['container']
                if 'token' in tmpItem:
                    datasetSpec.storageToken = tmpItem['token']
                if 'destination' in tmpItem:
                    datasetSpec.destination = tmpItem['destination']
                if 'attributes' in tmpItem:
                    datasetSpec.setDatasetAttribute(tmpItem['attributes'])
                if 'ratio' in tmpItem:
                    datasetSpec.setDatasetAttribute('ratio={0}'.format(tmpItem['ratio']))
                if 'eventRatio' in tmpItem:
                    datasetSpec.setEventRatio(tmpItem['eventRatio'])
                if 'check' in tmpItem:
                    datasetSpec.setDatasetAttribute('cc')
                if 'usedup' in tmpItem:
                    datasetSpec.setDatasetAttribute('ud')
                if 'random' in tmpItem:
                    datasetSpec.setDatasetAttribute('rd')
                if 'reusable' in tmpItem:
                    datasetSpec.setDatasetAttribute('ru')
                if 'indexConsistent' in tmpItem:
                    datasetSpec.setDatasetAttributeWithLabel('indexConsistent')
                if 'mergeOnly' in tmpItem:
                    datasetSpec.setDatasetAttributeWithLabel('mergeOnly')
                if 'offset' in tmpItem:
                    datasetSpec.setOffset(tmpItem['offset'])
                if 'allowNoOutput' in tmpItem:
                    datasetSpec.allowNoOutput()
                if 'nFilesPerJob' in tmpItem:
                    datasetSpec.setNumFilesPerJob(tmpItem['nFilesPerJob'])
                if 'num_records' in tmpItem:
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
                    if 'expandedList' not in tmpItem:
                        tmpItem['expandedList'] = []
                    # dataset names could be comma-concatenated
                    datasetNameList = datasetSpec.datasetName.split(',')
                    # datasets could be added by incexec
                    incexecDS = 'dsFor{0}'.format(datasetSpec.streamName)
                    # remove /XYZ
                    incexecDS = incexecDS.split('/')[0]
                    if incexecDS in taskParamMap:
                        for tmpDatasetName in taskParamMap[incexecDS].split(','):
                            if tmpDatasetName not in datasetNameList:
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
                            if self.taskSpec.is_work_segmented():
                                tmpDatasetNameList *= len(taskParamMap['segmentSpecs'])
                        else:
                            tmpIF = self.ddmIF.getInterface(self.taskSpec.vo, self.taskSpec.cloud)
                            if not tmpIF:
                                tmpDatasetNameList = []
                            else:
                                if 'expand' in tmpItem and tmpItem['expand'] is True:
                                    # expand dataset container
                                    tmpDatasetNameList = tmpIF.expandContainer(datasetName)
                                else:
                                    # normal dataset name
                                    tmpDatasetNameList = tmpIF.listDatasets(datasetName)
                        i_element = 0
                        for elementDatasetName in tmpDatasetNameList:
                            if nIn > 0 or elementDatasetName not in tmpItem['expandedList'] or \
                                    self.taskSpec.is_work_segmented():
                                tmpItem['expandedList'].append(elementDatasetName)
                                inDatasetSpec = copy.copy(datasetSpec)
                                inDatasetSpec.datasetName = elementDatasetName
                                if nIn > 0 or not self.taskSpec.is_hpo_workflow():
                                    inDatasetSpec.containerName = datasetName
                                else:
                                    if self.taskSpec.is_work_segmented():
                                        inDatasetSpec.containerName = "{}/{}".format(
                                            taskParamMap['segmentSpecs'][i_element]['name'],
                                            taskParamMap['segmentSpecs'][i_element]['id']
                                        )
                                    else:
                                        inDatasetSpec.containerName = "None/None"
                                inDatasetSpecList.append(inDatasetSpec)
                            i_element += 1
                    # empty input
                    if inDatasetSpecList == [] and self.oldTaskStatus != 'rerefine':
                        errStr = 'doBasicRefine : unknown input dataset "{0}"'.format(datasetSpec.datasetName)
                        self.taskSpec.setErrDiag(errStr)
                        if datasetSpec.datasetName not in self.unknownDatasetList:
                            self.unknownDatasetList.append(datasetSpec.datasetName)
                        raise JediException.UnknownDatasetError(errStr)
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
                    if datasetSpec.type not in nOutMap:
                        nOutMap[datasetSpec.type] = 0
                    # make stream name
                    if not datasetSpec.is_merge_only():
                        datasetSpec.streamName = "{0}{1}".format(datasetSpec.type.upper(),nOutMap[datasetSpec.type])
                    else:
                        datasetSpec.streamName = 'LOG_MERGE'
                    nOutMap[datasetSpec.type] += 1
                    # set attribute for event service
                    if self.taskSpec.useEventService() and 'objectStore' in taskParamMap and datasetSpec.type in ['output']:
                        datasetSpec.setObjectStore(taskParamMap['objectStore'])
                    # extract output filename template and change the value field
                    outFileTemplate,tmpItem['value'] = RefinerUtils.extractReplaceOutFileTemplate(tmpItem['value'],
                                                                                                  datasetSpec.streamName)
                    # make output template
                    if outFileTemplate is not None:
                        if 'offset' in tmpItem:
                            offsetVal = 1 + tmpItem['offset']
                        else:
                            offsetVal = 1
                        outTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                          'serialNr' : offsetVal,
                                          'streamName' : datasetSpec.streamName,
                                          'filenameTemplate' : outFileTemplate,
                                          'outtype' : datasetSpec.type,
                                          }
                        if datasetSpec.outputMapKey() in self.outputTemplateMap:
                            # multiple files are associated to the same output datasets
                            self.outputTemplateMap[datasetSpec.outputMapKey()].append(outTemplateMap)
                            # don't insert the same output dataset
                            continue
                        self.outputTemplateMap[datasetSpec.outputMapKey()] = [outTemplateMap]
                    # append
                    self.outDatasetSpecList.append(datasetSpec)
                    # used only in merge
                    if datasetSpec.is_merge_only():
                        continue
                    # make unmerged dataset
                    if 'mergeOutput' in taskParamMap and taskParamMap['mergeOutput'] is True:
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
                        if outFileTemplate is not None:
                            umOutTemplateMap = {'jediTaskID' : self.taskSpec.jediTaskID,
                                                'serialNr' : 1,
                                                'streamName' : umDatasetSpec.streamName,
                                                'outtype' : datasetSpec.type,
                                                }
                            # append temporary name
                            if 'umNameAtEnd' in taskParamMap and taskParamMap['umNameAtEnd'] is True:
                                # append temporary name at the end
                                umOutTemplateMap['filenameTemplate'] = outFileTemplate + '.panda.um'
                            else:
                                umOutTemplateMap['filenameTemplate'] = 'panda.um.' + outFileTemplate
                            if umDatasetSpec.outputMapKey() in self.outputTemplateMap:
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
        if 'mergeOutput' in taskParamMap and taskParamMap['mergeOutput'] is True:
            self.setSplitRule(None,1,JediTaskSpec.splitRuleToken['mergeOutput'])
        # make job parameters
        rndmSeedOffset = None
        firstEventOffset = None
        jobParameters = ''
        for tmpItem in taskParamMap['jobParameters']:
            if 'value' in tmpItem:
                # hidden parameter
                if 'hidden' in tmpItem and tmpItem['hidden'] is True:
                    continue
                # add tags for ES-only parameters
                esOnly = False
                if 'es_only' in tmpItem and tmpItem['es_only'] is True:
                    esOnly = True
                if esOnly:
                    jobParameters += '<PANDA_ES_ONLY>'
                jobParameters += '{0}'.format(tmpItem['value'])
                if esOnly:
                    jobParameters += '</PANDA_ES_ONLY>'
                # padding
                if 'padding' in tmpItem and tmpItem['padding'] is False:
                    pass
                else:
                    jobParameters += ' '
                # get offset for random seed and first event
                if tmpItem['type'] == 'template' and tmpItem['param_type'] == 'number':
                    if '${RNDMSEED}' in tmpItem['value']:
                        if 'offset' in tmpItem:
                            rndmSeedOffset = tmpItem['offset']
                        else:
                            rndmSeedOffset = 0
                    elif '${FIRSTEVENT}' in tmpItem['value']:
                        if 'offset' in tmpItem:
                            firstEventOffset = tmpItem['offset']
        jobParameters = jobParameters[:-1]
        # append parameters for event service merging if necessary
        esmergeParams = self.getParamsForEventServiceMerging(taskParamMap)
        if esmergeParams is not None:
            jobParameters += esmergeParams
        self.setJobParamsTemplate(jobParameters)
        # set random seed offset
        if rndmSeedOffset is not None:
            self.setSplitRule(None,rndmSeedOffset,JediTaskSpec.splitRuleToken['randomSeed'])
        if firstEventOffset is not None:
            self.setSplitRule(None,firstEventOffset,JediTaskSpec.splitRuleToken['firstEvent'])
        # send HPO request
        if self.taskSpec.is_hpo_workflow():
            try:
                data = copy.copy(taskParamMap['hpoRequestData'])
                data['workload_id'] = self.taskSpec.jediTaskID
                data['is_pseudo_input'] = True
                req = {
                    'requester': 'panda',
                    'request_type': idds.common.constants.RequestType.HyperParameterOpt,
                    'transform_tag': idds.common.constants.RequestType.HyperParameterOpt.value,
                    'status': idds.common.constants.RequestStatus.New,
                    'priority': 0,
                    'lifetime': 30,
                    'request_metadata': data,
                }
                c = iDDS_Client(idds.common.utils.get_rest_host())
                self.tmpLog.debug('req {0}'.format(str(req)))
                ret = c.add_request(**req)
                self.tmpLog.debug('got requestID={0}'.format(str(ret)))
            except Exception as e:
                errStr = 'iDDS failed with {0}'.format(str(e))
                raise JediException.ExternalTempError(errStr)
        # return
        return



    # replace placeholder with dict provided by prepro job
    def replacePlaceHolders(self,paramItem,placeHolderName,newValue):
        if isinstance(paramItem, dict):
            # loop over all dict params
            for tmpParName,tmpParVal in iteritems(paramItem):
                if tmpParVal == placeHolderName:
                    # replace placeholder
                    paramItem[tmpParName] = newValue
                elif isinstance(tmpParVal, dict) or \
                        isinstance(tmpParVal, list):
                    # recursive execution
                    self.replacePlaceHolders(tmpParVal,placeHolderName,newValue)
        elif isinstance(paramItem, list):
            # loop over all list items
            for tmpItem in paramItem:
                self.replacePlaceHolders(tmpItem,placeHolderName,newValue)



    # refinement procedure for preprocessing
    def doPreProRefine(self,taskParamMap):
        # no preprocessing
        if 'preproSpec' not in taskParamMap:
            return None,taskParamMap
        # already preprocessed
        if self.taskSpec.checkPreProcessed():
            # get replaced task params
            tmpStat,tmpJsonStr = self.taskBufferIF.getPreprocessMetadata_JEDI(self.taskSpec.jediTaskID)
            try:
                # replace placeholders
                replaceParams = RefinerUtils.decodeJSON(tmpJsonStr)
                self.tmpLog.debug("replace placeholders with "+str(replaceParams))
                for tmpKey,tmpVal in iteritems(replaceParams):
                    self.replacePlaceHolders(taskParamMap,tmpKey,tmpVal)
            except Exception:
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
        if taskParamMap is not None:
            if keyName not in taskParamMap:
                return
            tmpStr = '{0}={1}'.format(valName,taskParamMap[keyName])
        else:
            tmpStr = '{0}={1}'.format(valName,keyName)
        if self.taskSpec.splitRule in [None,'']:
            self.taskSpec.splitRule = tmpStr
        else:
            tmpMatch = re.search(valName+'=(-*\d+)(,-*\d+)*',self.taskSpec.splitRule)
            if tmpMatch is None:
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
        if 'esmergeSpec' in taskParamMap:
            if 'transPath' in taskParamMap['esmergeSpec']:
                transPath = taskParamMap['esmergeSpec']['transPath']
            if 'jobParameters' in taskParamMap['esmergeSpec']:
                jobParameters = taskParamMap['esmergeSpec']['jobParameters']
        # return
        return '<PANDA_ESMERGE_TRF>'+transPath+'</PANDA_ESMERGE_TRF>'+'<PANDA_ESMERGE_JOBP>'+jobParameters+'</PANDA_ESMERGE_JOBP>'



Interaction.installSC(TaskRefinerBase)
