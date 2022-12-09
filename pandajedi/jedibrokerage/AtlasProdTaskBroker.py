import sys
import random
import copy
import math
import traceback

from six import iteritems

try:
    long()
except Exception:
    long = int

from .AtlasProdJobBroker import AtlasProdJobBroker
from .TaskBrokerBase import TaskBrokerBase
from . import AtlasBrokerUtils

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread, MapWithLock
from pandajedi.jedirefine import RefinerUtils

from pandaserver.userinterface import Client as PandaClient
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer import JobUtils

# cannot use pandaserver.taskbuffer while Client is used
from taskbuffer.JobSpec import JobSpec

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for ATLAS production
class AtlasProdTaskBroker (TaskBrokerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskBrokerBase.__init__(self,taskBufferIF,ddmIF)


    # main to check
    def doCheck(self,taskSpecList):
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start doCheck')
        # return for failure
        retFatal    = self.SC_FATAL,{}
        retTmpError = self.SC_FAILED,{}
        # get list of jediTaskIDs
        taskIdList = []
        taskSpecMap = {}
        for taskSpec in taskSpecList:
            taskIdList.append(taskSpec.jediTaskID)
            taskSpecMap[taskSpec.jediTaskID] = taskSpec
        # check with panda
        tmpLog.debug('check with panda')
        tmpPandaStatus,cloudsInPanda = PandaClient.seeCloudTask(taskIdList)
        if tmpPandaStatus != 0:
            tmpLog.error('failed to see clouds')
            return retTmpError
        # make return map
        retMap = {}
        for tmpTaskID,tmpCoreName in iteritems(cloudsInPanda):
            tmpLog.debug('jediTaskID={0} -> {1}'.format(tmpTaskID,tmpCoreName))
            if tmpCoreName not in ['NULL','',None]:
                taskSpec = taskSpecMap[tmpTaskID]
                if taskSpec.useWorldCloud():
                    # get destinations for WORLD cloud
                    ddmIF = self.ddmIF.getInterface(taskSpec.vo)
                    # get site
                    siteSpec = self.siteMapper.getSite(tmpCoreName)
                    scopeSiteSpec_input, scopeSiteSpec_output = select_scope(siteSpec, taskSpec.prodSourceLabel,
                                                                             JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType))
                    # get nucleus
                    nucleus = siteSpec.pandasite
                    # get output/log datasets
                    tmpStat,tmpDatasetSpecs = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(tmpTaskID,['output','log'])
                    # get destinations
                    retMap[tmpTaskID] = {'datasets': [], 'nucleus': nucleus}
                    for datasetSpec in tmpDatasetSpecs:
                        # skip distributed datasets
                        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
                            continue
                        # get token
                        token = ddmIF.convertTokenToEndpoint(siteSpec.ddm_output[scopeSiteSpec_output], datasetSpec.storageToken)
                        # use default endpoint
                        if token is None:
                            token = siteSpec.ddm_output[scopeSiteSpec_output]
                        # add original token
                        if datasetSpec.storageToken not in ['',None]:
                            token += '/{0}'.format(datasetSpec.storageToken)
                        retMap[tmpTaskID]['datasets'].append({'datasetID':datasetSpec.datasetID,
                                                              'token':'dst:{0}'.format(token),
                                                              'destination':tmpCoreName})
                else:
                    retMap[tmpTaskID] = tmpCoreName
        tmpLog.debug('ret {0}'.format(str(retMap)))
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,retMap



    # main to assign
    def doBrokerage(self, inputList, vo, prodSourceLabel, workQueue, resource_name):
        # list with a lock
        inputListWorld = ListWithLock([])
        # variables for submission
        maxBunchTask = 100
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug('start doBrokerage')
        # return for failure
        retFatal    = self.SC_FATAL
        retTmpError = self.SC_FAILED
        tmpLog.debug('vo={0} label={1} queue={2} resource_name={3} nTasks={4}'.format(vo,prodSourceLabel,
                                                                    workQueue.queue_name, resource_name,
                                                                    len(inputList)))
        # loop over all tasks
        allRwMap    = {}
        prioMap     = {}
        tt2Map      = {}
        expRWs      = {}
        jobSpecList = []
        for tmpJediTaskID,tmpInputList in inputList:
            for taskSpec,cloudName,inputChunk in tmpInputList:
                # collect tasks for WORLD
                if taskSpec.useWorldCloud():
                    inputListWorld.append((taskSpec,inputChunk))
                    continue
                # make JobSpec to be submitted for TaskAssigner
                jobSpec = JobSpec()
                jobSpec.taskID     = taskSpec.jediTaskID
                jobSpec.jediTaskID = taskSpec.jediTaskID
                # set managed to trigger TA
                jobSpec.prodSourceLabel  = 'managed'
                jobSpec.processingType   = taskSpec.processingType
                jobSpec.workingGroup     = taskSpec.workingGroup
                jobSpec.metadata         = taskSpec.processingType
                jobSpec.assignedPriority = taskSpec.taskPriority
                jobSpec.currentPriority  = taskSpec.currentPriority
                jobSpec.maxDiskCount     = (taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()) // 1024 // 1024
                if taskSpec.useWorldCloud():
                    # use destinationSE to trigger task brokerage in WORLD cloud
                    jobSpec.destinationSE = taskSpec.cloud
                prodDBlock = None
                setProdDBlock = False
                for datasetSpec in inputChunk.getDatasets():
                    prodDBlock = datasetSpec.datasetName
                    if datasetSpec.isMaster():
                        jobSpec.prodDBlock = datasetSpec.datasetName
                        setProdDBlock = True
                    for fileSpec in datasetSpec.Files:
                        tmpInFileSpec = fileSpec.convertToJobFileSpec(datasetSpec)
                        jobSpec.addFile(tmpInFileSpec)
                # use secondary dataset name as prodDBlock
                if setProdDBlock is False and prodDBlock is not None:
                    jobSpec.prodDBlock = prodDBlock
                # append
                jobSpecList.append(jobSpec)
                prioMap[jobSpec.taskID] = jobSpec.currentPriority
                tt2Map[jobSpec.taskID]  = jobSpec.processingType
                # get RW for a priority
                if jobSpec.currentPriority not in allRwMap:
                    tmpRW = self.taskBufferIF.calculateRWwithPrio_JEDI(vo,prodSourceLabel,workQueue,
                                                                       jobSpec.currentPriority)
                    if tmpRW is None:
                        tmpLog.error('failed to calculate RW with prio={0}'.format(jobSpec.currentPriority))
                        return retTmpError
                    allRwMap[jobSpec.currentPriority] = tmpRW
                # get expected RW
                expRW = self.taskBufferIF.calculateTaskRW_JEDI(jobSpec.jediTaskID)
                if expRW is None:
                    tmpLog.error('failed to calculate RW for jediTaskID={0}'.format(jobSpec.jediTaskID))
                    return retTmpError
                expRWs[jobSpec.taskID] = expRW
        # for old clouds
        if jobSpecList != []:
            # get fullRWs
            fullRWs = self.taskBufferIF.calculateRWwithPrio_JEDI(vo,prodSourceLabel,None,None)
            if fullRWs is None:
                tmpLog.error('failed to calculate full RW')
                return retTmpError
            # set metadata
            for jobSpec in jobSpecList:
                rwValues = allRwMap[jobSpec.currentPriority]
                jobSpec.metadata = "%s;%s;%s;%s;%s;%s" % (jobSpec.metadata,
                                                          str(rwValues),str(expRWs),
                                                          str(prioMap),str(fullRWs),
                                                          str(tt2Map))
            tmpLog.debug('run task assigner for {0} tasks'.format(len(jobSpecList)))
            nBunchTask = 0
            while nBunchTask < len(jobSpecList):
                # get a bunch
                jobsBunch = jobSpecList[nBunchTask:nBunchTask+maxBunchTask]
                strIDs = 'jediTaskID='
                for tmpJobSpec in jobsBunch:
                    strIDs += '{0},'.format(tmpJobSpec.taskID)
                strIDs = strIDs[:-1]
                tmpLog.debug(strIDs)
                # increment index
                nBunchTask += maxBunchTask
                # run task brokerge
                stS,outSs = PandaClient.runTaskAssignment(jobsBunch)
                tmpLog.debug('{0}:{1}'.format(stS,str(outSs)))
        # for WORLD
        if len(inputListWorld) > 0:
            # thread pool
            threadPool = ThreadPool()
            # get full RW for WORLD
            fullRWs = self.taskBufferIF.calculateWorldRWwithPrio_JEDI(vo,prodSourceLabel,None,None)
            if fullRWs is None:
                tmpLog.error('failed to calculate full WORLD RW')
                return retTmpError
            # get RW per priority
            for taskSpec,inputChunk in inputListWorld:
                if taskSpec.currentPriority not in allRwMap:
                    tmpRW = self.taskBufferIF.calculateWorldRWwithPrio_JEDI(vo,prodSourceLabel,workQueue,
                                                                            taskSpec.currentPriority)
                    if tmpRW is None:
                        tmpLog.error('failed to calculate RW with prio={0}'.format(taskSpec.currentPriority))
                        return retTmpError
                    allRwMap[taskSpec.currentPriority] = tmpRW
            # live counter for RWs
            liveCounter = MapWithLock(allRwMap)
            # make workers
            ddmIF = self.ddmIF.getInterface(vo)
            for iWorker in range(4):
                thr = AtlasProdTaskBrokerThread(inputListWorld,threadPool,
                                                self.taskBufferIF,ddmIF,
                                                fullRWs,liveCounter,
                                                workQueue)
                thr.start()
            threadPool.join(60*10)
        # return
        tmpLog.debug('doBrokerage done')
        return self.SC_SUCCEEDED


# thread for real worker
class AtlasProdTaskBrokerThread (WorkerThread):

    # constructor
    def __init__(self,inputList,threadPool,taskbufferIF,ddmIF,
                 fullRW,prioRW,workQueue):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.inputList    = inputList
        self.taskBufferIF = taskbufferIF
        self.ddmIF        = ddmIF
        self.msgType      = 'taskbrokerage'
        self.fullRW       = fullRW
        self.prioRW       = prioRW
        self.numTasks     = 0
        self.workQueue    = workQueue
        self.summaryList = None

    # init summary list
    def init_summary_list(self, header, comment, initial_list):
        self.summaryList = []
        self.summaryList.append('===== {} ====='.format(header))
        if comment:
            self.summaryList.append(comment)
        self.summaryList.append('the number of initial candidates: {}'.format(len(initial_list)))

    # dump summary
    def dump_summary(self, tmp_log, final_candidates=None):
        if not self.summaryList:
            return
        tmp_log.info('')
        for m in self.summaryList:
            tmp_log.info(m)
        if not final_candidates:
            final_candidates = []
        tmp_log.info('the number of final candidates: {}'.format(len(final_candidates)))
        tmp_log.info('')

    # make summary
    def add_summary_message(self, old_list, new_list, message):
        if old_list and len(old_list) != len(new_list):
            red = int(math.ceil(((len(old_list) - len(new_list)) * 100) / len(old_list)))
            self.summaryList.append('{:>5} -> {:>3} candidates, {:>3}% cut : {}'.format(len(old_list),
                                                                                        len(new_list),
                                                                                        red, message))

    # post-process for errors
    def post_process_for_error(self, task_spec, tmp_log, msg, dump_summary=True):
        if dump_summary:
            self.dump_summary(tmp_log)
        tmp_log.error(msg)
        task_spec.resetChangedList()
        task_spec.setErrDiag(tmp_log.uploadLog(task_spec.jediTaskID))
        self.taskBufferIF.updateTask_JEDI(task_spec, {'jediTaskID': task_spec.jediTaskID},
                                          oldStatus=['assigning'], updateDEFT=False,
                                          setFrozenTime=False)


    # main function
    def runImpl(self):
        # cutoff for disk in TB
        diskThreshold = self.taskBufferIF.getConfigValue(self.msgType, 'DISK_THRESHOLD_{0}'.format(self.workQueue.queue_name),
                                                         'jedi', 'atlas')
        if diskThreshold is None:
            diskThreshold = 100 * 1024
        # dataset type to ignore file availability check
        datasetTypeToSkipCheck = ['log']
        # thresholds for data availability check
        thrInputSize = self.taskBufferIF.getConfigValue(self.msgType, 'INPUT_SIZE_THRESHOLD', 'jedi', 'atlas')
        if thrInputSize is None:
            thrInputSize = 1
        thrInputSize *= 1024*1024*1024
        thrInputNum = self.taskBufferIF.getConfigValue(self.msgType, 'INPUT_NUM_THRESHOLD', 'jedi', 'atlas')
        if thrInputNum is None:
            thrInputNum = 100
        thrInputSizeFrac = self.taskBufferIF.getConfigValue(self.msgType, 'INPUT_SIZE_FRACTION', 'jedi', 'atlas')
        if thrInputSizeFrac is None:
            thrInputSizeFrac = 10
        thrInputSizeFrac = float(thrInputSizeFrac) / 100
        thrInputNumFrac = self.taskBufferIF.getConfigValue(self.msgType, 'INPUT_NUM_FRACTION', 'jedi', 'atlas')
        if thrInputNumFrac is None:
            thrInputNumFrac = 10
        thrInputNumFrac = float(thrInputNumFrac) / 100
        cutOffRW = 50
        negWeightTape = 0.001
        minIoIntensityWithLD = self.taskBufferIF.getConfigValue(self.msgType, 'MIN_IO_INTENSITY_WITH_LOCAL_DATA',
                                                                'jedi', 'atlas')
        if minIoIntensityWithLD is None:
            minIoIntensityWithLD = 200
        minInputSizeWithLD = self.taskBufferIF.getConfigValue(self.msgType, 'MIN_INPUT_SIZE_WITH_LOCAL_DATA',
                                                                'jedi', 'atlas')
        if minInputSizeWithLD is None:
            minInputSizeWithLD = 10000
        maxTaskPrioWithLD = self.taskBufferIF.getConfigValue(self.msgType, 'MAX_TASK_PRIO_WITH_LOCAL_DATA',
                                                                'jedi', 'atlas')
        if maxTaskPrioWithLD is None:
            maxTaskPrioWithLD = 800
        # main
        lastJediTaskID = None
        siteMapper = self.taskBufferIF.getSiteMapper()
        while True:
            try:
                taskInputList = self.inputList.get(1)
                # no more datasets
                if len(taskInputList) == 0:
                    self.logger.debug('{0} terminating after processing {1} tasks since no more inputs '.format(self.__class__.__name__,
                                                                                                                self.numTasks))
                    return
                # loop over all tasks
                for taskSpec,inputChunk in taskInputList:
                    lastJediTaskID = taskSpec.jediTaskID
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID),monToken='jediTaskID={0}'.format(taskSpec.jediTaskID))
                    tmpLog.debug('start')
                    tmpLog.info('thrInputSize:{} thrInputNum:{} thrInputSizeFrac:{} thrInputNumFrac:{}'.format(thrInputSize,
                                                                                                               thrInputNum,
                                                                                                               thrInputSizeFrac,
                                                                                                               thrInputNumFrac))
                    tmpLog.info('full-chain:{}'.format(taskSpec.get_full_chain()))
                    # read task parameters
                    try:
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception:
                        tmpLog.error('failed to read task params')
                        taskSpec.resetChangedList()
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        self.taskBufferIF.updateTask_JEDI(taskSpec, {'jediTaskID': taskSpec.jediTaskID},
                                                          oldStatus=['assigning'], updateDEFT=False,
                                                          setFrozenTime=False)
                        continue
                    # RW
                    taskRW = self.taskBufferIF.calculateTaskWorldRW_JEDI(taskSpec.jediTaskID)
                    # get nuclei
                    nucleusList = copy.copy(siteMapper.nuclei)
                    if taskSpec.get_full_chain():
                        # use satellites with bareNucleus as nuclei for full chain
                        for tmpNucleus, tmpNucleusSpec in iteritems(siteMapper.satellites):
                            if tmpNucleusSpec.get_bare_nucleus_mode():
                                nucleusList[tmpNucleus] = tmpNucleusSpec
                    # init summary list
                    self.init_summary_list('Task brokerage summary',
                                           None,
                                           nucleusList)
                    if taskSpec.nucleus in siteMapper.nuclei:
                        candidateNucleus = taskSpec.nucleus
                    elif taskSpec.nucleus in siteMapper.satellites:
                        nucleusList = siteMapper.satellites
                        candidateNucleus = taskSpec.nucleus
                    else:
                        tmpLog.info('got {0} candidates'.format(len(nucleusList)))
                        ######################################
                        # check status
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        for tmpNucleus,tmpNucleusSpec in iteritems(nucleusList):
                            if tmpNucleusSpec.state not in ['ACTIVE']:
                                tmpLog.info('  skip nucleus={0} due to status={1} criteria=-status'.format(tmpNucleus,
                                                                                                            tmpNucleusSpec.state))
                            else:
                                newNucleusList[tmpNucleus] = tmpNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info('{0} candidates passed status check'.format(len(nucleusList)))
                        self.add_summary_message(oldNucleusList, nucleusList, 'status check')
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                            continue
                        ######################################
                        # check status of transfer backlog
                        t1Weight = taskSpec.getT1Weight()
                        if t1Weight < 0:
                            tmpLog.info('skip transfer backlog check due to negative T1Weight')
                        else:
                            newNucleusList = {}
                            oldNucleusList = copy.copy(nucleusList)
                            backlogged_nuclei = self.taskBufferIF.getBackloggedNuclei()
                            for tmpNucleus, tmpNucleusSpec in iteritems(nucleusList):
                                if tmpNucleus in backlogged_nuclei:
                                    tmpLog.info('  skip nucleus={0} due to long transfer backlog criteria=-transfer_backlog'.
                                                 format(tmpNucleus))
                                else:
                                    newNucleusList[tmpNucleus] = tmpNucleusSpec
                            nucleusList = newNucleusList
                            tmpLog.info('{0} candidates passed transfer backlog check'.format(len(nucleusList)))
                            self.add_summary_message(oldNucleusList, nucleusList, 'transfer backlog check')
                            if not nucleusList:
                                self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                                continue
                        ######################################
                        # check endpoint
                        fractionFreeSpace = {}
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        tmpStat,tmpDatasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,
                                                                                                      ['output','log'])
                        for tmpNucleus, tmpNucleusSpec in iteritems(nucleusList):
                            toSkip = False
                            origNucleusSpec = tmpNucleusSpec
                            for tmpDatasetSpec in tmpDatasetSpecList:
                                tmpNucleusSpec = origNucleusSpec
                                # use secondary nucleus for full-chain if defined
                                if taskSpec.get_full_chain() and tmpNucleusSpec.get_secondary_nucleus():
                                    tmpNucleusSpec = siteMapper.getNucleus(tmpNucleusSpec.get_secondary_nucleus())
                                # ignore distributed datasets
                                if DataServiceUtils.getDistributedDestination(tmpDatasetSpec.storageToken) is not None:
                                    continue
                                # get endpoint with the pattern
                                tmpEP = tmpNucleusSpec.getAssociatedEndpoint(tmpDatasetSpec.storageToken)
                                if tmpEP is None:
                                    tmpLog.info('  skip nucleus={0} since no endpoint with {1} criteria=-match'.format(tmpNucleus,
                                                                                                                        tmpDatasetSpec.storageToken))
                                    toSkip = True
                                    break
                                # check state
                                """
                                if tmpEP['state'] not in ['ACTIVE']:
                                    tmpLog.info('  skip nucleus={0} since endpoint {1} is in {2} criteria=-epstatus'.format(tmpNucleus,
                                                                                                                             tmpEP['ddm_endpoint_name'],
                                                                                                                             tmpEP['state']))
                                    toSkip = True
                                    break
                                """
                                # check space
                                tmpSpaceSize = 0
                                if tmpEP['space_free']:
                                    tmpSpaceSize += tmpEP['space_free']
                                if tmpEP['space_expired']:
                                    tmpSpaceSize += tmpEP['space_expired']
                                tmpSpaceToUse = 0
                                if tmpNucleus in self.fullRW:
                                    # 0.25GB per cpuTime/corePower/day
                                    tmpSpaceToUse = long(self.fullRW[tmpNucleus]/10/24/3600*0.25)
                                if tmpSpaceSize-tmpSpaceToUse < diskThreshold:
                                    tmpLog.info('  skip nucleus={0} since disk shortage (free {1} GB - reserved {2} GB < thr {3} GB) at endpoint {4} criteria=-space'.format(tmpNucleus,
                                                                                                                                                                     tmpSpaceSize,
                                                                                                                                                                     tmpSpaceToUse,
                                                                                                                                                                     diskThreshold,
                                                                                                                                                                     tmpEP['ddm_endpoint_name']))
                                    toSkip = True
                                    break
                                # keep fraction of free space
                                if tmpNucleus not in fractionFreeSpace:
                                    fractionFreeSpace[tmpNucleus] = {'total':0,'free':0}
                                try:
                                    tmpOld = float(fractionFreeSpace[tmpNucleus]['free']) / \
                                        float(fractionFreeSpace[tmpNucleus]['total'])
                                except Exception:
                                    tmpOld = None
                                try:
                                    tmpNew = float(tmpSpaceSize-tmpSpaceToUse)/float(tmpEP['space_total'])
                                except Exception:
                                    tmpNew = None
                                if tmpNew is not None and (tmpOld is None or tmpNew < tmpOld):
                                    fractionFreeSpace[tmpNucleus] = {'total':tmpEP['space_total'],
                                                                     'free':tmpSpaceSize-tmpSpaceToUse}
                            if not toSkip:
                                newNucleusList[tmpNucleus] = origNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info('{0} candidates passed endpoint check {1} TB'.format(len(nucleusList),diskThreshold/1024))
                        self.add_summary_message(oldNucleusList, nucleusList, 'storage endpoint check')
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                            continue
                        ######################################
                        # ability to execute jobs
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        # get all panda sites
                        tmpSiteList = []
                        for tmpNucleus,tmpNucleusSpec in iteritems(nucleusList):
                            tmpSiteList += tmpNucleusSpec.allPandaSites
                        tmpSiteList = list(set(tmpSiteList))
                        tmpLog.debug('===== start for job check')
                        jobBroker = AtlasProdJobBroker(self.ddmIF,self.taskBufferIF)
                        tmpSt,tmpRet = jobBroker.doBrokerage(taskSpec, taskSpec.cloud, inputChunk, None,
                                                             True, tmpSiteList, tmpLog)
                        tmpLog.debug('===== done for job check')
                        if tmpSt != Interaction.SC_SUCCEEDED:
                            tmpLog.error('no sites can run jobs')
                            taskSpec.resetChangedList()
                            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                            self.taskBufferIF.updateTask_JEDI(taskSpec, {'jediTaskID': taskSpec.jediTaskID},
                                                              oldStatus=['assigning'], updateDEFT=False,
                                                              setFrozenTime=False)
                            continue
                        okNuclei = set()
                        for tmpSite in tmpRet:
                            siteSpec = siteMapper.getSite(tmpSite)
                            okNuclei.add(siteSpec.pandasite)
                        for tmpNucleus,tmpNucleusSpec in iteritems(nucleusList):
                            if tmpNucleus in okNuclei:
                                newNucleusList[tmpNucleus] = tmpNucleusSpec
                            else:
                                tmpLog.info('  skip nucleus={0} due to missing ability to run jobs criteria=-job'.format(tmpNucleus))
                        nucleusList = newNucleusList
                        tmpLog.info('{0} candidates passed job check'.format(len(nucleusList)))
                        self.add_summary_message(oldNucleusList, nucleusList, 'job check')
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                            continue
                        ######################################
                        # data locality
                        toSkip = False
                        availableData = {}
                        for datasetSpec in inputChunk.getDatasets():
                            # only for real datasets
                            if datasetSpec.isPseudo():
                                continue
                            # ignore DBR
                            if DataServiceUtils.isDBR(datasetSpec.datasetName):
                                continue
                            # skip locality check
                            if DataServiceUtils.getDatasetType(datasetSpec.datasetName) in datasetTypeToSkipCheck:
                                continue
                            # primary only
                            if taskParamMap.get('taskBrokerOnMaster') is True and not datasetSpec.isMaster():
                                continue
                            # use deep scan for primary dataset unless data carousel
                            if datasetSpec.isMaster() and not taskSpec.inputPreStaging():
                                deepScan = True
                            else:
                                deepScan = False
                            # get nuclei where data is available
                            tmpSt,tmpRet = AtlasBrokerUtils.getNucleiWithData(siteMapper,self.ddmIF,
                                                                              datasetSpec.datasetName,
                                                                              list(nucleusList.keys()),
                                                                              deepScan)
                            if tmpSt != Interaction.SC_SUCCEEDED:
                                self.post_process_for_error(taskSpec, tmpLog,
                                                            'failed to get nuclei where data is available, '
                                                            'since {0}'.format(tmpRet),
                                                            False)
                                toSkip = True
                                break
                            # sum
                            for tmpNucleus,tmpVals in iteritems(tmpRet):
                                if tmpNucleus not in availableData:
                                    availableData[tmpNucleus] = tmpVals
                                else:
                                    availableData[tmpNucleus] = dict((k,v+tmpVals[k]) for (k,v) in iteritems(availableData[tmpNucleus]))
                        if toSkip:
                            continue
                        if availableData != {}:
                            newNucleusList = {}
                            oldNucleusList = copy.copy(nucleusList)
                            # skip if no data
                            skipMsgList = []
                            for tmpNucleus,tmpNucleusSpec in iteritems(nucleusList):
                                if taskSpec.inputPreStaging() and availableData[tmpNucleus]['ava_num_any'] > 0:
                                    # use incomplete replicas for data carousel since the completeness is guaranteed
                                    newNucleusList[tmpNucleus] = tmpNucleusSpec
                                elif availableData[tmpNucleus]['tot_size'] > thrInputSize and \
                                        availableData[tmpNucleus]['ava_size_any'] < availableData[tmpNucleus]['tot_size'] * thrInputSizeFrac:
                                    tmpMsg = '  skip nucleus={0} due to insufficient input size {1}B < {2}*{3} criteria=-insize'.format(tmpNucleus,
                                                                                                                                        availableData[tmpNucleus]['ava_size_any'],
                                                                                                                                        availableData[tmpNucleus]['tot_size'],
                                                                                                                                        thrInputSizeFrac)
                                    skipMsgList.append(tmpMsg)
                                elif availableData[tmpNucleus]['tot_num'] > thrInputNum and \
                                        availableData[tmpNucleus]['ava_num_any'] < availableData[tmpNucleus]['tot_num'] * thrInputNumFrac:
                                    tmpMsg = '  skip nucleus={0} due to short number of input files {1} < {2}*{3} criteria=-innum'.format(tmpNucleus,
                                                                                                                                          availableData[tmpNucleus]['ava_num_any'],
                                                                                                                                          availableData[tmpNucleus]['tot_num'],
                                                                                                                                          thrInputNumFrac)
                                    skipMsgList.append(tmpMsg)
                                else:
                                    newNucleusList[tmpNucleus] = tmpNucleusSpec
                            totInputSize = list(availableData.values())[0]['tot_size']/1024/1024/1024
                            data_locality_check_str = (
                                '(ioIntensity ({0}) is None or less than {1} kBPerS '
                                'and input size ({2} GB) is less than {3}) '
                                'or task.currentPriority ({4}) is higher than or equal to {5}').format(
                                taskSpec.ioIntensity, minIoIntensityWithLD,
                                int(totInputSize), minInputSizeWithLD,
                                taskSpec.currentPriority, maxTaskPrioWithLD)
                            if len(newNucleusList) > 0:
                                nucleusList = newNucleusList
                                for tmpMsg in skipMsgList:
                                    tmpLog.info(tmpMsg)
                            elif ((taskSpec.ioIntensity is None
                                  or taskSpec.ioIntensity <= minIoIntensityWithLD)
                                  and totInputSize <= minInputSizeWithLD) \
                                  or taskSpec.currentPriority >= maxTaskPrioWithLD:
                                availableData = {}
                                tmpLog.info(
                                    '  disable data locality check since no nucleus has input data, {}'.format(
                                        data_locality_check_str))
                            else:
                                # no candidate + unavoidable data locality check
                                nucleusList = newNucleusList
                                for tmpMsg in skipMsgList:
                                    tmpLog.info(tmpMsg)
                                tmpLog.info(
                                    '  the following conditions required to disable data locality check: {}'.format(
                                        data_locality_check_str))
                            tmpLog.info('{0} candidates passed data check'.format(len(nucleusList)))
                            self.add_summary_message(oldNucleusList, nucleusList, 'data check')
                            if not nucleusList:
                                self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                                continue
                        ######################################
                        # check for full chain
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        parent_full_chain = False
                        if taskSpec.get_full_chain() and taskSpec.jediTaskID != taskSpec.parent_tid:
                            tmpStat, parentTaskSpec = self.taskBufferIF.getTaskWithID_JEDI(taskSpec.parent_tid, False)

                            if not tmpStat or parentTaskSpec is None:
                                self.post_process_for_error(taskSpec, tmpLog,
                                                            'failed to get parent taskSpec with '
                                                            'jediTaskID={0}'.format(taskSpec.parent_tid),
                                                            False)
                                continue
                            parent_full_chain = parentTaskSpec.check_full_chain_with_nucleus(
                                siteMapper.getNucleus(parentTaskSpec.nucleus))
                        for tmpNucleus, tmpNucleusSpec in iteritems(nucleusList):
                            # nucleus to run only full-chain tasks
                            if tmpNucleusSpec.get_bare_nucleus_mode() == 'only' and taskSpec.get_full_chain() is None:
                                tmpLog.info('  skip nucleus={} since only full-chain tasks are '
                                            'allowed criteria=-full_chain'.format(tmpNucleus))
                                continue
                            # task requirement to run on nucleus with full-chain support
                            if tmpNucleusSpec.get_bare_nucleus_mode() is None and \
                                (taskSpec.check_full_chain_with_mode('only') or
                                 taskSpec.check_full_chain_with_mode('require')):
                                tmpLog.info('  skip nucleus={} due to lack of full-chain capability '
                                            'criteria=-full_chain'.format(tmpNucleus))
                                continue
                            # check parent task
                            if taskSpec.get_full_chain() and parent_full_chain:
                                if tmpNucleus != parentTaskSpec.nucleus:
                                    tmpLog.info('  skip nucleus={} since the parent of the full-chain ran elsewhere '
                                                'criteria=-full_chain'.format(tmpNucleus))
                                    continue
                            newNucleusList[tmpNucleus] = tmpNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info('{0} candidates passed full-chain check'.format(len(nucleusList)))
                        self.add_summary_message(oldNucleusList, nucleusList, 'full-chain check')
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, 'no candidates')
                            continue
                        self.dump_summary(tmpLog, nucleusList)
                        ######################################
                        # weight
                        self.prioRW.acquire()
                        nucleusRW = self.prioRW[taskSpec.currentPriority]
                        self.prioRW.release()
                        totalWeight = 0
                        nucleusweights = []
                        for tmpNucleus,tmpNucleusSpec in iteritems(nucleusList):
                            if tmpNucleus not in nucleusRW:
                                nucleusRW[tmpNucleus] = 0
                            wStr = '1'
                            # with RW
                            if tmpNucleus in nucleusRW and nucleusRW[tmpNucleus] and nucleusRW[tmpNucleus] >= cutOffRW:
                                weight = 1 / float(nucleusRW[tmpNucleus])
                                wStr += '/( RW={0} )'.format(nucleusRW[tmpNucleus])
                            else:
                                weight = 1
                                wStr += '/(1 : RW={0}<{1})'.format(nucleusRW[tmpNucleus],cutOffRW)
                            # with data
                            if availableData != {}:
                                if availableData[tmpNucleus]['tot_size'] > 0:
                                    weight *= float(availableData[tmpNucleus]['ava_size_any'])
                                    weight /= float(availableData[tmpNucleus]['tot_size'])
                                    wStr += '* ( available_input_size_DISKTAPE={0} )'.format(availableData[tmpNucleus]['ava_size_any'])
                                    wStr += '/ ( total_input_size={0} )'.format(availableData[tmpNucleus]['tot_size'])
                                    # negative weight for tape
                                    if availableData[tmpNucleus]['ava_size_any'] > availableData[tmpNucleus]['ava_size_disk']:
                                        weight *= negWeightTape
                                        wStr += '*( weight_TAPE={0} )'.format(negWeightTape)
                            # fraction of free space
                            if tmpNucleus in fractionFreeSpace:
                                try:
                                    tmpFrac = float(fractionFreeSpace[tmpNucleus]['free']) / \
                                        float(fractionFreeSpace[tmpNucleus]['total'])
                                    weight *= tmpFrac
                                    wStr += '*( free_space={0} )/( total_space={1} )'.format(fractionFreeSpace[tmpNucleus]['free'],
                                                                                         fractionFreeSpace[tmpNucleus]['total'])
                                except Exception:
                                    pass
                            tmpLog.info('  use nucleus={0} weight={1} {2} criteria=+use'.format(tmpNucleus,weight,wStr))
                            totalWeight += weight
                            nucleusweights.append((tmpNucleus,weight))
                        ######################################
                        # final selection
                        tgtWeight = random.uniform(0,totalWeight)
                        candidateNucleus = None
                        for tmpNucleus,weight in nucleusweights:
                            tgtWeight -= weight
                            if tgtWeight <= 0:
                                candidateNucleus = tmpNucleus
                                break
                        if candidateNucleus is None:
                            candidateNucleus = nucleusweights[-1][0]
                    ######################################
                    # update
                    nucleusSpec = nucleusList[candidateNucleus]
                    # get output/log datasets
                    tmpStat,tmpDatasetSpecs = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,
                                                                                               ['output','log'])
                    # get destinations
                    retMap = {taskSpec.jediTaskID: AtlasBrokerUtils.getDictToSetNucleus(nucleusSpec,tmpDatasetSpecs)}
                    tmpRet = self.taskBufferIF.setCloudToTasks_JEDI(retMap)
                    tmpLog.info('  set nucleus={0} with {1} criteria=+set'.format(candidateNucleus,tmpRet))
                    if tmpRet:
                        tmpMsg = 'set task_status=ready'
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                    # update RW table
                    self.prioRW.acquire()
                    for prio,rwMap in iteritems(self.prioRW):
                        if prio > taskSpec.currentPriority:
                            continue
                        if candidateNucleus in rwMap:
                            if not rwMap[candidateNucleus]:
                                rwMap[candidateNucleus] = 0
                            rwMap[candidateNucleus] += taskRW
                        else:
                            rwMap[candidateNucleus] = taskRW
                    self.prioRW.release()
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                errMsg  = '{0}.runImpl() failed with {1} {2} '.format(self.__class__.__name__,errtype.__name__,errvalue)
                errMsg += 'lastJediTaskID={0} '.format(lastJediTaskID)
                errMsg += traceback.format_exc()
                logger.error(errMsg)
