import re
import os
import sys
import time
import copy
import urllib
import socket
import random
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore import ParseJobXML
from pandajedi.jedicore import JediCoreUtils
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.userinterface import Client as PandaClient

from JobThrottler import JobThrottler
from JobBroker    import JobBroker
from JobSplitter  import JobSplitter
from TaskSetupper import TaskSetupper
from JediKnight   import JediKnight

from pandajedi.jediconfig import jedi_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])



# worker class to generate jobs
class JobGenerator (JediKnight):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
                 withThrottle=True,execJobs=True):
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-gen'.format(socket.getfqdn().split('.')[0],os.getpid())
        self.cloudList = cloudList
        self.withThrottle = withThrottle
        self.execJobs = execJobs
        

    # main
    def start(self):
        # start base class
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug('start')
                # get SiteMapper
                siteMapper = self.taskBufferIF.getSiteMapper()
                # get work queue mapper
                workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                # get Throttle
                throttle = JobThrottler(self.vos,self.prodSourceLabels)
                throttle.initializeMods(self.taskBufferIF)
                # get TaskSetupper
                taskSetupper = TaskSetupper(self.vos,self.prodSourceLabels)
                taskSetupper.initializeMods(self.taskBufferIF,self.ddmIF)
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get job statistics
                        tmpSt,jobStat = self.taskBufferIF.getJobStatWithWorkQueuePerCloud_JEDI(vo,prodSourceLabel)
                        if not tmpSt:
                            raise RuntimeError,'failed to get job statistics'
                        # loop over all clouds
                        random.shuffle(self.cloudList)
                        for cloudName in self.cloudList:
                            # loop over all work queues
                            workQueueList = workQueueMapper.getQueueListWithVoType(vo,prodSourceLabel)
                            tmpLog.debug("{0} workqueues for vo:{1} label:{2}".format(len(workQueueList),vo,prodSourceLabel))
                            for workQueue in workQueueList:
                                cycleStr = 'pid={5} vo={0} cloud={1} queue={2}(id={3}) label={4}'.format(vo,cloudName,
                                                                                                         workQueue.queue_name,
                                                                                                         workQueue.queue_id,
                                                                                                         workQueue.queue_type,
                                                                                                         self.pid)
                                tmpLog.debug('start {0}'.format(cycleStr))
                                # throttle
                                tmpLog.debug('check throttle with {0}'.format(throttle.getClassName(vo,workQueue.queue_type)))
                                try:
                                    tmpSt,thrFlag = throttle.toBeThrottled(vo,workQueue.queue_type,cloudName,workQueue,jobStat)
                                except:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error('throttler failed with {0} {1}'.format(errtype,errvalue))
                                    raise RuntimeError,'crashed when checking throttle'
                                if tmpSt != self.SC_SUCCEEDED:
                                    raise RuntimeError,'failed to check throttle'
                                if thrFlag:
                                    tmpLog.debug('throttled')
                                    if self.withThrottle:
                                        continue
                                tmpLog.debug('minPriority={0} maxNumJobs={1}'.format(throttle.minPriority,throttle.maxNumJobs))
                                # get typical number of files
                                typicalNumFilesMap = self.taskBufferIF.getTypicalNumInput_JEDI(vo,workQueue.queue_type,workQueue,
                                                                                               useResultCache=600)
                                if typicalNumFilesMap == None:
                                    raise RuntimeError,'failed to get typical number of files'
                                # get the list of input 
                                tmpList = self.taskBufferIF.getTasksToBeProcessed_JEDI(self.pid,vo,
                                                                                       workQueue,
                                                                                       workQueue.queue_type,
                                                                                       cloudName,
                                                                                       nTasks=jedi_config.jobgen.nTasks,
                                                                                       nFiles=jedi_config.jobgen.nFiles,
                                                                                       minPriority=throttle.minPriority,
                                                                                       maxNumJobs=throttle.maxNumJobs,
                                                                                       typicalNumFilesMap=typicalNumFilesMap, 
                                                                                       )
                                if tmpList == None:
                                    # failed
                                    tmpLog.error('failed to get the list of input chunks to generate jobs')
                                else:
                                    tmpLog.debug('got {0} input chunks'.format(len(tmpList)))
                                    if len(tmpList) != 0: 
                                        # put to a locked list
                                        inputList = ListWithLock(tmpList)
                                        # make thread pool
                                        threadPool = ThreadPool() 
                                        # make workers
                                        nWorker = jedi_config.jobgen.nWorkers
                                        for iWorker in range(nWorker):
                                            thr = JobGeneratorThread(inputList,threadPool,
                                                                     self.taskBufferIF,self.ddmIF,
                                                                     siteMapper,self.execJobs,
                                                                     taskSetupper,
                                                                     self.pid)
                                            thr.start()
                                        # join
                                        threadPool.join()
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,
                                                                         errtype.__name__,errvalue))
            # sleep if needed
            tmpLog.debug('end')            
            loopCycle = jedi_config.jobgen.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep()
        


# thread for real worker
class JobGeneratorThread (WorkerThread):

    # constructor
    def __init__(self,inputList,threadPool,taskbufferIF,ddmIF,siteMapper,
                 execJobs,taskSetupper,pid):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.inputList    = inputList
        self.taskBufferIF = taskbufferIF
        self.ddmIF        = ddmIF
        self.siteMapper   = siteMapper
        self.execJobs     = execJobs
        self.numGenJobs   = 0
        self.taskSetupper = taskSetupper
        self.msgType      = 'jobgenerator'
        self.pid          = pid


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nInput = 2
                inputList = self.inputList.get(nInput)
                # no more datasets
                if len(inputList) == 0:
                    self.logger.debug('{0} terminating after generating {1} jobs since no more inputs '.format(self.__class__.__name__,
                                                                                                               self.numGenJobs))
                    return
                # loop over all inputs
                for taskSpec,cloudName,inputChunk in inputList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0} datasetID={1}>'.format(taskSpec.jediTaskID,
                                                                                            inputChunk.masterIndexName),
                                        monToken='<jediTaskID={0}>'.format(taskSpec.jediTaskID))
                    tmpLog.info('start with VO={0} cloud={1}'.format(taskSpec.vo,cloudName))
                    tmpLog.sendMsg('start to generate jobs',self.msgType)
                    readyToSubmitJob = False
                    jobsSubmitted = False
                    goForward = True
                    taskParamMap = None
                    # initialize brokerage
                    if goForward:        
                        jobBroker = JobBroker(taskSpec.vo,taskSpec.prodSourceLabel)
                        tmpStat = jobBroker.initializeMods(self.ddmIF.getInterface(taskSpec.vo),
                                                           self.taskBufferIF)
                        if not tmpStat:
                            tmpErrStr = 'failed to initialize JobBroker'
                            tmpLog.error(tmpErrStr)
                            taskSpec.status = 'tobroken'
                            taskSpec.setErrDiag(tmpErrStr)                        
                            goForward = False
                    # read task params if nessesary
                    if taskSpec.useLimitedSites():
                        tmpStat,taskParamMap = self.readTaskParams(taskSpec,taskParamMap,tmpLog)
                        if not tmpStat:
                            tmpErrStr = 'failed to read task params'
                            tmpLog.error(tmpErrStr)
                            taskSpec.status = 'tobroken'
                            taskSpec.setErrDiag(tmpErrStr)                        
                            goForward = False
                    # run brokerage
                    if goForward:
                        tmpLog.info('run brokerage with {0}'.format(jobBroker.getClassName(taskSpec.vo,
                                                                                           taskSpec.prodSourceLabel)))
                        try:
                            tmpStat,inputChunk = jobBroker.doBrokerage(taskSpec,cloudName,inputChunk,taskParamMap)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('brokerage crashed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                        if tmpStat != Interaction.SC_SUCCEEDED:
                            tmpErrStr = 'brokerage failed'
                            tmpLog.error(tmpErrStr)
                            taskSpec.setOnHold()
                            taskSpec.setErrDiag(tmpErrStr,True)
                            goForward = False
                    # run splitter
                    if goForward:
                        tmpLog.info('run splitter')
                        splitter = JobSplitter()
                        try:
                            tmpStat,subChunks = splitter.doSplit(taskSpec,inputChunk,self.siteMapper)
                            # * remove the last sub-chunk when inputChunk is read in a block 
                            #   since alignment could be broken in the last sub-chunk 
                            #    e.g., inputChunk=10 -> subChunks=4,4,2 and remove 2
                            # * don't remove it for the last inputChunk
                            # e.g., inputChunks = 10(remove),10(remove),3(not remove)
                            if len(subChunks[-1]['subChunks']) > 1 and inputChunk.masterDataset != None \
                                    and inputChunk.readBlock == True:
                                subChunks[-1]['subChunks'] = subChunks[-1]['subChunks'][:-1]
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('splitter crashed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                        if tmpStat != Interaction.SC_SUCCEEDED:
                            tmpErrStr = 'splitting failed'
                            tmpLog.error(tmpErrStr)
                            taskSpec.setOnHold()
                            taskSpec.setErrDiag(tmpErrStr)                                
                            goForward = False
                    # generate jobs
                    if goForward:
                        tmpLog.info('run job generator')
                        try:
                            tmpStat,pandaJobs,datasetToRegister,oldPandaIDs = self.doGenerate(taskSpec,cloudName,subChunks,
                                                                                              inputChunk,tmpLog,
                                                                                              taskParamMap=taskParamMap)
                            # increase event service consumers
                            if tmpStat == Interaction.SC_SUCCEEDED:
                                if taskSpec.useEventService():
                                    pandaJobs = self.increaseEventServiceConsumers(pandaJobs,taskSpec.getNumEventServiceConsumer())
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('generator crashed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                        if tmpStat != Interaction.SC_SUCCEEDED:
                            tmpErrStr = 'job generation failed'
                            tmpLog.error(tmpErrStr)
                            taskSpec.status = 'tobroken'
                            taskSpec.setErrDiag(tmpErrStr)
                            goForward = False
                    # lock task
                    if goForward:
                        tmpLog.info('lock task')
                        tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID,self.pid)
                        if tmpStat == False:
                            tmpLog.info('skip due to lock failure')
                            continue
                    # setup task
                    if goForward:
                        tmpLog.info('run setupper with {0}'.format(self.taskSetupper.getClassName(taskSpec.vo,
                                                                                                  taskSpec.prodSourceLabel)))
                        tmpStat = self.taskSetupper.doSetup(taskSpec,datasetToRegister)
                        if tmpStat == Interaction.SC_FATAL:
                            tmpErrStr = 'fatal error when setup task'
                            tmpLog.error(tmpErrStr)
                            taskSpec.status = 'tobroken'
                            taskSpec.setErrDiag(tmpErrStr,True)
                        elif tmpStat != Interaction.SC_SUCCEEDED:
                            tmpErrStr = 'failed to setup task'
                            tmpLog.error(tmpErrStr)
                            taskSpec.setOnHold()
                            taskSpec.setErrDiag(tmpErrStr,True)
                        else:
                            readyToSubmitJob = True
                    # lock task
                    if goForward:
                        tmpLog.info('lock task')
                        tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID,self.pid)
                        if tmpStat == False:
                            tmpLog.info('skip due to lock failure')
                            continue
                    # submit
                    if readyToSubmitJob:
                        # submit
                        fqans = taskSpec.makeFQANs()
                        tmpLog.info('submit jobs with FQAN={0}'.format(','.join(str(fqan) for fqan in fqans)))
                        resSubmit = self.taskBufferIF.storeJobs(pandaJobs,taskSpec.userName,
                                                                fqans=fqans,toPending=True)
                        pandaIDs = []
                        oldNewPandaIDs = {}
                        for idxItem,items in enumerate(resSubmit):
                            if items[0] != 'NULL':
                                pandaIDs.append(items[0])
                                if len(oldPandaIDs) > idxItem and oldPandaIDs[idxItem] != []:
                                    oldNewPandaIDs[items[0]] = oldPandaIDs[idxItem]
                        # record retry history
                        if inputChunk.isMerging:
                            relationType = 'merge'
                        else:
                            relationType = 'retry'
                        self.taskBufferIF.recordRetryHistory_JEDI(taskSpec.jediTaskID,oldNewPandaIDs,relationType)
                        # check if submission was successful
                        if len(pandaIDs) == len(pandaJobs):
                            tmpMsg = 'successfully submitted {0}/{1}'.format(len(pandaIDs),len(pandaJobs))
                            tmpLog.info(tmpMsg)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                            if self.execJobs:
                                statExe,retExe = PandaClient.reassignJobs(pandaIDs,forPending=True)
                                tmpLog.info('exec {0} jobs with status={1}'.format(len(pandaIDs),retExe))
                            jobsSubmitted = True
                            if inputChunk.isMerging:
                                # don't change task status by merging
                                pass
                            elif taskSpec.usePrePro():
                                taskSpec.status = 'preprocessing'
                            elif inputChunk.useScout():
                                taskSpec.status = 'scouting'
                            else:
                                taskSpec.status = 'running'
                        else:
                            tmpErrStr = 'submitted only {0}/{1}'.format(len(pandaIDs),len(pandaJobs))
                            tmpLog.error(tmpErrStr)
                            taskSpec.setOnHold()
                            taskSpec.setErrDiag(tmpErrStr)
                        # the number of generated jobs     
                        self.numGenJobs += len(pandaIDs)    
                    # reset unused files
                    self.taskBufferIF.resetUnusedFiles_JEDI(taskSpec.jediTaskID,inputChunk)
                    # update task
                    taskSpec.lockedBy = None
                    retDB = self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID},
                                                              oldStatus=JediTaskSpec.statusForJobGenerator())
                    tmpMsg = 'set task.status={0}'.format(taskSpec.status)
                    tmpLog.info(tmpMsg)
                    if not taskSpec.errorDialog in ['',None]:
                        tmpMsg += ' ' + taskSpec.errorDialog
                    tmpLog.sendMsg(tmpMsg,self.msgType)
                    tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('%s.runImpl() failed with %s %s' % (self.__class__.__name__,errtype.__name__,errvalue))



    # read task parameters
    def readTaskParams(self,taskSpec,taskParamMap,tmpLog):
        # already read
        if taskParamMap != None:
            return True,taskParamMap
        try:
            # read task parameters
            taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            taskParamMap = RefinerUtils.decodeJSON(taskParam)
            return True,taskParamMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('task param conversion from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
            return False,None



    # generate jobs
    def doGenerate(self,taskSpec,cloudName,inSubChunkList,inputChunk,tmpLog,simul=False,taskParamMap=None):
        # return for failure
        failedRet = Interaction.SC_FAILED,None,None,None
        # read task parameters
        if taskSpec.useBuild() or taskSpec.usePrePro() or inputChunk.isMerging or \
                taskSpec.useLoadXML() or taskSpec.useListPFN():
            tmpStat,taskParamMap = self.readTaskParams(taskSpec,taskParamMap,tmpLog)
            if not tmpStat:
                return failedRet
        # special priorities 
        scoutPriority = 900
        mergePriority = 5000
        try:
            # load XML
            xmlConfig = None
            if taskSpec.useLoadXML():
                loadXML = taskParamMap['loadXML']
                xmlConfig = ParseJobXML.dom_parser(xmlStr=loadXML)
            # loop over all sub chunks
            jobSpecList = []
            outDsMap = {}
            datasetToRegister = []
            oldPandaIDs = []
            siteDsMap = {}
            esIndex = 0
            for tmpInChunk in inSubChunkList:
                siteName      = tmpInChunk['siteName']
                inSubChunks   = tmpInChunk['subChunks']
                siteCandidate = tmpInChunk['siteCandidate']
                buildFileSpec = None
                # make preprocessing job
                if taskSpec.usePrePro():
                    tmpStat,preproJobSpec,tmpToRegister = self.doGeneratePrePro(taskSpec,cloudName,siteName,taskParamMap,
                                                                                inSubChunks,tmpLog,simul)
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error('failed to generate prepro job')
                        return failedRet
                    # append
                    jobSpecList.append(preproJobSpec)
                    oldPandaIDs.append([])
                    # append datasets
                    for tmpToRegisterItem in tmpToRegister:
                        if not tmpToRegisterItem in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                    break
                # make build job
                elif taskSpec.useBuild():
                    tmpStat,buildJobSpec,buildFileSpec,tmpToRegister = self.doGenerateBuild(taskSpec,cloudName,siteName,
                                                                                            taskParamMap,tmpLog,simul)
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error('failed to generate build job')
                        return failedRet
                    # append
                    if buildJobSpec != None:
                        jobSpecList.append(buildJobSpec)
                        oldPandaIDs.append([])
                    # append datasets
                    for tmpToRegisterItem in tmpToRegister:
                        if not tmpToRegisterItem in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                # make normal jobs
                for inSubChunk in inSubChunks:
                    subOldPandaIDs = []
                    jobSpec = JobSpec()
                    jobSpec.jobDefinitionID  = 0
                    jobSpec.jobExecutionID   = 0
                    jobSpec.attemptNr        = 1
                    if taskSpec.disableAutoRetry():
                        # disable server/pilot retry
                        jobSpec.maxAttempt   = -1
                    elif taskSpec.useEventService():
                        # set max attempt for event service
                        jobSpec.maxAttempt   = 3
                    else:
                        jobSpec.maxAttempt   = jobSpec.attemptNr
                    jobSpec.jobName          = taskSpec.taskName
                    if inputChunk.isMerging:
                        # use merge trf
                        jobSpec.transformation = taskParamMap['mergeSpec']['transPath']
                    else:
                        jobSpec.transformation = taskSpec.transPath
                    jobSpec.cmtConfig        = taskSpec.architecture
                    if taskSpec.transHome != None:
                        jobSpec.homepackage  = re.sub('-(?P<dig>\d)','/\g<dig>',taskSpec.transHome)
                        jobSpec.homepackage  = re.sub('\r','',jobSpec.homepackage)
                    jobSpec.prodSourceLabel  = taskSpec.prodSourceLabel
                    if inputChunk.isMerging:
                        # set merging type
                        jobSpec.processingType = 'pmerge'
                    else:
                        jobSpec.processingType = taskSpec.processingType
                    jobSpec.jediTaskID       = taskSpec.jediTaskID
                    jobSpec.taskID           = taskSpec.reqID
                    jobSpec.jobsetID         = taskSpec.reqID
                    jobSpec.workingGroup     = taskSpec.workingGroup
                    jobSpec.countryGroup     = taskSpec.countryGroup
                    jobSpec.computingSite    = siteName
                    jobSpec.cloud            = cloudName
                    jobSpec.VO               = taskSpec.vo
                    jobSpec.prodSeriesLabel  = 'pandatest'
                    jobSpec.AtlasRelease     = taskSpec.transUses
                    jobSpec.AtlasRelease     = re.sub('\r','',jobSpec.AtlasRelease)
                    jobSpec.maxCpuCount      = taskSpec.walltime
                    jobSpec.maxCpuUnit       = taskSpec.walltimeUnit
                    jobSpec.maxDiskCount     = taskSpec.outDiskCount
                    jobSpec.maxDiskUnit      = taskSpec.outDiskUnit
                    jobSpec.minRamCount      = taskSpec.ramCount
                    jobSpec.minRamUnit       = taskSpec.ramUnit
                    jobSpec.coreCount        = taskSpec.coreCount
                    jobSpec.ipConnectivity   = 'yes'
                    jobSpec.metadata         = ''
                    if inputChunk.isMerging:
                        # give higher priority to merge jobs
                        jobSpec.assignedPriority = mergePriority
                    elif inputChunk.useScout() and taskSpec.taskPriority < scoutPriority:
                        # give higher priority to scouts
                        jobSpec.assignedPriority = scoutPriority
                    else:
                        jobSpec.assignedPriority = taskSpec.taskPriority
                    jobSpec.currentPriority  = jobSpec.assignedPriority
                    jobSpec.lockedby         = 'jedi'
                    jobSpec.workQueue_ID     = taskSpec.workQueue_ID
                    # using grouping with boundaryID
                    useBoundary = taskSpec.useGroupWithBoundaryID()
                    boundaryID = None
                    # flag for merging
                    isUnMerging = False
                    isMerging = False
                    # set specialHandling for Event Service
                    specialHandling = ''
                    if taskSpec.useEventService():
                        nEventsPerWorker = taskSpec.getNumEventsPerWorker()
                        specialHandling = EventServiceUtils.getHeaderForES(esIndex)
                    # inputs
                    prodDBlock = None
                    setProdDBlock = False
                    totalMasterSize = 0
                    for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
                        # get boundaryID if grouping is done with boundaryID
                        if useBoundary != None and boundaryID == None:
                            if tmpDatasetSpec.isMaster():
                                boundaryID = tmpFileSpecList[0].boundaryID
                        # get prodDBlock        
                        if not tmpDatasetSpec.isPseudo():
                            if tmpDatasetSpec.isMaster():
                                jobSpec.prodDBlock = tmpDatasetSpec.datasetName
                                setProdDBlock = True
                            else:
                                prodDBlock = tmpDatasetSpec.datasetName
                        # making files
                        for tmpFileSpec in tmpFileSpecList:
                            if inputChunk.isMerging:
                                tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec,setType='input')
                            else:
                                tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec)
                            # set status
                            if tmpFileSpec.locality in ['localdisk','remote']:
                                tmpInFileSpec.status = 'ready'
                            elif tmpFileSpec.locality == 'cache':
                                tmpInFileSpec.status = 'cached'
                            # local IO
                            if taskSpec.useLocalIO() or (inputChunk.isMerging and taskParamMap['mergeSpec'].has_key('useLocalIO')):
                                tmpInFileSpec.prodDBlockToken = 'local'
                            jobSpec.addFile(tmpInFileSpec)
                            # use remote access
                            if tmpFileSpec.locality == 'remote':
                                jobSpec.transferType = siteCandidate.remoteProtocol
                                jobSpec.sourceSite = siteCandidate.remoteSource
                            # collect old PandaIDs
                            if tmpFileSpec.PandaID != None and not tmpFileSpec.PandaID in subOldPandaIDs:
                                subOldPandaIDs.append(tmpFileSpec.PandaID)
                            # set specialHandling for Event Service
                            if taskSpec.useEventService() and tmpDatasetSpec.isMaster() and not tmpDatasetSpec.isPseudo():
                                specialHandling += EventServiceUtils.encodeFileInfo(tmpFileSpec.lfn,
                                                                                    tmpFileSpec.firstEvent+tmpFileSpec.startEvent-1,
                                                                                    tmpFileSpec.firstEvent+tmpFileSpec.endEvent-1,
                                                                                    nEventsPerWorker)
                            # calcurate total master size
                            if tmpDatasetSpec.isMaster():
                                totalMasterSize += JediCoreUtils.getEffectiveFileSize(tmpFileSpec.fsize,tmpFileSpec.startEvent,
                                                                                      tmpFileSpec.endEvent,tmpFileSpec.nEvents)
                        # check if merging 
                        if taskSpec.mergeOutput() and tmpDatasetSpec.isMaster() and not tmpDatasetSpec.toMerge():
                            isUnMerging = True
                    specialHandling = specialHandling[:-1]
                    if specialHandling != '':
                        jobSpec.specialHandling = specialHandling
                    # use secondary dataset name as prodDBlock
                    if setProdDBlock == False and prodDBlock != None:
                        jobSpec.prodDBlock = prodDBlock
                    # set provenanceID
                    provenanceID = None    
                    if useBoundary != None and useBoundary['outMap'] == True:
                        provenanceID = boundaryID
                    # instantiate template datasets
                    instantiateTmpl  = False
                    instantiatedSite = None
                    if isUnMerging:
                        instantiateTmpl = True
                        instantiatedSite = siteName
                    elif taskSpec.instantiateTmpl():
                        instantiateTmpl = True
                        if taskSpec.instantiateTmplSite():
                            instantiatedSite = siteName
                    else:
                        instantiateTmpl = False
                    # multiply maxDiskCount and maxCpuCount by total master size
                    try:
                        jobSpec.maxCpuCount *= totalMasterSize
                    except:
                        pass
                    try:
                        jobSpec.maxDiskCount *= totalMasterSize
                    except:
                        pass
                    # add offset to maxDiskCount
                    try:
                        jobSpec.maxDiskCount += taskSpec.workDiskCount
                    except:
                        pass
                    # XML config
                    xmlConfigJob = None
                    if xmlConfig != None:
                        try:
                            xmlConfigJob = xmlConfig.jobs[boundaryID]
                        except:
                            tmpLog.error('failed to get XML config for N={0}'.format(boundaryID))
                            return failedRet
                    # outputs
                    outSubChunk,serialNr,tmpToRegister,siteDsMap = self.taskBufferIF.getOutputFiles_JEDI(taskSpec.jediTaskID,
                                                                                                         provenanceID,
                                                                                                         simul,
                                                                                                         instantiateTmpl,
                                                                                                         instantiatedSite,
                                                                                                         isUnMerging,
                                                                                                         False,
                                                                                                         xmlConfigJob,
                                                                                                         siteDsMap)
                    if outSubChunk == None:
                        # failed
                        tmpLog.error('failed to get OutputFiles')
                        return failedRet
                    for tmpToRegisterItem in tmpToRegister:
                        if not tmpToRegisterItem in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                    for tmpFileSpec in outSubChunk.values():
                        # get dataset
                        if not outDsMap.has_key(tmpFileSpec.datasetID):
                            tmpStat,tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID,
                                                                                         tmpFileSpec.datasetID)
                            # not found
                            if not tmpStat:
                                tmpLog.error('failed to get DS with datasetID={0}'.format(tmpFileSpec.datasetID))
                                return failedRet
                            outDsMap[tmpFileSpec.datasetID] = tmpDataset 
                        # convert to job's FileSpec     
                        tmpDatasetSpec = outDsMap[tmpFileSpec.datasetID]
                        tmpOutFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec,
                                                                          useEventService=taskSpec.useEventService())
                        jobSpec.addFile(tmpOutFileSpec)
                    # lib.tgz
                    paramList = []    
                    if buildFileSpec != None:
                        tmpBuildFileSpec = copy.copy(buildFileSpec)
                        jobSpec.addFile(tmpBuildFileSpec)
                        paramList.append(('LIB',buildFileSpec.lfn))
                    # placeholders for XML config
                    if xmlConfigJob != None:
                        paramList.append(('XML_OUTMAP',xmlConfigJob.get_outmap_str(outSubChunk)))
                        paramList.append(('XML_EXESTR',xmlConfigJob.exec_string_enc()))
                    # job parameter
                    jobSpec.jobParameters = self.makeJobParameters(taskSpec,inSubChunk,outSubChunk,
                                                                   serialNr,paramList,jobSpec,simul,
                                                                   taskParamMap,inputChunk.isMerging,
                                                                   jobSpec.Files)
                    # addd
                    jobSpecList.append(jobSpec)
                    oldPandaIDs.append(subOldPandaIDs)
                    # incremet index of event service job
                    if taskSpec.useEventService():
                        esIndex += 1
            # return
            return Interaction.SC_SUCCEEDED,jobSpecList,datasetToRegister,oldPandaIDs
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('{0}.doGenerate() failed with {1}:{2}'.format(self.__class__.__name__,
                                                                       errtype.__name__,errvalue))
            return failedRet



    # generate build jobs
    def doGenerateBuild(self,taskSpec,cloudName,siteName,taskParamMap,tmpLog,simul=False):
        # return for failure
        failedRet = Interaction.SC_FAILED,None,None,None
        try:
            datasetToRegister = []
            # get lib.tgz file
            tmpStat,fileSpec,datasetSpec = self.taskBufferIF.getBuildFileSpec_JEDI(taskSpec.jediTaskID,siteName)
            # failed
            if not tmpStat:
                tmpLog.error('failed to get lib.tgz for jediTaskID={0} siteName={0}'.format(taskSpec.jediTaskID,siteName))
                return failedRet
            # lib.tgz is already available
            if fileSpec != None:
                pandaFileSpec = fileSpec.convertToJobFileSpec(datasetSpec,setType='input')
                pandaFileSpec.dispatchDBlock = pandaFileSpec.dataset
                if fileSpec.status == 'finished':
                    pandaFileSpec.status = 'ready'
                # make dummy jobSpec
                jobSpec = JobSpec()
                jobSpec.addFile(pandaFileSpec)
                return Interaction.SC_SUCCEEDED,None,pandaFileSpec,datasetToRegister
            # make job spec for build
            jobSpec = JobSpec()
            jobSpec.jobDefinitionID  = 0
            jobSpec.jobExecutionID   = 0
            jobSpec.attemptNr        = 0
            jobSpec.maxAttempt       = 0
            jobSpec.jobName          = taskSpec.taskName
            jobSpec.transformation   = taskParamMap['buildSpec']['transPath']
            jobSpec.cmtConfig        = taskSpec.architecture
            if taskSpec.transHome != None:
                jobSpec.homepackage  = re.sub('-(?P<dig>\d)','/\g<dig>',taskSpec.transHome)
                jobSpec.homepackage  = re.sub('\r','',jobSpec.homepackage)
            jobSpec.prodSourceLabel  = taskParamMap['buildSpec']['prodSourceLabel']
            jobSpec.processingType   = taskSpec.processingType
            jobSpec.jediTaskID       = taskSpec.jediTaskID
            jobSpec.jobsetID         = taskSpec.reqID
            jobSpec.workingGroup     = taskSpec.workingGroup
            jobSpec.countryGroup     = taskSpec.countryGroup
            jobSpec.computingSite    = siteName
            jobSpec.cloud            = cloudName
            jobSpec.VO               = taskSpec.vo
            jobSpec.prodSeriesLabel  = 'pandatest'
            jobSpec.AtlasRelease     = taskSpec.transUses
            jobSpec.AtlasRelease     = re.sub('\r','',jobSpec.AtlasRelease)
            jobSpec.lockedby         = 'jedi'
            jobSpec.workQueue_ID     = taskSpec.workQueue_ID
            jobSpec.metadata         = ''
            # make libDS name
            if datasetSpec == None or datasetSpec.state == 'closed' or fileSpec == None:
                # make new libDS
                reusedDatasetID = None
                libDsName = 'panda.{0}.{1}.lib._{2:06d}'.format(time.strftime('%m%d%H%M%S',time.gmtime()),
                                                                datetime.datetime.utcnow().microsecond,
                                                                jobSpec.jediTaskID)
            else:
                # reuse existing DS
                reusedDatasetID = datasetSpec.datasetID
                libDsName = datasetSpec.datasetName
            jobSpec.destinationDBlock = libDsName
            jobSpec.destinationSE = siteName  
            libDsFileNameBase = libDsName + '.$JEDIFILEID'
            # make lib.tgz
            fileSpec = FileSpec()
            fileSpec.lfn        = libDsFileNameBase + '.lib.tgz'
            fileSpec.type       = 'output'
            fileSpec.attemptNr  = 0
            fileSpec.jediTaskID = taskSpec.jediTaskID
            fileSpec.dataset           = jobSpec.destinationDBlock
            fileSpec.destinationDBlock = jobSpec.destinationDBlock
            fileSpec.destinationSE     = jobSpec.destinationSE
            jobSpec.addFile(fileSpec)
            # make log
            logFileSpec = FileSpec()
            logFileSpec.lfn        = libDsFileNameBase + '.log.tgz'
            logFileSpec.type       = 'log'
            logFileSpec.attemptNr  = 0
            logFileSpec.jediTaskID = taskSpec.jediTaskID
            logFileSpec.dataset           = jobSpec.destinationDBlock
            logFileSpec.destinationDBlock = jobSpec.destinationDBlock
            logFileSpec.destinationSE     = jobSpec.destinationSE
            jobSpec.addFile(logFileSpec)
            # insert lib.tgz file
            tmpStat,fileIdMap = self.taskBufferIF.insertBuildFileSpec_JEDI(jobSpec,reusedDatasetID,simul)
            # failed
            if not tmpStat:
                tmpLog.error('failed to insert libDS for jediTaskID={0} siteName={0}'.format(taskSpec.jediTaskID,siteName))
                return failedRet
            # set attributes
            for tmpFile in jobSpec.Files:
                tmpFile.fileID = fileIdMap[tmpFile.lfn]['fileID']
                tmpFile.datasetID = fileIdMap[tmpFile.lfn]['datasetID']
                tmpFile.scope = fileIdMap[tmpFile.lfn]['scope']
                # set new LFN where place holder is replaced
                tmpFile.lfn = fileIdMap[tmpFile.lfn]['newLFN']
                if not tmpFile.datasetID in datasetToRegister:
                    datasetToRegister.append(tmpFile.datasetID)
            # parameter map
            paramMap = {}
            paramMap['OUT']  = fileSpec.lfn
            paramMap['IN']   = taskParamMap['buildSpec']['archiveName']
            paramMap['SURL'] = taskParamMap['sourceURL']
            # job parameter
            jobSpec.jobParameters = self.makeBuildJobParameters(taskParamMap['buildSpec']['jobParameters'],
                                                                paramMap)
            # make file spec which will be used by runJobs
            runFileSpec = copy.copy(fileSpec)
            runFileSpec.dispatchDBlock = fileSpec.dataset
            runFileSpec.destinationDBlock = None
            runFileSpec.type = 'input'
            # return
            return Interaction.SC_SUCCEEDED,jobSpec,runFileSpec,datasetToRegister
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('{0}.doGenerateBuild() failed with {1}:{2}'.format(self.__class__.__name__,
                                                                            errtype.__name__,errvalue))
            return failedRet



    # generate preprocessing jobs
    def doGeneratePrePro(self,taskSpec,cloudName,siteName,taskParamMap,inSubChunks,tmpLog,simul=False):
        # return for failure
        failedRet = Interaction.SC_FAILED,None,None
        try:
            # make job spec for preprocessing
            jobSpec = JobSpec()
            jobSpec.jobDefinitionID  = 0
            jobSpec.jobExecutionID   = 0
            jobSpec.attemptNr        = 0
            jobSpec.maxAttempt       = 0
            jobSpec.jobName          = taskSpec.taskName
            jobSpec.transformation   = taskParamMap['preproSpec']['transPath']
            """
            jobSpec.cmtConfig        = taskSpec.architecture
            jobSpec.homepackage      = re.sub('-(?P<dig>\d)','/\g<dig>',taskSpec.transHome)
            jobSpec.homepackage      = re.sub('\r','',jobSpec.homepackage)
            """
            jobSpec.prodSourceLabel  = taskParamMap['preproSpec']['prodSourceLabel']
            jobSpec.processingType   = taskSpec.processingType
            jobSpec.jediTaskID       = taskSpec.jediTaskID
            jobSpec.jobsetID         = taskSpec.reqID
            jobSpec.workingGroup     = taskSpec.workingGroup
            jobSpec.countryGroup     = taskSpec.countryGroup
            jobSpec.computingSite    = siteName
            jobSpec.cloud            = cloudName
            jobSpec.VO               = taskSpec.vo
            jobSpec.prodSeriesLabel  = 'pandatest'
            """
            jobSpec.AtlasRelease     = taskSpec.transUses
            jobSpec.AtlasRelease     = re.sub('\r','',jobSpec.AtlasRelease)
            """
            jobSpec.lockedby         = 'jedi'
            jobSpec.workQueue_ID     = taskSpec.workQueue_ID
            jobSpec.destinationSE    = siteName
            jobSpec.metadata         = ''
            # get log file
            outSubChunk,serialNr,datasetToRegister,siteDsMap = self.taskBufferIF.getOutputFiles_JEDI(taskSpec.jediTaskID,
                                                                                                     None,
                                                                                                     simul,
                                                                                                     True,
                                                                                                     siteName,
                                                                                                     False,
                                                                                                     True)
            if outSubChunk == None:
                # failed
                tmpLog.error('doGeneratePrePro failed to get OutputFiles')
                return failedRet
            outDsMap = {}
            for tmpFileSpec in outSubChunk.values():
                # get dataset
                if not outDsMap.has_key(tmpFileSpec.datasetID):
                    tmpStat,tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID,
                                                                                 tmpFileSpec.datasetID)
                    # not found
                    if not tmpStat:
                        tmpLog.error('doGeneratePrePro failed to get logDS with datasetID={0}'.format(tmpFileSpec.datasetID))
                        return failedRet
                    outDsMap[tmpFileSpec.datasetID] = tmpDataset 
                # convert to job's FileSpec     
                tmpDatasetSpec = outDsMap[tmpFileSpec.datasetID]
                tmpOutFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec,setType='log')
                jobSpec.addFile(tmpOutFileSpec)
                jobSpec.destinationDBlock = tmpDatasetSpec.datasetName
            # make pseudo input
            tmpDatasetSpec,tmpFileSpecList = inSubChunks[0][0]
            tmpFileSpec = tmpFileSpecList[0]
            tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec,
                                                             setType='pseudo_input')
            jobSpec.addFile(tmpInFileSpec)
            # parameter map
            paramMap = {}
            paramMap['SURL'] = taskParamMap['sourceURL']
            # job parameter
            jobSpec.jobParameters = self.makeBuildJobParameters(taskParamMap['preproSpec']['jobParameters'],
                                                                paramMap)
            # return
            return Interaction.SC_SUCCEEDED,jobSpec,datasetToRegister
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('{0}.doGeneratePrePro() failed with {1}:{2}'.format(self.__class__.__name__,
                                                                             errtype.__name__,errvalue))
            return failedRet



    # make job parameters
    def makeJobParameters(self,taskSpec,inSubChunk,outSubChunk,serialNr,paramList,jobSpec,simul,
                          taskParamMap,isMerging,jobFileList):
        if not isMerging:
            parTemplate = taskSpec.jobParamsTemplate
        else:
            # use params template for merging
            parTemplate = taskParamMap['mergeSpec']['jobParameters']
        # make the list of stream/LFNs
        streamLFNsMap = {}
        # parameters for placeholders
        skipEvents = None
        maxEvents  = None
        firstEvent = None
        rndmSeed   = None
        sourceURL  = None
        # source URL
        if taskParamMap != None and taskParamMap.has_key('sourceURL'):
            sourceURL = taskParamMap['sourceURL']
        # get random seed
        if taskSpec.useRandomSeed() and not isMerging:
            tmpStat,randomSpecList = self.taskBufferIF.getRandomSeed_JEDI(taskSpec.jediTaskID,simul)
            if tmpStat == True:
                tmpRandomFileSpec,tmpRandomDatasetSpec = randomSpecList 
                if tmpRandomFileSpec!= None:
                    tmpJobFileSpec = tmpRandomFileSpec.convertToJobFileSpec(tmpRandomDatasetSpec,
                                                                            setType='pseudo_input')
                    rndmSeed = tmpRandomFileSpec.firstEvent + taskSpec.getRndmSeedOffset()
                    jobSpec.addFile(tmpJobFileSpec)
        # input
        for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
            # stream name
            streamName = tmpDatasetSpec.streamName
            # LFNs
            tmpLFNs = []
            for tmpFileSpec in tmpFileSpecList:
                tmpLFNs.append(tmpFileSpec.lfn)
            tmpLFNs.sort()
            # change stream name and LFNs for PFN list
            if taskSpec.useListPFN() and tmpDatasetSpec.isMaster() and tmpDatasetSpec.isPseudo():
                streamName = 'IN'
                tmpPFNs = []
                for tmpLFN in tmpLFNs:
                    tmpPFN = taskParamMap['pfnList'][long(tmpLFN.split(':')[0])]
                    tmpPFNs.append(tmpPFN)
                tmpLFNs = tmpPFNs
            # add
            streamLFNsMap[streamName] = tmpLFNs
            # collect parameters for event-level split
            if tmpDatasetSpec.isMaster():
                # skipEvents and firstEvent
                if len(tmpFileSpecList) > 0:
                    firstEvent = tmpFileSpecList[0].firstEvent
                    if tmpFileSpecList[0].startEvent != None:
                        skipEvents = tmpFileSpecList[0].startEvent
                        # maxEvents
                        maxEvents = 0
                        for tmpFileSpec in tmpFileSpecList:
                            if tmpFileSpec.startEvent != None and tmpFileSpec.endEvent != None:
                                maxEvents += (tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1) 
        # output
        for streamName,tmpFileSpec in outSubChunk.iteritems():
            streamLFNsMap[streamName] = [tmpFileSpec.lfn]
        # extract place holders with range expression, e.g., IN[0:2] 
        for tmpMatch in re.finditer('\$\{([^\}]+)\}',parTemplate):
            tmpPatt = tmpMatch.group(1)
            # split to stream name and range expression
            tmpStRaMatch = re.search('([^\[]+)(.*)',tmpPatt)
            if tmpStRaMatch != None:
                tmpStream = tmpStRaMatch.group(1)
                tmpRange  = tmpStRaMatch.group(2)
                if tmpPatt != tmpStream and streamLFNsMap.has_key(tmpStream):
                    try:
                        exec "streamLFNsMap['{0}']=streamLFNsMap['{1}']{2}".format(tmpPatt,tmpStream,
                                                                                   tmpRange)
                    except:
                        pass
        # loop over all place holders
        for tmpMatch in re.finditer('\$\{([^\}]+)\}',parTemplate):
            placeHolder = tmpMatch.group(1)
            # remove decorators
            streamNames = placeHolder.split('/')[0]
            streamNameList = streamNames.split(',')
            listLFN = []
            for streamName in streamNameList:
                if streamName in streamLFNsMap:
                    listLFN += streamLFNsMap[streamName] 
            if listLFN != []:
                decorators = re.sub('^'+streamNames,'',placeHolder)
                # long format
                if '/L' in decorators:
                    longLFNs = ''
                    for tmpLFN in listLFN:
                        if '/A' in decorators:
                            longLFNs += "'"
                        longLFNs += tmpLFN
                        if '/A' in decorators:
                            longLFNs += "'"
                        if '/S' in decorators:
                            # use white-space as separator
                            longLFNs += ' '
                        else:
                            longLFNs += ','
                    longLFNs = longLFNs[:-1]
                    parTemplate = parTemplate.replace('${'+placeHolder+'}',longLFNs)
                    continue
                # list to string
                if '/T' in decorators:
                    parTemplate = parTemplate.replace('${'+placeHolder+'}',str(listLFN))
                    continue
                # single file
                if len(listLFN) == 1:
                    # just replace with the original file name
                    replaceStr = listLFN[0]
                    parTemplate = parTemplate.replace('${'+streamNames+'}',replaceStr)
                    # encoded    
                    encStreamName = streamNames+'/E'
                    replaceStr = urllib.unquote(replaceStr)
                    parTemplate = parTemplate.replace('${'+encStreamName+'}',replaceStr)
                else:
                    # compact format
                    compactLFNs = []
                    # remove attempt numbers
                    for tmpLFN in listLFN:
                        compactLFNs.append(re.sub('\.\d+$','',tmpLFN))
                    # find head and tail to convert file.1.pool,file.2.pool,file.4.pool to file.[1,2,4].pool
                    tmpHead = ''
                    tmpTail = ''
                    tmpLFN0 = compactLFNs[0]
                    tmpLFN1 = compactLFNs[1]
                    fullLFNList = ''
                    for i in range(len(tmpLFN0)):
                        match = re.search('^(%s)' % tmpLFN0[:i],tmpLFN1)
                        if match:
                            tmpHead = match.group(1)
                        match = re.search('(%s)$' % tmpLFN0[-i:],tmpLFN1)
                        if match:
                            tmpTail = match.group(1)
                    # remove numbers : ABC_00,00_XYZ -> ABC_,_XYZ
                    tmpHead = re.sub('\d*$','',tmpHead)
                    tmpTail = re.sub('^\d*','',tmpTail)
                    # create compact paramter
                    compactPar = '%s[' % tmpHead
                    for tmpLFN in compactLFNs:
                        # keep full LFNs
                        fullLFNList += '%s,' % tmpLFN
                        # extract number
                        tmpLFN = re.sub('^%s' % tmpHead,'',tmpLFN)
                        tmpLFN = re.sub('%s$' % tmpTail,'',tmpLFN)
                        compactPar += '%s,' % tmpLFN
                    compactPar = compactPar[:-1]
                    compactPar += ']%s' % tmpTail
                    fullLFNList = fullLFNList[:-1]
                    # check contents in []
                    conMatch = re.search('\[([^\]]+)\]',compactPar)
                    if conMatch != None and re.search('^[\d,]+$',conMatch.group(1)) != None:
                        # replace with compact format
                        replaceStr = compactPar
                    else:
                        # replace with full format since [] contains non digits
                        replaceStr = fullLFNList
                    parTemplate = parTemplate.replace('${'+streamNames+'}',replaceStr)
                    # encoded    
                    encStreamName = streamNames+'/E'
                    replaceStr = urllib.unquote(replaceStr)
                    parTemplate = parTemplate.replace('${'+encStreamName+'}',replaceStr)
        # loop over all streams to collect transient and final steams
        transientStreamCombo = {}
        for streamName,listLFN in streamLFNsMap.iteritems():
            # collect transient and final steams
            if streamName != None and streamName.startswith('TRN_'):
                counterStreamName = re.sub('^TRN_','',streamName)
                if counterStreamName in streamLFNsMap:
                    transientStreamCombo[counterStreamName] = {
                        'out': counterStreamName,
                        'in':  streamName
                        }
        # replace params related to transient files
        replaceStrMap = {}
        for streamName,transientStreamMap in transientStreamCombo.iteritems():
            # remove serial number
            streamNameBase = re.sub('\d+$','',streamName)
            # make param
            replaceStr = ''
            for tmpLFN in streamLFNsMap[transientStreamMap['in']]:
                replaceStr += '{0},'.format(tmpLFN)
            replaceStr = replaceStr[:-1]
            replaceStr += ':'
            for tmpLFN in streamLFNsMap[transientStreamMap['out']]:
                replaceStr += '{0},'.format(tmpLFN)
            replaceStr = replaceStr[:-1]
            # concatenate per base stream name
            if not replaceStrMap.has_key(streamNameBase):
                replaceStrMap[streamNameBase] = ''
            replaceStrMap[streamNameBase] += '{0} '.format(replaceStr)
        for streamNameBase,replaceStr in replaceStrMap.iteritems():
            targetName = '${TRN_'+streamNameBase+':'+streamNameBase+'}'
            parTemplate = parTemplate.replace(targetName,replaceStr)
        # replace placeholders for numbers
        for streamName,parVal in [('SN',         serialNr),
                                  ('SN/P',       '{0:06d}'.format(serialNr)),
                                  ('RNDMSEED',   rndmSeed),
                                  ('MAXEVENTS',  maxEvents),
                                  ('SKIPEVENTS', skipEvents),
                                  ('FIRSTEVENT', firstEvent),
                                  ('SURL',       sourceURL),
                                  ] + paramList:
            # ignore undefined
            if parVal == None:
                continue
            # replace
            parTemplate = parTemplate.replace('${'+streamName+'}',str(parVal))
        # replace unmerge files
        for jobFileSpec in jobFileList:
            if jobFileSpec.isUnMergedOutput():
                mergedFileName = re.sub('^panda\.um\.','',jobFileSpec.lfn)
                parTemplate = parTemplate.replace(mergedFileName ,jobFileSpec.lfn)
        # return
        return parTemplate



    # make build/prepro job parameters
    def makeBuildJobParameters(self,jobParameters,paramMap):
        parTemplate = jobParameters
        # replace placeholders
        for streamName,parVal in paramMap.iteritems():
            # ignore undefined
            if parVal == None:
                continue
            # replace
            parTemplate = parTemplate.replace('${'+streamName+'}',str(parVal))
        # return
        return parTemplate



    # increase event service consumers
    def increaseEventServiceConsumers(self,pandaJobs,nConsumers):
        newPandaJobs = []
        for pandaJob in pandaJobs:
            for iConsumers in range (nConsumers):
                newPandaJob = self.clonePandaJob(pandaJob)
                newPandaJobs.append(newPandaJob)
        # return
        return newPandaJobs
                    


    # close panda job with new specialHandling
    def clonePandaJob(self,pandaJob):
        newPandaJob = copy.copy(pandaJob)
        newPandaJob.Files = []
        for fileSpec in pandaJob.Files:
            newFileSpec = copy.copy(fileSpec)
            # append PandaID as suffix for log files
            if newFileSpec.type == 'log':
                newFileSpec.lfn += '.$PANDAID'
            newPandaJob.addFile(newFileSpec)
        return newPandaJob




########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
             withThrottle=True,execJobs=True):
    p = JobGenerator(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
                     withThrottle,execJobs)
    p.start()
