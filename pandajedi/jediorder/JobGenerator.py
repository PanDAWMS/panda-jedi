import re
import os
import sys
import time
import socket
import random
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client as PandaClient

from JobThrottler import JobThrottler
from JobBroker import JobBroker
from JobSplitter import JobSplitter
from JediKnight import JediKnight

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
                                                                                                         prodSourceLabel,
                                                                                                         self.pid)
                                tmpLog.debug('start {0}'.format(cycleStr))
                                # throttle
                                tmpLog.debug('check throttle with {0}'.format(throttle.getClassName(vo,prodSourceLabel)))
                                try:
                                    tmpSt,thrFlag = throttle.toBeThrottled(vo,prodSourceLabel,cloudName,workQueue,jobStat)
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
                                typicalNumFilesMap = self.taskBufferIF.getTypicalNumInput_JEDI(vo,prodSourceLabel,workQueue,
                                                                                               useResultCache=600)
                                if typicalNumFilesMap == None:
                                    raise RuntimeError,'failed to get typical number of files'
                                # get the list of input 
                                tmpList = self.taskBufferIF.getTasksToBeProcessed_JEDI(self.pid,vo,
                                                                                       workQueue,
                                                                                       prodSourceLabel,
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
                                                                     siteMapper,self.execJobs)
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
        


# thread for real worker
class JobGeneratorThread (WorkerThread):

    # constructor
    def __init__(self,inputList,threadPool,taskbufferIF,ddmIF,siteMapper,execJobs):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.inputList    = inputList
        self.taskBufferIF = taskbufferIF
        self.ddmIF        = ddmIF
        self.siteMapper   = siteMapper
        self.execJobs     = execJobs
        self.numGenJobs   = 0
        

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
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID))
                    tmpLog.info('start with VO={0} cloud={1}'.format(taskSpec.vo,cloudName))
                    readyToSubmitJob = False
                    jobsSubmitted = False
                    # initialize brokerage
                    jobBroker = JobBroker(taskSpec.vo,taskSpec.prodSourceLabel)
                    tmpStat = jobBroker.initializeMods(self.ddmIF.getInterface(taskSpec.vo),
                                                       self.taskBufferIF)
                    if not tmpStat:
                        tmpErrStr = 'failed to initialize JobBroker'
                        tmpLog.error(tmpErrStr)
                        taskSpec.status = 'broken'
                        taskSpec.setErrDiag(tmpErrStr)                        
                    else:    
                        # run brokerage
                        tmpLog.info('run brokerage with {0}'.format(jobBroker.getClassName(taskSpec.vo,
                                                                                           taskSpec.prodSourceLabel)))
                        try:
                            tmpStat,inputChunk = jobBroker.doBrokerage(taskSpec,cloudName,inputChunk)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('brokerage crashed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                        if tmpStat != Interaction.SC_SUCCEEDED:
                            tmpErrStr = 'brokerage failed'
                            tmpLog.error(tmpErrStr)
                            taskSpec.setOnHold()
                            taskSpec.setErrDiag(tmpErrStr)
                        else:
                            # run splitter
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
                            else:
                                # generate jobs
                                tmpLog.info('run job generator')
                                try:
                                    tmpStat,pandaJobs = self.doGenerate(taskSpec,cloudName,subChunks,
                                                                        inputChunk,tmpLog)
                                except:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error('generator crashed with {0}:{1}'.format(errtype.__name__,errvalue))
                                    tmpStat = Interaction.SC_FAILED
                                if tmpStat != Interaction.SC_SUCCEEDED:
                                    tmpErrStr = 'job generation failed'
                                    tmpLog.error(tmpErrStr)
                                    taskSpec.status = 'broken'
                                    taskSpec.setErrDiag(tmpErrStr)
                                else:
                                    readyToSubmitJob = True
                    if readyToSubmitJob:
                        # submit
                        tmpLog.info('submit jobs')
                        resSubmit = self.taskBufferIF.storeJobs(pandaJobs,taskSpec.userName,toPending=True)
                        pandaIDs = []
                        for items in resSubmit:
                            if items[0] != 'NULL':
                                pandaIDs.append(items[0])
                        if len(pandaIDs) == len(pandaJobs):
                            tmpLog.info('successfully submitted {0}/{1}'.format(len(pandaIDs),len(pandaJobs)))
                            if self.execJobs:
                                statExe,retExe = PandaClient.reassignJobs(pandaIDs,forPending=True)
                                tmpLog.info('exec {0} jobs with status={1}'.format(len(pandaIDs),retExe))
                            jobsSubmitted = True
                            if inputChunk.masterDataset != None and \
                                    inputChunk.masterDataset.nFiles > inputChunk.masterDataset.nFilesToBeUsed:
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
                    tmpLog.info('update task.status=%s' % taskSpec.status)
                    taskSpec.lockedBy = None
                    self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})
                    tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('%s.runImpl() failed with %s %s' % (self.__class__.__name__,errtype.__name__,errvalue))


    # generate jobs
    def doGenerate(self,taskSpec,cloudName,inSubChunkList,inputChunk,tmpLog):
        # return for failure
        failedRet = Interaction.SC_FAILED,None
        try:
            # loop over all sub chunks
            jobSpecList = []
            outDsMap = {}
            for tmpInChunk in inSubChunkList:
                siteName    = tmpInChunk['siteName']
                inSubChunks = tmpInChunk['subChunks']
                for inSubChunk in inSubChunks:
                    jobSpec = JobSpec()
                    jobSpec.jobDefinitionID  = 0
                    jobSpec.jobExecutionID   = 0
                    jobSpec.attemptNr        = 0
                    jobSpec.maxAttempt       = 0
                    jobSpec.jobName          = taskSpec.taskName
                    jobSpec.transformation   = taskSpec.transPath
                    jobSpec.cmtConfig        = taskSpec.architecture
                    jobSpec.homepackage      = taskSpec.transHome
                    jobSpec.homepackage      = re.sub('\r','',jobSpec.homepackage)
                    jobSpec.prodSourceLabel  = taskSpec.prodSourceLabel
                    jobSpec.processingType   = taskSpec.processingType
                    jobSpec.computingSite    = siteName
                    jobSpec.jediTaskID       = taskSpec.jediTaskID
                    jobSpec.taskID           = taskSpec.reqID
                    jobSpec.workingGroup     = taskSpec.workingGroup
                    jobSpec.computingSite    = siteName
                    jobSpec.cloud            = cloudName
                    jobSpec.prodSeriesLabel  = 'pandatest'
                    jobSpec.AtlasRelease     = taskSpec.transUses
                    jobSpec.AtlasRelease     = re.sub('\r','',jobSpec.AtlasRelease)
                    jobSpec.maxCpuCount      = taskSpec.walltime
                    jobSpec.maxCpuUnit       = taskSpec.walltimeUnit
                    jobSpec.maxDiskCount     = taskSpec.workDiskCount
                    jobSpec.maxDiskUnit      = taskSpec.workDiskUnit
                    jobSpec.minRamCount      = taskSpec.ramCount
                    jobSpec.minRamUnit       = taskSpec.ramUnit
                    jobSpec.coreCount        = taskSpec.coreCount
                    jobSpec.ipConnectivity   = 'yes'
                    jobSpec.assignedPriority = taskSpec.taskPriority
                    jobSpec.currentPriority  = taskSpec.currentPriority
                    jobSpec.lockedby         = 'jedi'
                    jobSpec.workQueue_ID     = taskSpec.workQueue_ID
                    # inputs
                    prodDBlock = None
                    setProdDBlock = False
                    for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
                        if not tmpDatasetSpec.isPseudo():
                            if tmpDatasetSpec.isMaster():
                                jobSpec.prodDBlock = tmpDatasetSpec.datasetName
                                setProdDBlock = True
                            else:
                                prodDBlock = tmpDatasetSpec.datasetName
                        for tmpFileSpec in tmpFileSpecList:
                            tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec)
                            # set status
                            if tmpFileSpec.locality == 'localdisk':
                                tmpInFileSpec.status = 'ready'
                            elif tmpFileSpec.locality == 'cache':
                                tmpInFileSpec.status = 'cached'
                            jobSpec.addFile(tmpInFileSpec)
                    # use secondary dataset name as prodDBlock
                    if setProdDBlock == False and prodDBlock != None:
                        jobSpec.prodDBlock = prodDBlock
                    # outputs
                    outSubChunk,serialNr = self.taskBufferIF.getOutputFiles_JEDI(taskSpec.jediTaskID)
                    if outSubChunk == None:
                        # failed
                        tmpLog.error('failed to get OutputFiles')
                        return failedRet
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
                        tmpOutFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec)
                        jobSpec.addFile(tmpOutFileSpec)
                    # job parameter
                    jobSpec.jobParameters = self.makeJobParameters(taskSpec,inSubChunk,outSubChunk,serialNr)
                    # addd
                    jobSpecList.append(jobSpec)
            # return
            return Interaction.SC_SUCCEEDED,jobSpecList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('{0}.doGenerate() failed with {1}:{2}'.format(self.__class__.__name__,
                                                                       errtype.__name__,errvalue))
            return failedRet



    # make job parameters
    def makeJobParameters(self,taskSpec,inSubChunk,outSubChunk,serialNr):
        parTemplate = taskSpec.jobParamsTemplate
        # make the list of stream/LFNs
        streamLFNsMap = {}
        # parameters for placeholders
        skipEvents = None
        maxEvents  = None
        firstEvent = None
        rndmSeed   = serialNr + taskSpec.getRndmSeedOffset()
        # input
        for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
            # stream name
            streamName = tmpDatasetSpec.streamName
            # LFNs
            tmpLFNs = []
            for tmpFileSpec in tmpFileSpecList:
                tmpLFNs.append(tmpFileSpec.lfn)
            tmpLFNs.sort()
            # add
            streamLFNsMap[streamName] = tmpLFNs
            # collect parameters for event-level split
            if tmpDatasetSpec.isMaster():
                # skipEvents and firstEvent
                if len(tmpFileSpecList) > 0 and tmpFileSpecList[0].startEvent != None:
                    skipEvents = tmpFileSpecList[0].startEvent
                    firstEvent = tmpFileSpecList[0].firstEvent
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
        # loop over all streams
        for streamName,listLFN in streamLFNsMap.iteritems():
            # ignore pseudo input with streamName=None
            if streamName == None:
                continue
            if len(listLFN) == 1:
                # just replace with the original file name
                parTemplate = parTemplate.replace('${'+streamName+'}',listLFN[0])
            else:
                # remove attempt numbers
                compactLFNs = []
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
                    parTemplate = parTemplate.replace('${'+streamName+'}',compactPar)
                else:
                    # replace with full format since [] contains non digits
                    parTemplate = parTemplate.replace('${'+streamName+'}',fullLFNList)
        # replace placeholders for numbers
        for streamName,parVal in [('SN',         serialNr),
                                  ('RNDMSEED',   rndmSeed),
                                  ('MAXEVENTS',  maxEvents),
                                  ('SKIPEVENTS', skipEvents),
                                  ('FIRSTEVENT', firstEvent),
                                  ]:
            # ignore undefined
            if parVal == None:
                continue
            # replace
            parTemplate = parTemplate.replace('${'+streamName+'}',str(parVal))
        # return
        return parTemplate


        
########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
             withThrottle=True,execJobs=True):
    p = JobGenerator(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
                     withThrottle,execJobs)
    p.start()
