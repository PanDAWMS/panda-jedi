import re
import os
import sys
import time
import copy
import signal
import urllib
import socket
import random
import datetime
import traceback

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread,MapWithLock
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
from pandajedi.jedicore import ParseJobXML
from pandajedi.jedicore import JediCoreUtils
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.dataservice import DataServiceUtils
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
        self.paramsToGetTasks = None
        


    # main
    def start(self):
        # start base class
        #JediKnight.start(self)
        # global thread pool
        globalThreadPool = ThreadPool()
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug('start')
                # get SiteMapper
                siteMapper = self.taskBufferIF.getSiteMapper()
                tmpLog.debug('got siteMapper')
                # get work queue mapper
                workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                tmpLog.debug('got workQueueMapper')
                # get Throttle
                throttle = JobThrottler(self.vos,self.prodSourceLabels)
                throttle.initializeMods(self.taskBufferIF)
                tmpLog.debug('got Throttle')
                # get TaskSetupper
                taskSetupper = TaskSetupper(self.vos,self.prodSourceLabels)
                taskSetupper.initializeMods(self.taskBufferIF,self.ddmIF)
                # loop over all vos
                tmpLog.debug('go into loop')
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
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
                                # check if to lock
                                lockFlag = self.toLockProcess(vo,prodSourceLabel,workQueue.queue_name,cloudName)
                                if lockFlag:
                                    tmpLog.debug('check if to lock')
                                    # lock
                                    flagLocked = self.taskBufferIF.lockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
                                    if not flagLocked:
                                        tmpLog.debug('skip since locked by another process')    
                                        continue
                                # get job statistics
                                tmpSt,jobStat = self.taskBufferIF.getJobStatWithWorkQueuePerCloud_JEDI(vo,prodSourceLabel,cloudName)
                                if not tmpSt:
                                    raise RuntimeError,'failed to get job statistics'
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
                                mergeUnThrottled = None
                                if thrFlag == True:
                                    if self.withThrottle:
                                        tmpLog.debug('throttled')
                                        self.taskBufferIF.unlockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
                                        continue
                                elif thrFlag == False:
                                    # check if the queue has jobs more than nQueueLimit
                                    if self.withThrottle and throttle.lackOfJobs(vo,workQueue.queue_type):
                                        tmpLog.debug('unlock for multiple processes to quickly fill the queue until nQueueLimit is reached')
                                        self.taskBufferIF.unlockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
                                else:
                                    # leveled flag
                                    mergeUnThrottled = not throttle.mergeThrottled(vo,workQueue.queue_type,thrFlag)
                                    if not mergeUnThrottled:
                                        tmpLog.debug('throttled including merge')
                                        if self.withThrottle:
                                            self.taskBufferIF.unlockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
                                            continue
                                    else:
                                        tmpLog.debug('only merge is unthrottled')
                                tmpLog.debug('minPriority={0} maxNumJobs={1}'.format(throttle.minPriority,throttle.maxNumJobs))
                                # get typical number of files
                                typicalNumFilesMap = self.taskBufferIF.getTypicalNumInput_JEDI(vo,workQueue.queue_type,workQueue,
                                                                                               useResultCache=600)
                                if typicalNumFilesMap == None:
                                    raise RuntimeError,'failed to get typical number of files'
                                # get params
                                tmpParamsToGetTasks = self.getParamsToGetTasks(vo,prodSourceLabel,workQueue.queue_name,cloudName)
                                nTasksToGetTasks = tmpParamsToGetTasks['nTasks']
                                nFilesToGetTasks = tmpParamsToGetTasks['nFiles']
                                tmpLog.debug('nTasks={0} nFiles={1} to get tasks'.format(nTasksToGetTasks,nFilesToGetTasks))
                                # get the list of input 
                                tmpList = self.taskBufferIF.getTasksToBeProcessed_JEDI(self.pid,vo,
                                                                                       workQueue,
                                                                                       workQueue.queue_type,
                                                                                       cloudName,
                                                                                       nTasks=nTasksToGetTasks,
                                                                                       nFiles=nFilesToGetTasks,
                                                                                       minPriority=throttle.minPriority,
                                                                                       maxNumJobs=throttle.maxNumJobs,
                                                                                       typicalNumFilesMap=typicalNumFilesMap,
                                                                                       mergeUnThrottled=mergeUnThrottled
                                                                                       )
                                if tmpList == None:
                                    # failed
                                    tmpLog.error('failed to get the list of input chunks to generate jobs')
                                else:
                                    tmpLog.debug('got {0} input tasks'.format(len(tmpList)))
                                    if len(tmpList) != 0: 
                                        # put to a locked list
                                        inputList = ListWithLock(tmpList)
                                        # make thread pool
                                        threadPool = ThreadPool() 
                                        # make lock if nessesary
                                        if lockFlag:
                                            liveCounter = MapWithLock()
                                        else:
                                            liveCounter = None
                                        # make list for brokerage lock
                                        brokerageLockIDs = ListWithLock([])
                                        # make workers
                                        nWorker = jedi_config.jobgen.nWorkers
                                        for iWorker in range(nWorker):
                                            thr = JobGeneratorThread(inputList,threadPool,
                                                                     self.taskBufferIF,self.ddmIF,
                                                                     siteMapper,self.execJobs,
                                                                     taskSetupper,
                                                                     self.pid,
                                                                     workQueue,
                                                                     cloudName,
                                                                     liveCounter,
                                                                     brokerageLockIDs)
                                            globalThreadPool.add(thr)
                                            thr.start()
                                        # join
                                        tmpLog.debug('try to join')
                                        threadPool.join(60*10)
                                        # unlock locks made by brokerage
                                        for brokeragelockID in brokerageLockIDs:
                                            self.taskBufferIF.unlockProcessWithPID_JEDI(vo,prodSourceLabel,workQueue.queue_id,
                                                                                        brokeragelockID,True)
                                        # dump
                                        tmpLog.debug('dump one-time pool : {0}'.format(threadPool.dump()))
                                # unlock
                                self.taskBufferIF.unlockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,
                                                                         errtype.__name__,errvalue))
            # unlock just in case
            try:
                for vo in self.vos:
                    for prodSourceLabel in self.prodSourceLabels:
                        for cloudName in self.cloudList:
                            for workQueue in workQueueList:
                                self.taskBufferIF.unlockProcess_JEDI(vo,prodSourceLabel,cloudName,workQueue.queue_id,self.pid)
            except:
                pass 
            try:
                # clean up global thread pool
                globalThreadPool.clean()
                # dump
                tmpLog.debug('dump global pool : {0}'.format(globalThreadPool.dump()))
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to dump global pool with {0} {1}'.format(errtype.__name__,errvalue))
            tmpLog.debug('end')
            # memory check
            try:
                memLimit = 1.5*1024
                memNow = JediCoreUtils.getMemoryUsage()
                tmpLog.debug('memUsage now {0} MB pid={1}'.format(memNow,os.getpid()))
                if memNow > memLimit:
                    tmpLog.warning('memory limit exceeds {0} > {1} MB pid={2}'.format(memNow,memLimit,
                                                                                      os.getpid()))
                    tmpLog.debug('join')
                    globalThreadPool.join()
                    tmpLog.debug('kill')
                    os.kill(os.getpid(),signal.SIGKILL)
            except:
                pass
            # sleep if needed
            loopCycle = jedi_config.jobgen.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep()



    # get parameters to get tasks
    def getParamsToGetTasks(self,vo,prodSourceLabel,queueName,cloudName):
        paramsList = ['nFiles','nTasks']
        # get group specified params
        if self.paramsToGetTasks == None:
            self.paramsToGetTasks = {}
            # loop over all params
            for paramName in paramsList:
                self.paramsToGetTasks[paramName] = {}
                configParamName = paramName + 'PerGroup'
                # check if param is defined in config
                if hasattr(jedi_config.jobgen,configParamName):
                    for item in jedi_config.jobgen.nFilesPerGroup.split(','):
                        # decompose config params
                        try:
                            tmpVOs,tmpProdSourceLabels,tmpQueueNames,tmpCloudNames,nXYZ = item.split(':')
                            # loop over all VOs
                            for tmpVO in tmpVOs.split('|'):
                                if tmpVO == '':
                                    tmpVO = 'any'
                                self.paramsToGetTasks[paramName][tmpVO] = {}
                                # loop over all labels
                                for tmpProdSourceLabel in tmpProdSourceLabels.split('|'):
                                    if tmpProdSourceLabel == '':
                                        tmpProdSourceLabel = 'any'
                                    self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel] = {}
                                    # loop over all queues
                                    for tmpQueueName in tmpQueueNames.split('|'):
                                        if tmpQueueName == '':
                                            tmpQueueName = 'any'
                                        self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName] = {}
                                        for tmpCloudName in tmpCloudNames.split('|'):
                                            if tmpCloudName == '':
                                                tmpCloudName = 'any'
                                            # add
                                            self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName][tmpCloudName] = long(nXYZ)
                        except:
                            pass
        # make return
        retMap = {}
        for paramName in paramsList:
            # set default
            retMap[paramName] = getattr(jedi_config.jobgen,paramName)
            if paramName in self.paramsToGetTasks:
                # check VO
                if vo in self.paramsToGetTasks[paramName]:
                    tmpVO = vo
                elif 'any' in self.paramsToGetTasks[paramName]:
                    tmpVO = 'any'
                else:
                    continue
                # check label
                if prodSourceLabel in self.paramsToGetTasks[paramName][tmpVO]:
                    tmpProdSourceLabel = prodSourceLabel
                elif 'any' in self.paramsToGetTasks[paramName][tmpVO]:
                    tmpProdSourceLabel = 'any'
                else:
                    continue
                # check queue
                if queueName in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel]:
                    tmpQueueName = queueName
                elif 'any' in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel]:
                    tmpQueueName = 'any'
                else:
                    continue
                # check cloud
                if cloudName in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName]:
                    tmpCloudName = cloudName
                else:
                    tmpCloudName = 'any'
                # set
                retMap[paramName] = self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName][tmpCloudName]
        # return
        return retMap



    # check if lock process
    def toLockProcess(self,vo,prodSourceLabel,queueName,cloudName):
        try:
            # check if param is defined in config
            if hasattr(jedi_config.jobgen,'lockProcess'):
                for item in jedi_config.jobgen.lockProcess.split(','):
                    tmpVo,tmpProdSourceLabel,tmpQueueName,tmpCloudName = item.split(':')
                    if not tmpVo in ['','any',None] and not vo in tmpVo.split('|'):
                        continue
                    if not tmpProdSourceLabel in ['','any',None] and not prodSourceLabel in tmpProdSourceLabel.split('|'):
                        continue
                    if not tmpQueueName in ['','any',None] and not queueName in tmpQueueName.split('|'):
                        continue
                    if not tmpCloudName in ['','any',None] and not cloudName in tmpCloudName.split('|'):
                        continue
                    return True
        except:
            pass
        return False
                



# thread for real worker
class JobGeneratorThread (WorkerThread):

    # constructor
    def __init__(self,inputList,threadPool,taskbufferIF,ddmIF,siteMapper,
                 execJobs,taskSetupper,pid,workQueue,cloud,liveCounter,
                 brokerageLockIDs):
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
        self.buildSpecMap = {}
        self.workQueue    = workQueue
        self.cloud        = cloud
        self.liveCounter  = liveCounter
        self.brokerageLockIDs = brokerageLockIDs



    # main
    def runImpl(self):
        while True:
            try:
                lastJediTaskID = None
                # get a part of list
                nInput = 1
                taskInputList = self.inputList.get(nInput)
                # no more datasets
                if len(taskInputList) == 0:
                    self.logger.debug('{0} terminating after generating {1} jobs since no more inputs '.format(self.__class__.__name__,
                                                                                                               self.numGenJobs))
                    if self.numGenJobs > 0:
                        tmpMsg = ": submitted {0} jobs".format(self.numGenJobs)
                        tmpLog = MsgWrapper(self.logger,monToken='<{0}:{1} cloud={2} queue={3}>'.format(self.workQueue.VO,
                                                                                                        self.workQueue.queue_type,
                                                                                                        self.cloud,
                                                                                                        self.workQueue.queue_name))
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                    return
                # loop over all tasks
                for tmpJediTaskID,inputList in taskInputList:
                    lastJediTaskID = tmpJediTaskID
                    # loop over all inputs
                    for idxInputList,tmpInputItem in enumerate(inputList):
                        taskSpec,cloudName,inputChunk = tmpInputItem
                        # reset map of buildSpec
                        self.buildSpecMap = {}
                        # make logger
                        tmpLog = MsgWrapper(self.logger,'<jediTaskID={0} datasetID={1}>'.format(taskSpec.jediTaskID,
                                                                                                inputChunk.masterIndexName),
                                            monToken='<jediTaskID={0}>'.format(taskSpec.jediTaskID))
                        tmpLog.info('start with VO={0} cloud={1} queue={2}'.format(taskSpec.vo,cloudName,
                                                                                   self.workQueue.queue_name))
                        tmpLog.sendMsg('start to generate jobs',self.msgType)
                        readyToSubmitJob = False
                        jobsSubmitted = False
                        goForward = True
                        taskParamMap = None
                        oldStatus = taskSpec.status
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
                            # set live counter
                            jobBroker.setLiveCounter(taskSpec.vo,taskSpec.prodSourceLabel,self.liveCounter)
                            # set lock ID
                            jobBroker.setLockID(taskSpec.vo,taskSpec.prodSourceLabel,self.pid,self.ident)
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
                            if self.liveCounter != None:
                                tmpLog.info('trying to lock counter')
                                self.liveCounter.acquire() 
                                tmpLog.info('locked counter')
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
                            else:
                                # collect brokerage lock ID
                                brokerageLockID = jobBroker.getBaseLockID(taskSpec.vo,taskSpec.prodSourceLabel)
                                if brokerageLockID != None:
                                    self.brokerageLockIDs.append(brokerageLockID)
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
                                # update counter
                                if self.liveCounter != None:
                                    for tmpSubChunk in subChunks:
                                        self.liveCounter.add(tmpSubChunk['siteName'],len(tmpSubChunk['subChunks']))
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
                        # release lock
                        if self.liveCounter != None:
                            tmpLog.info('release counter')
                            self.liveCounter.release() 
                        # lock task
                        if goForward:
                            tmpLog.info('lock task')
                            tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID,self.pid)
                            if tmpStat == False:
                                tmpLog.info('skip due to lock failure')
                                continue
                        # generate jobs
                        if goForward:
                            tmpLog.info('run job generator')
                            try:
                                tmpStat,pandaJobs,datasetToRegister,oldPandaIDs,parallelOutMap,outDsMap = self.doGenerate(taskSpec,cloudName,subChunks,
                                                                                                                          inputChunk,tmpLog,
                                                                                                                          taskParamMap=taskParamMap)
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
                            tmpStat = self.taskSetupper.doSetup(taskSpec,datasetToRegister,pandaJobs)
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpErrStr = 'failed to setup task'
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr,True)
                            else:
                                readyToSubmitJob = True
                                if taskSpec.toRegisterDatasets():
                                    taskSpec.registeredDatasets()
                        # lock task
                        if goForward:
                            tmpLog.info('lock task')
                            tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID,self.pid)
                            if tmpStat == False:
                                tmpLog.info('skip due to lock failure')
                                continue
                        # submit
                        if readyToSubmitJob:
                            # check if first submission
                            if oldStatus == 'ready' and inputChunk.useScout():
                                firstSubmission = True
                            else:
                                firstSubmission = False
                            # type of relation
                            if inputChunk.isMerging:
                                relationType = 'merge'
                            else:
                                relationType = 'retry'
                            # submit
                            fqans = taskSpec.makeFQANs()
                            tmpLog.info('submit {0} jobs with FQAN={1}'.format(len(pandaJobs),','.join(str(fqan) for fqan in fqans)))
                            resSubmit = self.taskBufferIF.storeJobs(pandaJobs,taskSpec.userName,
                                                                    fqans=fqans,toPending=True,
                                                                    oldPandaIDs=oldPandaIDs,
                                                                    relationType=relationType)
                            pandaIDs = []
                            for idxItem,items in enumerate(resSubmit):
                                if items[0] != 'NULL':
                                    pandaIDs.append(items[0])
                            # check if submission was successful
                            if len(pandaIDs) == len(pandaJobs):
                                tmpMsg = 'successfully submitted {0}/{1} for VO={2} cloud={3} queue={4}'.format(len(pandaIDs),
                                                                                                                len(pandaJobs),
                                                                                                                taskSpec.vo,cloudName,
                                                                                                                self.workQueue.queue_name)
                                tmpLog.info(tmpMsg)
                                tmpLog.sendMsg(tmpMsg,self.msgType)
                                if self.execJobs:
                                    statExe,retExe = PandaClient.reassignJobs(pandaIDs,forPending=True,
                                                                              firstSubmission=firstSubmission)
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
                        # unset lockedBy when all inputs are done for a task
                        if idxInputList+1 == len(inputList):
                            taskSpec.lockedBy = None
                            taskSpec.lockedTime = None
                        else:
                            taskSpec.lockedBy = self.pid
                            taskSpec.lockedTime = datetime.datetime.utcnow()
                        # update task
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
                logger.error('%s.runImpl() failed with %s %s lastJediTaskID=%s' % (self.__class__.__name__,errtype.__name__,errvalue,
                                                                                   lastJediTaskID))



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
        failedRet = Interaction.SC_FAILED,None,None,None,None,None
        # read task parameters
        if taskSpec.useBuild() or taskSpec.usePrePro() or inputChunk.isMerging or \
                taskSpec.useLoadXML() or taskSpec.useListPFN():
            tmpStat,taskParamMap = self.readTaskParams(taskSpec,taskParamMap,tmpLog)
            if not tmpStat:
                return failedRet
        # special priorities 
        scoutPriority = 900
        mergePriority = 5000
        # register datasets
        registerDatasets = taskSpec.toRegisterDatasets()
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
            parallelOutMap = {}
            for tmpInChunk in inSubChunkList:
                siteName      = tmpInChunk['siteName']
                inSubChunks   = tmpInChunk['subChunks']
                siteCandidate = tmpInChunk['siteCandidate']
                siteSpec      = self.siteMapper.getSite(siteName.split(',')[0]) 
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
                tmpJobSpecList = []
                for inSubChunk in inSubChunks:
                    subOldPandaIDs = []
                    jobSpec = JobSpec()
                    jobSpec.jobDefinitionID  = 0
                    jobSpec.jobExecutionID   = 0
                    jobSpec.attemptNr        = self.getLargestAttemptNr(inSubChunk)
                    if taskSpec.disableAutoRetry():
                        # disable server/pilot retry
                        jobSpec.maxAttempt   = -1
                    elif taskSpec.useEventService(siteSpec) and not inputChunk.isMerging:
                        # set max attempt for event service
                        if taskSpec.getMaxAttemptES() == None:
                            jobSpec.maxAttempt = jobSpec.attemptNr + 3
                        else:
                            jobSpec.maxAttempt = jobSpec.attemptNr + taskSpec.getMaxAttemptES()
                    else:
                        jobSpec.maxAttempt   = jobSpec.attemptNr
                    jobSpec.jobName          = taskSpec.taskName + '.$ORIGINPANDAID'
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
                    jobSpec.taskID           = taskSpec.jediTaskID
                    jobSpec.jobsetID         = taskSpec.reqID
                    jobSpec.reqID            = taskSpec.reqID
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
                    jobSpec.maxDiskCount     = taskSpec.getOutDiskSize()
                    jobSpec.maxDiskUnit      = 'MB'
                    jobSpec.minRamCount      = max(taskSpec.ramCount, self.getLargestRamCount(inSubChunk))
                    jobSpec.minRamUnit       = taskSpec.ramUnit
                    if inputChunk.isMerging and taskSpec.mergeCoreCount != None:
                        jobSpec.coreCount    = taskSpec.mergeCoreCount
                    else:
                        jobSpec.coreCount    = taskSpec.coreCount
                    jobSpec.ipConnectivity   = 'yes'
                    jobSpec.metadata         = ''
                    if inputChunk.isMerging:
                        # give higher priority to merge jobs
                        jobSpec.assignedPriority = mergePriority
                    elif inputChunk.useScout():
                        # give higher priority to scouts max(scoutPriority,taskPrio+1)
                        if taskSpec.currentPriority < scoutPriority:
                            jobSpec.assignedPriority = scoutPriority
                        else:
                            jobSpec.assignedPriority = taskSpec.currentPriority + 1
                    else:
                        jobSpec.assignedPriority = taskSpec.currentPriority
                    jobSpec.currentPriority  = jobSpec.assignedPriority
                    jobSpec.lockedby         = 'jedi'
                    jobSpec.workQueue_ID     = taskSpec.workQueue_ID
                    # disable reassign
                    if taskSpec.disableReassign():
                        jobSpec.relocationFlag = 2
                    # using grouping with boundaryID
                    useBoundary = taskSpec.useGroupWithBoundaryID()
                    boundaryID = None
                    # flag for merging
                    isUnMerging = False
                    isMerging = False
                    # special handling
                    specialHandling = ''
                    # DDM backend
                    tmpDdmBackEnd = taskSpec.getDdmBackEnd()
                    if tmpDdmBackEnd != None:
                        if specialHandling == '':
                            specialHandling = 'ddm:{0},'.format(tmpDdmBackEnd)
                        else:
                            specialHandling += ',ddm:{0},'.format(tmpDdmBackEnd)
                    # set specialHandling for Event Service
                    if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging:
                        specialHandling += EventServiceUtils.getHeaderForES(esIndex)
                    # inputs
                    prodDBlock = None
                    setProdDBlock = False
                    totalMasterSize = 0
                    totalMasterEvents = 0
                    totalFileSize = 0
                    lumiBlockNr = None
                    setSpecialHandlingForJC = False
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
                            # set specialHandling for normal Event Service
                            if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging \
                                    and tmpDatasetSpec.isMaster() and not tmpDatasetSpec.isPseudo():
                                if not taskSpec.useJobCloning() or not setSpecialHandlingForJC:
                                    if taskSpec.useJobCloning():
                                        # single event range for job cloning
                                        nEventsPerWorker = tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                                        setSpecialHandlingForJC = True
                                    else:
                                        nEventsPerWorker = taskSpec.getNumEventsPerWorker()
                                    specialHandling += EventServiceUtils.encodeFileInfo(tmpFileSpec.lfn,
                                                                                        tmpFileSpec.startEvent,
                                                                                        tmpFileSpec.endEvent,
                                                                                        nEventsPerWorker,
                                                                                        taskSpec.getMaxAttemptES())
                            # calcurate total master size
                            if tmpDatasetSpec.isMaster():
                                totalMasterSize += JediCoreUtils.getEffectiveFileSize(tmpFileSpec.fsize,tmpFileSpec.startEvent,
                                                                                      tmpFileSpec.endEvent,tmpFileSpec.nEvents)
                                totalMasterEvents += tmpFileSpec.getEffectiveNumEvents()
                            # total file size
                            if tmpInFileSpec.status != 'cached':
                                totalFileSize += tmpFileSpec.fsize
                            # lumi block number
                            if tmpDatasetSpec.isMaster() and lumiBlockNr == None:
                                lumiBlockNr = tmpFileSpec.lumiBlockNr
                        # check if merging 
                        if taskSpec.mergeOutput() and tmpDatasetSpec.isMaster() and not tmpDatasetSpec.toMerge():
                            isUnMerging = True
                    specialHandling = specialHandling[:-1]
                    # using job cloning
                    if setSpecialHandlingForJC:
                        specialHandling = EventServiceUtils.setHeaderForJobCloning(specialHandling,taskSpec.getJobCloningType())
                    # set specialHandling
                    if specialHandling != '':
                        jobSpec.specialHandling = specialHandling
                    # set home cloud
                    if taskSpec.useWorldCloud():
                        jobSpec.setHomeCloud(siteSpec.cloud)
                    # allow partial finish
                    if taskSpec.allowPartialFinish():
                        jobSpec.setToAcceptPartialFinish()
                    # set lumi block number
                    if lumiBlockNr != None:
                        jobSpec.setLumiBlockNr(lumiBlockNr)
                    # use secondary dataset name as prodDBlock
                    if setProdDBlock == False and prodDBlock != None:
                        jobSpec.prodDBlock = prodDBlock
                    # extract middle name
                    middleName = ''
                    if taskSpec.getFieldNumToLFN() != None and not jobSpec.prodDBlock in [None,'NULL','']:
                        if inputChunk.isMerging:
                            # extract from LFN of unmerged files
                            for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
                                if not tmpDatasetSpec.isMaster():
                                    try:
                                        middleName = '.'+tmpFileSpecList[0].lfn.split('.')[4]
                                    except:
                                        pass
                                    break
                        else:
                            # extract from dataset name
                            if len(jobSpec.prodDBlock.split('.')) >= taskSpec.getFieldNumToLFN():
                                middleName = '.'+jobSpec.prodDBlock.split('.')[taskSpec.getFieldNumToLFN()-1]
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
                    # multiply maxCpuCount by total master size
                    try:
                        if jobSpec.maxCpuCount > 0:
                            jobSpec.maxCpuCount *= totalMasterSize
                            jobSpec.maxCpuCount = long(jobSpec.maxCpuCount)
                        else:
                            # negative cpu count to suppress looping job detection
                            jobSpec.maxCpuCount *= -1
                    except:
                        pass
                    # maxWalltime
                    try:
                        if taskSpec.cpuTime != None:
                            jobSpec.maxWalltime = taskSpec.cpuTime
                            if jobSpec.maxWalltime != None and jobSpec.maxWalltime > 0:
                                jobSpec.maxWalltime *= totalMasterEvents
                                if siteSpec.coreCount > 0:
                                    jobSpec.maxWalltime /= float(siteSpec.coreCount)
                                if not siteSpec.corepower in [0,None]:
                                    jobSpec.maxWalltime /= siteSpec.corepower
                            if taskSpec.cpuEfficiency != None:
                                jobSpec.maxWalltime /= (float(taskSpec.cpuEfficiency) / 100.0)
                            if taskSpec.baseWalltime != None:
                                jobSpec.maxWalltime += taskSpec.baseWalltime
                            jobSpec.maxWalltime = long(jobSpec.maxWalltime)
                    except:
                        pass
                    # multiply maxDiskCount by total master size or # of events
                    try:
                        if taskSpec.outputScaleWithEvents():
                            jobSpec.maxDiskCount *= totalMasterSize
                        else:
                            jobSpec.maxDiskCount *= totalMasterEvents
                    except:
                        pass
                    # add offset to maxDiskCount
                    try:
                        jobSpec.maxDiskCount += taskSpec.getWorkDiskSize()
                    except:
                        pass
                    # add input size
                    if taskSpec.useLocalIO() or not siteSpec.isDirectIO():
                        jobSpec.maxDiskCount += totalFileSize
                    # maxDiskCount in MB
                    jobSpec.maxDiskCount /= (1024*1024)
                    jobSpec.maxDiskCount = long(jobSpec.maxDiskCount)
                    # FIXME one maxwdir is used
                    if siteSpec.maxinputsize < jobSpec.maxDiskCount:
                        jobSpec.maxDiskCount = siteSpec.maxinputsize
                    # unset maxCpuCount and minRamCount for merge jobs 
                    if inputChunk.isMerging:
                        if jobSpec.maxCpuCount != [None,'NULL']:
                            jobSpec.maxCpuCount = 0
                        if jobSpec.minRamCount != [None,'NULL']:
                            jobSpec.minRamCount = 0
                    # XML config
                    xmlConfigJob = None
                    if xmlConfig != None:
                        try:
                            xmlConfigJob = xmlConfig.jobs[boundaryID]
                        except:
                            tmpLog.error('failed to get XML config for N={0}'.format(boundaryID))
                            return failedRet
                    # outputs
                    outSubChunk,serialNr,tmpToRegister,siteDsMap,parallelOutMap = self.taskBufferIF.getOutputFiles_JEDI(taskSpec.jediTaskID,
                                                                                                                        provenanceID,
                                                                                                                        simul,
                                                                                                                        instantiateTmpl,
                                                                                                                        instantiatedSite,
                                                                                                                        isUnMerging,
                                                                                                                        False,
                                                                                                                        xmlConfigJob,
                                                                                                                        siteDsMap,
                                                                                                                        middleName,
                                                                                                                        registerDatasets,
                                                                                                                        parallelOutMap)
                    if outSubChunk == None:
                        # failed
                        tmpLog.error('failed to get OutputFiles')
                        return failedRet
                    for tmpToRegisterItem in tmpToRegister:
                        if not tmpToRegisterItem in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                    destinationDBlock = None
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
                                                                          useEventService=taskSpec.useEventService(siteSpec))
                        # stay output on site
                        if taskSpec.stayOutputOnSite():
                            tmpOutFileSpec.destinationSE = siteName
                            tmpOutFileSpec.destinationDBlockToke = 'dst:{0}'.format(siteSpec.ddm)
                        jobSpec.addFile(tmpOutFileSpec)
                        # use the first dataset as destinationDBlock
                        if destinationDBlock == None:
                            destinationDBlock = tmpDatasetSpec.datasetName
                    if destinationDBlock != None:
                        jobSpec.destinationDBlock = destinationDBlock
                    # get datasetSpec for parallel jobs
                    for tmpFileSpecList in parallelOutMap.values():
                        for tmpFileSpec in tmpFileSpecList:
                            if not tmpFileSpec.datasetID in outDsMap:
                                tmpStat,tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID,
                                                                                             tmpFileSpec.datasetID)
                                # not found
                                if not tmpStat:
                                    tmpLog.error('failed to get DS with datasetID={0}'.format(tmpFileSpec.datasetID))
                                    return failedRet
                                outDsMap[tmpFileSpec.datasetID] = tmpDataset
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
                    # middle name
                    paramList.append(('MIDDLENAME',middleName))
                    # job parameter
                    jobSpec.jobParameters = self.makeJobParameters(taskSpec,inSubChunk,outSubChunk,
                                                                   serialNr,paramList,jobSpec,simul,
                                                                   taskParamMap,inputChunk.isMerging,
                                                                   jobSpec.Files,
                                                                   taskSpec.useEventService(siteSpec))
                    # add
                    tmpJobSpecList.append(jobSpec)
                    oldPandaIDs.append(subOldPandaIDs)
                    # incremet index of event service job
                    if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging:
                        esIndex += 1
                # increase event service consumers
                if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging:
                    tmpJobSpecList = self.increaseEventServiceConsumers(tmpJobSpecList,taskSpec.getNumEventServiceConsumer(),
                                                                        taskSpec.getNumSitesPerJob(),parallelOutMap,outDsMap)
                # add to all list
                jobSpecList += tmpJobSpecList
                # sort
                if taskSpec.useEventService() and taskSpec.getNumSitesPerJob():
                    jobSpecList = self.sortParallelJobsBySite(jobSpecList)
            # return
            return Interaction.SC_SUCCEEDED,jobSpecList,datasetToRegister,oldPandaIDs,parallelOutMap,outDsMap
        except:
            tmpLog.error('{0}.doGenerate() failed with {1}'.format(self.__class__.__name__,
                                                                   traceback.format_exc()))
            return failedRet



    # generate build jobs
    def doGenerateBuild(self,taskSpec,cloudName,siteName,taskParamMap,tmpLog,simul=False):
        # return for failure
        failedRet = Interaction.SC_FAILED,None,None,None
        try:
            datasetToRegister = []
            # get sites which share DDM endpoint
            associatedSites = DataServiceUtils.getSitesShareDDM(self.siteMapper,siteName) 
            associatedSites.sort()
            # key for map of buildSpec
            secondKey = [siteName] + associatedSites
            secondKey.sort()
            buildSpecMapKey = (taskSpec.jediTaskID,tuple(secondKey))
            if buildSpecMapKey in self.buildSpecMap:
                # reuse lib.tgz
                reuseDatasetID,reuseFileID = self.buildSpecMap[buildSpecMapKey]
                tmpStat,fileSpec,datasetSpec = self.taskBufferIF.getOldBuildFileSpec_JEDI(taskSpec.jediTaskID,reuseDatasetID,reuseFileID)
            else:
                # get lib.tgz file
                tmpStat,fileSpec,datasetSpec = self.taskBufferIF.getBuildFileSpec_JEDI(taskSpec.jediTaskID,siteName,associatedSites)
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
            jobSpec.reqID            = taskSpec.reqID
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
            # special handling
            specialHandling = ''
            # DDM backend
            tmpDdmBackEnd = taskSpec.getDdmBackEnd()
            if tmpDdmBackEnd != None:
                if specialHandling == '':
                    specialHandling = 'ddm:{0},'.format(tmpDdmBackEnd)
                else:
                    specialHandling += 'ddm:{0},'.format(tmpDdmBackEnd)
            specialHandling = specialHandling[:-1]
            if specialHandling != '':
                jobSpec.specialHandling = specialHandling
            libDsFileNameBase = libDsName + '.$JEDIFILEID'
            # make lib.tgz
            libTgzName = libDsFileNameBase + '.lib.tgz'
            fileSpec = FileSpec()
            fileSpec.lfn        = libTgzName
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
            # append to map of buildSpec
            self.buildSpecMap[buildSpecMapKey] = [fileIdMap[libTgzName]['datasetID'],fileIdMap[libTgzName]['fileID']]
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
            jobSpec.reqID            = taskSpec.reqID
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
            outSubChunk,serialNr,datasetToRegister,siteDsMap,parallelOutMap = self.taskBufferIF.getOutputFiles_JEDI(taskSpec.jediTaskID,
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
                          taskParamMap,isMerging,jobFileList,useEventService):
        if not isMerging:
            parTemplate = taskSpec.jobParamsTemplate
        else:
            # use params template for merging
            parTemplate = taskParamMap['mergeSpec']['jobParameters']
        # make the list of stream/LFNs
        streamLFNsMap = {}
        # parameters for placeholders
        skipEvents = 0
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
                    fullLFNList = ''
                    for tmpLFN in listLFN:
                        # keep full LFNs
                        fullLFNList += '%s,' % tmpLFN
                        compactLFNs.append(re.sub('\.\d+$','',tmpLFN))
                    fullLFNList = fullLFNList[:-1]
                    # find head and tail to convert file.1.pool,file.2.pool,file.4.pool to file.[1,2,4].pool
                    tmpHead = ''
                    tmpTail = ''
                    tmpLFN0 = compactLFNs[0]
                    tmpLFN1 = compactLFNs[1]
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
                        # extract number
                        tmpLFN = re.sub('^%s' % tmpHead,'',tmpLFN)
                        tmpLFN = re.sub('%s$' % tmpTail,'',tmpLFN)
                        compactPar += '%s,' % tmpLFN
                    compactPar = compactPar[:-1]
                    compactPar += ']%s' % tmpTail
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
        for streamName in streamLFNsMap.keys():
            # collect transient and final steams
            if streamName != None and not streamName.startswith('TRN_'):
                counterStreamName = 'TRN_'+streamName
                if not counterStreamName in streamLFNsMap:
                    # empty list
                    streamLFNsMap[counterStreamName] = []
                transientStreamCombo[streamName] = {
                    'out': streamName,
                    'in':  counterStreamName,
                    }
        # replace params related to transient files
        replaceStrMap = {}
        emptyStreamMap = {}
        for streamName,transientStreamMap in transientStreamCombo.iteritems():
            # remove serial number
            streamNameBase = re.sub('\d+$','',streamName)
            # empty streams
            if not streamNameBase in emptyStreamMap:
                emptyStreamMap[streamNameBase] = []
            # make param
            replaceStr = ''
            if streamLFNsMap[transientStreamMap['in']] == []:
                emptyStreamMap[streamNameBase].append(streamName) 
            else:
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
            if targetName in parTemplate:
                parTemplate = parTemplate.replace(targetName,replaceStr)
                # remove putputs with empty input files
                for emptyStream in emptyStreamMap[streamNameBase]:
                    tmpFileIdx = 0
                    for tmpJobFileSpec in jobFileList:
                        if tmpJobFileSpec.lfn in streamLFNsMap[emptyStream]:
                            jobFileList.pop(tmpFileIdx)
                            break
                        tmpFileIdx += 1
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
                parTemplate = parTemplate.replace(mergedFileName,jobFileSpec.lfn)
        # remove duplicated panda.um
        parTemplate = parTemplate.replace('panda.um.panda.um.','panda.um.')
        # remove ES parameters if necessary
        if useEventService:
            parTemplate = parTemplate.replace('<PANDA_ES_ONLY>','')
            parTemplate = parTemplate.replace('</PANDA_ES_ONLY>','')
        else:
            parTemplate = re.sub('<PANDA_ES_ONLY>(.*)</PANDA_ES_ONLY>','',parTemplate)
            parTemplate = re.sub('<PANDA_ESMERGE.*>(.*)</PANDA_ESMERGE.*>','',parTemplate)
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
    def increaseEventServiceConsumers(self,pandaJobs,nConsumers,nSitesPerJob,parallelOutMap,outDsMap):
        newPandaJobs = []
        for pandaJob in pandaJobs:
            for iConsumers in range (nConsumers):
                newPandaJob = self.clonePandaJob(pandaJob,iConsumers,parallelOutMap,outDsMap)
                newPandaJobs.append(newPandaJob)
        # return
        return newPandaJobs
                    


    # close panda job with new specialHandling
    def clonePandaJob(self,pandaJob,index,parallelOutMap,outDsMap):
        newPandaJob = copy.copy(pandaJob)
        sites = newPandaJob.computingSite.split(',')
        nSites = len(sites)
        newPandaJob.Files = []
        for fileSpec in pandaJob.Files:
            if nSites == 1 or not fileSpec.type in ['log','output'] or \
                    (fileSpec.fileID in parallelOutMap and len(parallelOutMap[fileSpec.fileID]) == 1):
                newFileSpec = copy.copy(fileSpec)
                # append PandaID as suffix for log files of normal ES consumers
                if newFileSpec.type == 'log':
                    newFileSpec.lfn += '.$PANDAID'
            else:
                newFileSpec = parallelOutMap[fileSpec.fileID][index % nSites]
                datasetSpec = outDsMap[newFileSpec.datasetID]
                newFileSpec = newFileSpec.convertToJobFileSpec(datasetSpec,useEventService=True)
            newPandaJob.addFile(newFileSpec)
        # set site for parallel jobs
        if nSites > 1:
            newPandaJob.computingSite = sites[index % nSites]
        return newPandaJob



    # sort parallel jobs by site 
    def sortParallelJobsBySite(self,pandaJobs):
        tmpMap = {}
        for pandaJob in pandaJobs:
            if not pandaJob.computingSite in tmpMap:
                tmpMap[pandaJob.computingSite] = []
            tmpMap[pandaJob.computingSite].append(pandaJob)
        newPandaJobs = []
        for computingSite in tmpMap.keys():
            newPandaJobs += tmpMap[computingSite]
        # return
        return newPandaJobs



    # get the largest attempt number
    def getLargestAttemptNr(self,inSubChunk):
        largestAttemptNr = 0
        for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
            if tmpDatasetSpec.isMaster():
                for tmpFileSpec in tmpFileSpecList:
                    if tmpFileSpec.attemptNr != None and tmpFileSpec.attemptNr > largestAttemptNr:
                        largestAttemptNr = tmpFileSpec.attemptNr
        return largestAttemptNr+1


    # get the largest ramCount
    def getLargestRamCount(self,inSubChunk):
        largestRamCount = 0
        for tmpDatasetSpec,tmpFileSpecList in inSubChunk:
            if tmpDatasetSpec.isMaster():
                for tmpFileSpec in tmpFileSpecList:
                    if tmpFileSpec.ramCount != None and tmpFileSpec.ramCount > largestRamCount:
                        largestRamCount = tmpFileSpec.ramCount
        return largestRamCount


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
             withThrottle=True,execJobs=True):
    p = JobGenerator(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels,cloudList,
                     withThrottle,execJobs)
    p.start()
