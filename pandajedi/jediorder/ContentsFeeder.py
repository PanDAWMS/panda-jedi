import os
import sys
import time
import uuid
import math
import socket
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedirefine import RefinerUtils
from JediKnight import JediKnight
from TaskGenerator import TaskGenerator

from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to take care of DatasetContents table
class ContentsFeeder (JediKnight):
    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-con'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)


    # main
    def start(self):
        # start base class
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of datasets to feed contents to DB
                        tmpList = self.taskBufferIF.getDatasetsToFeedContents_JEDI(vo,prodSourceLabel)
                        if tmpList == None:
                            # failed
                            logger.error('failed to get the list of datasets to feed contents')
                        else:
                            logger.debug('got %s datasets' % len(tmpList))
                            # put to a locked list
                            dsList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool() 
                            # make workers
                            nWorker = jedi_config.confeeder.nWorkers
                            for iWorker in range(nWorker):
                                thr = ContentsFeederThread(dsList,threadPool,
                                                           self.taskBufferIF,self.ddmIF,
                                                           self.pid)
                                thr.start()
                            # join
                            threadPool.join()
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('failed in %s.start() with %s %s' % (self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.confeeder.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep()



# thread for real worker
class ContentsFeederThread (WorkerThread):

    # constructor
    def __init__(self,taskDsList,threadPool,taskbufferIF,ddmIF,pid):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskDsList = taskDsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = 'contentsfeeder'
        self.pid     = pid



    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskDsList = self.taskDsList.get(nTasks)
                # no more datasets
                if len(taskDsList) == 0:
                    self.logger.debug('%s terminating since no more items' % self.__class__.__name__)
                    return
                # loop over all tasks
                for jediTaskID,dsList in taskDsList:
                    allUpdated = True
                    taskBroken = False
                    taskOnHold = False
                    runningTask = False
                    missingMap = {}
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0}>'.format(jediTaskID))
                    # get task
                    tmpStat,taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID,False,True,self.pid,10)
                    if not tmpStat or taskSpec == None:
                        tmpLog.error('failed to get taskSpec for jediTaskID={0}'.format(jediTaskID))
                        continue
                    try:
                        # get task parameters
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('task param conversion from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
                        taskBroken = True
                    # renaming of parameters
                    if taskParamMap.has_key('nEventsPerInputFile'):
                        taskParamMap['nEventsPerFile'] = taskParamMap['nEventsPerInputFile']
                    # the number of files per job
                    nFilesPerJob = None
                    if taskParamMap.has_key('nFilesPerJob'):
                        nFilesPerJob = taskParamMap['nFilesPerJob']
                    # the number of chunks used by scout 
                    nChunksForScout = 10
                    # load XML
                    if taskSpec.useLoadXML():
                        xmlConfig = taskParamMap['loadXML']
                    else:
                        xmlConfig = None
                    # skip files used by another task
                    if 'skipFilesUsedBy' in taskParamMap:
                        skipFilesUsedBy = taskParamMap['skipFilesUsedBy']
                    else:
                        skipFilesUsedBy = None
                    # check no wait
                    noWaitParent = False
                    parentOutDatasets = set()
                    if taskSpec.noWaitParent() and not taskSpec.parent_tid in [None,taskSpec.jediTaskID]:
                        tmpStat = self.taskBufferIF.checkParentTask_JEDI(taskSpec.parent_tid)
                        if tmpStat == 'running':
                            noWaitParent = True
                            # get output datasets from parent task
                            tmpParentStat,tmpParentOutDatasets = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.parent_tid,
                                                                                                                  ['output','log'])
                            # collect dataset names
                            for tmpParentOutDataset in tmpParentOutDatasets:
                                parentOutDatasets.add(tmpParentOutDataset.datasetName)
                    # loop over all datasets
                    nFilesMaster = 0
                    checkedMaster = False
                    setFrozenTime = True
                    if not taskBroken:
                        ddmIF = self.ddmIF.getInterface(taskSpec.vo) 
                        origNumFiles = None
                        if taskParamMap.has_key('nFiles'):
                            origNumFiles = taskParamMap['nFiles']
                        for datasetSpec in dsList:
                            tmpLog.info('start loop for {0}(id={1})'.format(datasetSpec.datasetName,datasetSpec.datasetID))
                            # get dataset metadata
                            tmpLog.info('get metadata')
                            gotMetadata = False
                            stateUpdateTime = datetime.datetime.utcnow()                    
                            try:
                                if not datasetSpec.isPseudo():
                                    tmpMetadata = ddmIF.getDatasetMetaData(datasetSpec.datasetName)
                                else:
                                    # dummy metadata for pseudo dataset
                                    tmpMetadata = {'state':'closed'}
                                # set mutable when and the dataset is open and parent is running or task is configured to run until the dataset is closed 
                                if (noWaitParent or taskSpec.runUntilClosed()) and \
                                        (tmpMetadata['state'] == 'open' \
                                             or datasetSpec.datasetName in parentOutDatasets \
                                             or datasetSpec.datasetName.split(':')[-1] in parentOutDatasets):
                                    # dummy metadata when parent is running
                                    tmpMetadata = {'state':'mutable'}
                                gotMetadata = True
                            except:
                                errtype,errvalue = sys.exc_info()[:2]
                                tmpLog.error('{0} failed to get metadata to {1}:{2}'.format(self.__class__.__name__,
                                                                                            errtype.__name__,errvalue))
                                if errtype == Interaction.JEDIFatalError:
                                    # fatal error
                                    datasetStatus = 'broken'
                                    taskBroken = True
                                    # update dataset status    
                                    self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                                else:
                                    # temporary error
                                    taskOnHold = True
                                taskSpec.setErrDiag('failed to get metadata for {0}'.format(datasetSpec.datasetName))
                                allUpdated = False
                            else:
                                # get file list specified in task parameters
                                fileList,includePatt,excludePatt = RefinerUtils.extractFileList(taskParamMap,datasetSpec.datasetName)   
                                # get the number of events in metadata
                                if taskParamMap.has_key('getNumEventsInMetadata'):
                                    getNumEvents = True
                                else:
                                    getNumEvents = False
                                # get file list from DDM
                                tmpLog.info('get files')
                                try:
                                    useInFilesWithNewAttemptNr = False
                                    skipDuplicate = not datasetSpec.useDuplicatedFiles()
                                    if not datasetSpec.isPseudo():
                                        if fileList != [] and taskParamMap.has_key('useInFilesInContainer') and \
                                                not datasetSpec.containerName in ['',None]:
                                            # read files from container if file list is specified in task parameters
                                            tmpDatasetName = datasetSpec.containerName
                                        else:
                                            tmpDatasetName = datasetSpec.datasetName
                                        # use long format for LB
                                        longFormat = False
                                        if taskSpec.respectLumiblock():
                                            longFormat = True
                                        tmpRet = ddmIF.getFilesInDataset(tmpDatasetName,
                                                                         getNumEvents=getNumEvents,
                                                                         skipDuplicate=skipDuplicate,
                                                                         longFormat=longFormat
                                                                         )
                                        tmpLog.info('got {0} files in {1}'.format(len(tmpRet),tmpDatasetName))
                                        # remove lost files
                                        tmpLostFiles = ddmIF.findLostFiles(tmpDatasetName,tmpRet)
                                        if tmpLostFiles != {}:
                                            tmpLog.info('found {0} lost files in {1}'.format(len(tmpLostFiles),tmpDatasetName))
                                            for tmpListGUID,tmpLostLFN in tmpLostFiles.iteritems():
                                                tmpLog.info('removed {0}'.format(tmpLostLFN))
                                                del tmpRet[tmpListGUID]
                                    else:
                                        if datasetSpec.isSeqNumber():
                                            # make dummy files for seq_number
                                            if datasetSpec.getNumRecords() != None:
                                                nPFN = datasetSpec.getNumRecords()
                                            elif origNumFiles != None:
                                                nPFN = origNumFiles
                                                if taskParamMap.has_key('nEventsPerJob') and taskParamMap.has_key('nEventsPerFile') \
                                                        and taskParamMap['nEventsPerFile'] > taskParamMap['nEventsPerJob']:
                                                    nPFN = nPFN * taskParamMap['nEventsPerFile'] / taskParamMap['nEventsPerJob']
                                            elif 'nEvents' in taskParamMap and 'nEventsPerJob' in taskParamMap:
                                                nPFN = taskParamMap['nEvents'] / taskParamMap['nEventsPerJob']
                                            elif 'nEvents' in taskParamMap and 'nEventsPerFile' in taskParamMap \
                                                    and 'nFilesPerJob' in taskParamMap:
                                                nPFN = taskParamMap['nEvents'] / taskParamMap['nEventsPerFile'] / taskParamMap['nFilesPerJob']
                                            else:
                                                # the default number of records for seq_number
                                                seqDefNumRecords = 10000
                                                # get nFiles of the master
                                                tmpMasterAtt = self.taskBufferIF.getDatasetAttributes_JEDI(datasetSpec.jediTaskID,
                                                                                                           datasetSpec.masterID,
                                                                                                           ['nFiles'])
                                                # use nFiles of the master as the number of records if it is larger than the default
                                                if tmpMasterAtt['nFiles'] > seqDefNumRecords:
                                                    nPFN = tmpMasterAtt['nFiles']
                                                else:
                                                    nPFN = seqDefNumRecords
                                            tmpRet = {}
                                            # get offset
                                            tmpOffset = datasetSpec.getOffset()
                                            tmpOffset += 1
                                            for iPFN in range(nPFN):
                                                tmpRet[str(uuid.uuid4())] = {'lfn':iPFN+tmpOffset,
                                                                             'scope':None,
                                                                             'filesize':0,
                                                                             'checksum':None,
                                                                             }
                                        elif not taskSpec.useListPFN():
                                            # dummy file list for pseudo dataset
                                            tmpRet = {str(uuid.uuid4()):{'lfn':'pseudo_lfn',
                                                                         'scope':None,
                                                                         'filesize':0,
                                                                         'checksum':None,
                                                                         }
                                                      }
                                        else:
                                            # make dummy file list for PFN list
                                            if taskParamMap.has_key('nFiles'):
                                                nPFN = taskParamMap['nFiles']
                                            else:
                                                nPFN = 1
                                            tmpRet = {}
                                            for iPFN in range(nPFN):
                                                tmpRet[str(uuid.uuid4())] = {'lfn':'{0:06d}:{1}'.format(iPFN,taskParamMap['pfnList'][iPFN].split('/')[-1]),
                                                                             'scope':None,
                                                                             'filesize':0,
                                                                             'checksum':None,
                                                                             }
                                except:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error('failed to get files due to {0}:{1}'.format(self.__class__.__name__,
                                                                                                 errtype.__name__,errvalue))
                                    if errtype == Interaction.JEDIFatalError:
                                        # fatal error
                                        datasetStatus = 'broken'
                                        taskBroken = True
                                        # update dataset status    
                                        self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                                    else:
                                        # temporary error
                                        taskOnHold = True
                                    taskSpec.setErrDiag('failed to get files for {0}'.format(datasetSpec.datasetName))
                                    allUpdated = False
                                else:
                                    # parameters for master input
                                    respectLB = False
                                    useRealNumEvents = False
                                    if datasetSpec.isMaster():
                                        # respect LB boundaries
                                        respectLB = taskSpec.respectLumiblock()
                                        # use real number of events
                                        useRealNumEvents = taskSpec.useRealNumEvents()
                                    # the number of events per file
                                    nEventsPerFile  = None
                                    nEventsPerJob   = None
                                    nEventsPerRange = None
                                    tgtNumEventsPerJob = None
                                    if (datasetSpec.isMaster() and (taskParamMap.has_key('nEventsPerFile') or useRealNumEvents)) or \
                                            (datasetSpec.isPseudo() and taskParamMap.has_key('nEvents') and not datasetSpec.isSeqNumber()):
                                        if taskParamMap.has_key('nEventsPerFile'):
                                            nEventsPerFile = taskParamMap['nEventsPerFile']
                                        elif datasetSpec.isPseudo() and taskParamMap.has_key('nEvents'):
                                            # use nEvents as nEventsPerFile for pseudo input
                                            nEventsPerFile = taskParamMap['nEvents']
                                        if taskParamMap.has_key('nEventsPerJob'):
                                            nEventsPerJob = taskParamMap['nEventsPerJob']
                                        elif taskParamMap.has_key('nEventsPerRange'):
                                            nEventsPerRange = taskParamMap['nEventsPerRange']
                                        if 'tgtNumEventsPerJob' in taskParamMap:
                                            tgtNumEventsPerJob = taskParamMap['tgtNumEventsPerJob']
                                            # reset nEventsPerJob
                                            nEventsPerJob = None
                                    # max attempts
                                    maxAttempt = None
                                    maxFailure = None
                                    if datasetSpec.isMaster() or datasetSpec.toKeepTrack():
                                        # max attempts 
                                        if taskSpec.disableAutoRetry():
                                            # disable auto retry 
                                            maxAttempt = 1
                                        elif taskParamMap.has_key('maxAttempt'):
                                            maxAttempt = taskParamMap['maxAttempt']
                                        else:
                                            # use default value
                                            maxAttempt = 3
                                        # max failure
                                        if 'maxFailure' in taskParamMap:
                                            maxFailure = taskParamMap['maxFailure']
                                    # first event number
                                    firstEventNumber = None
                                    if datasetSpec.isMaster():
                                        # first event number
                                        firstEventNumber = 1 + taskSpec.getFirstEventOffset()
                                    # nMaxEvents
                                    nMaxEvents = None 
                                    if datasetSpec.isMaster() and taskParamMap.has_key('nEvents'):
                                        nMaxEvents = taskParamMap['nEvents']
                                    # nMaxFiles
                                    nMaxFiles = None
                                    if taskParamMap.has_key('nFiles'):
                                        if datasetSpec.isMaster():
                                            nMaxFiles = taskParamMap['nFiles']
                                        else:
                                            # calculate for secondary
                                            nMaxFiles = datasetSpec.getNumMultByRatio(origNumFiles)
                                            # multipled by the number of jobs per file for event-level splitting
                                            if nMaxFiles != None and taskParamMap.has_key('nEventsPerFile'):
                                                if taskParamMap.has_key('nEventsPerJob'):
                                                    if taskParamMap['nEventsPerFile'] > taskParamMap['nEventsPerJob']:
                                                        nMaxFiles *= float(taskParamMap['nEventsPerFile'])/float(taskParamMap['nEventsPerJob'])
                                                        nMaxFiles = int(math.ceil(nMaxFiles))
                                                elif taskParamMap.has_key('nEventsPerRange'):
                                                    if taskParamMap['nEventsPerFile'] > taskParamMap['nEventsPerRange']:
                                                        nMaxFiles *= float(taskParamMap['nEventsPerFile'])/float(taskParamMap['nEventsPerRange'])
                                                        nMaxFiles = int(math.ceil(nMaxFiles))
                                    # use scout
                                    useScout = False    
                                    if datasetSpec.isMaster() and taskSpec.useScout() and datasetSpec.status != 'toupdate':
                                        useScout = True
                                    # use files with new attempt numbers    
                                    useFilesWithNewAttemptNr = False
                                    if not datasetSpec.isPseudo() and fileList != [] and taskParamMap.has_key('useInFilesWithNewAttemptNr'):
                                        useFilesWithNewAttemptNr = True
                                    #ramCount
                                    ramCount = 0
                                    if taskParamMap.has_key('ramCount'):
                                        ramCount = taskParamMap['ramCount']
                                    # feed files to the contents table
                                    tmpLog.info('update contents')
                                    retDB,missingFileList,nFilesUnique,diagMap = self.taskBufferIF.insertFilesForDataset_JEDI(datasetSpec,tmpRet,
                                                                                                                              tmpMetadata['state'],
                                                                                                                              stateUpdateTime,
                                                                                                                              nEventsPerFile,
                                                                                                                              nEventsPerJob,
                                                                                                                              maxAttempt,
                                                                                                                              firstEventNumber,
                                                                                                                              nMaxFiles,
                                                                                                                              nMaxEvents,
                                                                                                                              useScout,
                                                                                                                              fileList,
                                                                                                                              useFilesWithNewAttemptNr,
                                                                                                                              nFilesPerJob,
                                                                                                                              nEventsPerRange,
                                                                                                                              nChunksForScout,
                                                                                                                              includePatt,
                                                                                                                              excludePatt,
                                                                                                                              xmlConfig,
                                                                                                                              noWaitParent,
                                                                                                                              taskSpec.parent_tid,
                                                                                                                              self.pid,
                                                                                                                              maxFailure,
                                                                                                                              useRealNumEvents,
                                                                                                                              respectLB,
                                                                                                                              tgtNumEventsPerJob,
                                                                                                                              skipFilesUsedBy,
                                                                                                                              ramCount)
                                    if retDB == False:
                                        taskSpec.setErrDiag('failed to insert files for {0}. {1}'.format(datasetSpec.datasetName,
                                                                                                         diagMap['errMsg']))
                                        allUpdated = False
                                        taskBroken = True
                                        break
                                    elif retDB == None:
                                        # the dataset is locked by another or status is not applicable
                                        allUpdated = False
                                        tmpLog.info('escape since task or dataset is locked')
                                        break
                                    elif missingFileList != []:
                                        # files are missing
                                        tmpErrStr = '{0} files missing in {1}'.format(len(missingFileList),datasetSpec.datasetName)
                                        tmpLog.info(tmpErrStr)
                                        taskSpec.setErrDiag(tmpErrStr)
                                        allUpdated = False
                                        taskOnHold = True
                                        missingMap[datasetSpec.datasetName] = {'datasetSpec':datasetSpec,
                                                                               'missingFiles':missingFileList} 
                                    else:
                                        # reduce the number of files to be read
                                        if taskParamMap.has_key('nFiles'):
                                            if datasetSpec.isMaster():
                                                taskParamMap['nFiles'] -= nFilesUnique
                                        # reduce the number of files for scout
                                        if useScout:
                                            nChunksForScout = diagMap['nChunksForScout']
                                        # number of master input files
                                        if datasetSpec.isMaster():
                                            checkedMaster = True
                                            nFilesMaster += nFilesUnique
                                    # running task
                                    if diagMap['isRunningTask']:
                                        runningTask = True
                                    # no activated pending input for noWait
                                    if noWaitParent and diagMap['nActivatedPending'] == 0 and not (useScout and nChunksForScout == 0) \
                                            and tmpMetadata['state'] != 'closed' and datasetSpec.isMaster():
                                        tmpErrStr = 'insufficient inputs are ready. '
                                        tmpErrStr += diagMap['errMsg']
                                        tmpLog.info(tmpErrStr)
                                        taskSpec.setErrDiag(tmpErrStr)
                                        taskOnHold = True
                                        setFrozenTime = False
                                        break
                            tmpLog.info('end loop')
                    # no mater input
                    if not taskOnHold and not taskBroken and allUpdated and nFilesMaster == 0 and checkedMaster:
                        tmpErrStr = 'no master input files. input dataset is empty'
                        tmpLog.error(tmpErrStr)
                        taskSpec.setErrDiag(tmpErrStr,None)
                        if taskSpec.allowEmptyInput() or noWaitParent:
                            taskOnHold = True
                        else:
                            taskBroken = True
                    # update task status
                    if taskBroken:
                        # task is broken
                        taskSpec.status = 'tobroken'
                        tmpMsg = 'set task.status={0}'.format(taskSpec.status)
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                        allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,pid=self.pid)
                    # change task status unless the task is running
                    if not runningTask:
                        if taskOnHold:
                            if not noWaitParent:
                                # initialize task generator
                                taskGenerator = TaskGenerator(taskSpec.vo,taskSpec.prodSourceLabel)
                                tmpStat = taskGenerator.initializeMods(self.taskBufferIF,
                                                                       self.ddmIF.getInterface(taskSpec.vo))
                                if not tmpStat:
                                    tmpErrStr = 'failed to initialize TaskGenerator'
                                    tmpLog.error(tmpErrStr)
                                    taskSpec.status = 'tobroken'
                                    taskSpec.setErrDiag(tmpErrStr)
                                else:
                                    # make parent tasks if necessary
                                    tmpLog.info('make parent tasks with {0} (if necessary)'.format(taskGenerator.getClassName(taskSpec.vo,
                                                                                                                              taskSpec.prodSourceLabel)))
                                    tmpStat = taskGenerator.doGenerate(taskSpec,taskParamMap,missingFilesMap=missingMap)
                                    if tmpStat == Interaction.SC_FATAL:
                                        # failed to make parent tasks
                                        taskSpec.status = 'tobroken'
                                        tmpLog.error('failed to make parent tasks')
                            # go to pending state
                            if not taskSpec.status in ['broken','tobroken']:
                                taskSpec.setOnHold()
                            tmpMsg = 'set task.status={0}'.format(taskSpec.status)
                            tmpLog.info(tmpMsg)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                            allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,pid=self.pid,setFrozenTime=setFrozenTime)
                        elif allUpdated:
                            # all OK
                            allRet,newTaskStatus = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,getTaskStatus=True,pid=self.pid,
                                                                                                       useWorldCloud=taskSpec.useWorldCloud())
                            tmpMsg = 'set task.status={0}'.format(newTaskStatus)
                            tmpLog.info(tmpMsg)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                    else:
                        # just unlock
                        retUnlock = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID,self.pid)
                        tmpLog.info('unlock task with {0}'.format(retUnlock))
                    tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))


    # update dataset
    def updateDatasetStatus(self,datasetSpec,datasetStatus,tmpLog):
        # update dataset status
        datasetSpec.status   = datasetStatus
        datasetSpec.lockedBy = None
        tmpLog.info('update dataset status with {0}'.format(datasetSpec.status))                    
        self.taskBufferIF.updateDataset_JEDI(datasetSpec,
                                             {'datasetID':datasetSpec.datasetID,
                                              'jediTaskID':datasetSpec.jediTaskID},
                                             lockTask=True)

        


########## lauch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = ContentsFeeder(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
