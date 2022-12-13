import os
import re
import sys
import time
import uuid
import math
import socket
import datetime
import traceback

from six import iteritems

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedirefine import RefinerUtils
from .JediKnight import JediKnight

from pandajedi.jediconfig import jedi_config

try:
    from idds.client.client import Client as iDDS_Client
    import idds.common.constants
    import idds.common.utils
except ImportError:
    pass


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to take care of DatasetContents table
class ContentsFeeder (JediKnight):
    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}_{2}-con'.format(socket.getfqdn().split('.')[0],os.getpid(),os.getpgrp())
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
                        if tmpList is None:
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
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('failed in %s.start() with %s %s' % (self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.confeeder.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)



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
                    datasetsIdxConsistency = []

                    # get task
                    tmpStat,taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID, False, True, self.pid, 10,
                                                                            clearError=True)
                    if not tmpStat or taskSpec is None:
                        self.logger.debug('failed to get taskSpec for jediTaskID={0}'.format(jediTaskID))
                        continue

                    # make logger
                    try:
                        gshare = '_'.join(taskSpec.gshare.split(' '))
                    except Exception:
                        gshare = 'Undefined'
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0} gshare={1}>'.format(jediTaskID, gshare))

                    try:
                        # get task parameters
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception as e:
                        tmpLog.error('task param conversion from json failed with {}'.format(str(e)))
                        # unlock
                        tmpStat = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID, self.pid)
                        tmpLog.debug('unlocked with {}'.format(tmpStat))
                        continue
                    # renaming of parameters
                    if 'nEventsPerInputFile' in taskParamMap:
                        taskParamMap['nEventsPerFile'] = taskParamMap['nEventsPerInputFile']
                    # the number of files per job
                    nFilesPerJob = taskSpec.getNumFilesPerJob()
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
                    if taskSpec.noWaitParent() and taskSpec.parent_tid not in [None,taskSpec.jediTaskID]:
                        tmpStat = self.taskBufferIF.checkParentTask_JEDI(taskSpec.parent_tid)
                        if tmpStat is None or tmpStat == 'running':
                            noWaitParent = True
                            # get output datasets from parent task
                            tmpParentStat,tmpParentOutDatasets = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.parent_tid,
                                                                                                                  ['output','log'])
                            # collect dataset names
                            for tmpParentOutDataset in tmpParentOutDatasets:
                                parentOutDatasets.add(tmpParentOutDataset.datasetName)
                                if tmpParentOutDataset.containerName:
                                    if tmpParentOutDataset.containerName.endswith('/'):
                                        parentOutDatasets.add(tmpParentOutDataset.containerName)
                                        parentOutDatasets.add(tmpParentOutDataset.containerName[:-1])
                                    else:
                                        parentOutDatasets.add(tmpParentOutDataset.containerName)
                                        parentOutDatasets.add(tmpParentOutDataset.containerName+'/')
                    # loop over all datasets
                    nFilesMaster = 0
                    checkedMaster = False
                    setFrozenTime = True
                    if not taskBroken:
                        ddmIF = self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud)
                        origNumFiles = None
                        if 'nFiles' in taskParamMap:
                            origNumFiles = taskParamMap['nFiles']
                        id_to_container = {}
                        [id_to_container.update({datasetSpec.datasetID: datasetSpec.containerName}) \
                            for datasetSpec in dsList]
                        for datasetSpec in dsList:
                            tmpLog.debug('start loop for {0}(id={1})'.format(datasetSpec.datasetName,datasetSpec.datasetID))
                            # index consistency
                            if datasetSpec.indexConsistent():
                                datasetsIdxConsistency.append(datasetSpec.datasetID)
                            # prestaging
                            if taskSpec.inputPreStaging() and (datasetSpec.isMaster() or datasetSpec.isSeqNumber()):
                                nStaging = self.taskBufferIF.getNumStagingFiles_JEDI(taskSpec.jediTaskID)
                                if nStaging is not None and nStaging == 0 and datasetSpec.nFiles > 0:
                                    inputPreStaging = False
                                else:
                                    inputPreStaging = True
                            else:
                                inputPreStaging = False
                            # get dataset metadata
                            tmpLog.debug('get metadata')
                            gotMetadata = False
                            stateUpdateTime = datetime.datetime.utcnow()
                            try:
                                if not datasetSpec.isPseudo():
                                    tmpMetadata = ddmIF.getDatasetMetaData(datasetSpec.datasetName, ignore_missing=True)
                                else:
                                    # dummy metadata for pseudo dataset
                                    tmpMetadata = {'state':'closed'}
                                # set mutable when the dataset is open and parent is running or task is configured to run until the dataset is closed
                                if (noWaitParent or taskSpec.runUntilClosed() or inputPreStaging) and \
                                        (tmpMetadata['state'] == 'open'
                                        or datasetSpec.datasetName in parentOutDatasets
                                        or datasetSpec.datasetName.split(':')[-1] in parentOutDatasets
                                        or inputPreStaging):
                                    # dummy metadata when parent is running
                                    tmpMetadata = {'state':'mutable'}
                                gotMetadata = True
                            except Exception:
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
                                    if not taskSpec.ignoreMissingInDS():
                                        # temporary error
                                        taskOnHold = True
                                    else:
                                        # ignore missing
                                        datasetStatus = 'failed'
                                        # update dataset status
                                        self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                                taskSpec.setErrDiag('failed to get metadata for {0}'.format(datasetSpec.datasetName))
                                if not taskSpec.ignoreMissingInDS():
                                    allUpdated = False
                            else:
                                # to skip missing dataset
                                if tmpMetadata['state'] == 'missing':
                                    # ignore missing
                                    datasetStatus = 'finished'
                                    # update dataset status
                                    self.updateDatasetStatus(datasetSpec, datasetStatus, tmpLog, 'closed')
                                    tmpLog.debug('disabled missing {0}'.format(datasetSpec.datasetName))
                                    continue
                                # get file list specified in task parameters
                                if taskSpec.is_work_segmented() and not datasetSpec.isPseudo() and \
                                        not datasetSpec.isMaster():
                                    fileList = []
                                    includePatt = []
                                    excludePatt = []
                                    try:
                                        segment_id = int(id_to_container[datasetSpec.masterID].split('/')[-1])
                                        for item in taskParamMap['segmentSpecs']:
                                            if item['id'] == segment_id:
                                                if 'files' in item:
                                                    fileList = item['files']
                                                elif 'datasets' in item:
                                                    for tmpDatasetName in item['datasets']:
                                                        tmpRet = ddmIF.getFilesInDataset(tmpDatasetName)
                                                        fileList += [tmpAttr['lfn'] for tmpAttr in tmpRet.values()]
                                    except Exception:
                                        pass
                                else:
                                    fileList,includePatt,excludePatt = RefinerUtils.extractFileList(
                                        taskParamMap, datasetSpec.datasetName)
                                # get the number of events in metadata
                                if 'getNumEventsInMetadata' in taskParamMap:
                                    getNumEvents = True
                                else:
                                    getNumEvents = False
                                # get file list from DDM
                                tmpLog.debug('get files')
                                try:
                                    useInFilesWithNewAttemptNr = False
                                    skipDuplicate = not datasetSpec.useDuplicatedFiles()
                                    if not datasetSpec.isPseudo():
                                        if fileList != [] and 'useInFilesInContainer' in taskParamMap and \
                                                datasetSpec.containerName not in ['',None]:
                                            # read files from container if file list is specified in task parameters
                                            tmpDatasetName = datasetSpec.containerName
                                        else:
                                            tmpDatasetName = datasetSpec.datasetName
                                        # use long format for LB
                                        longFormat = False
                                        if taskSpec.respectLumiblock() or taskSpec.orderByLB():
                                            longFormat = True
                                        tmpRet = ddmIF.getFilesInDataset(tmpDatasetName,
                                                                         getNumEvents=getNumEvents,
                                                                         skipDuplicate=skipDuplicate,
                                                                         longFormat=longFormat
                                                                         )
                                        tmpLog.debug('got {0} files in {1}'.format(len(tmpRet),tmpDatasetName))
                                        # remove lost files
                                        """
                                        tmpLostFiles = ddmIF.findLostFiles(tmpDatasetName,tmpRet)
                                        if tmpLostFiles != {}:
                                            tmpLog.debug('found {0} lost files in {1}'.format(len(tmpLostFiles),tmpDatasetName))
                                            for tmpListGUID,tmpLostLFN in iteritems(tmpLostFiles):
                                                tmpLog.debug('removed {0}'.format(tmpLostLFN))
                                                del tmpRet[tmpListGUID]
                                        """
                                    else:
                                        if datasetSpec.isSeqNumber():
                                            # make dummy files for seq_number
                                            if datasetSpec.getNumRecords() is not None:
                                                nPFN = datasetSpec.getNumRecords()
                                            elif origNumFiles is not None:
                                                nPFN = origNumFiles
                                                if 'nEventsPerFile' in taskParamMap and taskSpec.get_min_granularity():
                                                    nPFN = nPFN * taskParamMap['nEventsPerFile'] // taskSpec.get_min_granularity()
                                                elif 'nEventsPerJob' in taskParamMap and 'nEventsPerFile' in taskParamMap \
                                                        and taskParamMap['nEventsPerFile'] > taskParamMap['nEventsPerJob']:
                                                    nPFN = nPFN * math.ceil(taskParamMap['nEventsPerFile'] / taskParamMap['nEventsPerJob'])
                                            elif 'nEvents' in taskParamMap and 'nEventsPerJob' in taskParamMap:
                                                nPFN = math.ceil(taskParamMap['nEvents'] / taskParamMap['nEventsPerJob'])
                                            elif 'nEvents' in taskParamMap and 'nEventsPerFile' in taskParamMap \
                                                    and taskSpec.getNumFilesPerJob() is not None:
                                                nPFN = math.ceil(taskParamMap['nEvents'] / taskParamMap['nEventsPerFile'] / taskSpec.getNumFilesPerJob())
                                            else:
                                                # the default number of records for seq_number
                                                seqDefNumRecords = 10000
                                                # get nFiles of the master
                                                tmpMasterAtt = self.taskBufferIF.getDatasetAttributes_JEDI(datasetSpec.jediTaskID,
                                                                                                           datasetSpec.masterID,
                                                                                                           ['nFiles'])
                                                # use nFiles of the master as the number of records if it is larger than the default
                                                if 'nFiles' in tmpMasterAtt and tmpMasterAtt['nFiles'] > seqDefNumRecords:
                                                    nPFN = tmpMasterAtt['nFiles']
                                                else:
                                                    nPFN = seqDefNumRecords
                                                # check usedBy
                                                if skipFilesUsedBy is not None:
                                                    for tmpJediTaskID in str(skipFilesUsedBy).split(','):
                                                        tmpParentAtt = self.taskBufferIF.getDatasetAttributesWithMap_JEDI(tmpJediTaskID,
                                                                                                                          {'datasetName':datasetSpec.datasetName},
                                                                                                                          ['nFiles'])
                                                        if 'nFiles' in tmpParentAtt and tmpParentAtt['nFiles']:
                                                            nPFN += tmpParentAtt['nFiles']
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
                                            if 'nFiles' in taskParamMap:
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
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error('failed to get files due to {0}:{1} {2}'.format(self.__class__.__name__,
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
                                    if (datasetSpec.isMaster() and ('nEventsPerFile' in taskParamMap or useRealNumEvents)) or \
                                            (datasetSpec.isPseudo() and 'nEvents' in taskParamMap and not datasetSpec.isSeqNumber()):
                                        if 'nEventsPerFile' in taskParamMap:
                                            nEventsPerFile = taskParamMap['nEventsPerFile']
                                        elif datasetSpec.isMaster() and datasetSpec.isPseudo() and 'nEvents' in taskParamMap:
                                            # use nEvents as nEventsPerFile for pseudo input
                                            nEventsPerFile = taskParamMap['nEvents']
                                        if taskSpec.get_min_granularity():
                                            nEventsPerRange = taskSpec.get_min_granularity()
                                        elif 'nEventsPerJob' in taskParamMap:
                                            nEventsPerJob = taskParamMap['nEventsPerJob']
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
                                        elif 'maxAttempt' in taskParamMap:
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
                                    if datasetSpec.isMaster() and 'nEvents' in taskParamMap:
                                        nMaxEvents = taskParamMap['nEvents']
                                    # nMaxFiles
                                    nMaxFiles = None
                                    if 'nFiles' in taskParamMap:
                                        if datasetSpec.isMaster():
                                            nMaxFiles = taskParamMap['nFiles']
                                        else:
                                            # calculate for secondary
                                            if not datasetSpec.isPseudo():
                                                # check nFilesPerJob
                                                nFilesPerJobSec = datasetSpec.getNumFilesPerJob()
                                                if nFilesPerJobSec is not None:
                                                    nMaxFiles = origNumFiles * nFilesPerJobSec
                                            # check ratio
                                            if nMaxFiles is None:
                                                nMaxFiles = datasetSpec.getNumMultByRatio(origNumFiles)
                                            # multiplied by the number of jobs per file for event-level splitting
                                            if nMaxFiles is not None:
                                                if 'nEventsPerFile' in taskParamMap:
                                                    if taskSpec.get_min_granularity():
                                                        if taskParamMap['nEventsPerFile'] > taskSpec.get_min_granularity():
                                                            nMaxFiles *= float(taskParamMap['nEventsPerFile'])/float(taskSpec.get_min_granularity())
                                                            nMaxFiles = int(math.ceil(nMaxFiles))
                                                    elif 'nEventsPerJob' in taskParamMap:
                                                        if taskParamMap['nEventsPerFile'] > taskParamMap['nEventsPerJob']:
                                                            nMaxFiles *= float(taskParamMap['nEventsPerFile'])/float(taskParamMap['nEventsPerJob'])
                                                            nMaxFiles = int(math.ceil(nMaxFiles))
                                                elif 'useRealNumEvents' in taskParamMap:
                                                    # reset nMaxFiles since it is unknown
                                                    nMaxFiles = None
                                    # use scout
                                    useScout = False
                                    if datasetSpec.isMaster() and taskSpec.useScout() and (datasetSpec.status != 'toupdate' or not taskSpec.isPostScout()):
                                        useScout = True
                                    # use files with new attempt numbers
                                    useFilesWithNewAttemptNr = False
                                    if not datasetSpec.isPseudo() and fileList != [] and 'useInFilesWithNewAttemptNr' in taskParamMap:
                                        useFilesWithNewAttemptNr = True
                                    # ramCount
                                    ramCount = 0
                                    # skip short input
                                    if datasetSpec.isMaster() and not datasetSpec.isPseudo() \
                                            and nEventsPerFile is not None and nEventsPerJob is not None \
                                            and nEventsPerFile >= nEventsPerJob \
                                            and 'skipShortInput' in taskParamMap and taskParamMap['skipShortInput'] is True:
                                        skipShortInput = True
                                    else:
                                        skipShortInput = False
                                    # order by
                                    if taskSpec.order_input_by() and datasetSpec.isMaster() \
                                            and not datasetSpec.isPseudo():
                                        orderBy = taskSpec.order_input_by()
                                    else:
                                        orderBy = None
                                    # feed files to the contents table
                                    tmpLog.debug('update contents')
                                    retDB,missingFileList,nFilesUnique,diagMap = \
                                        self.taskBufferIF.insertFilesForDataset_JEDI(datasetSpec,tmpRet,
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
                                                                                     ramCount,
                                                                                     taskSpec,
                                                                                     skipShortInput,
                                                                                     inputPreStaging,
                                                                                     orderBy)
                                    if retDB is False:
                                        taskSpec.setErrDiag('failed to insert files for {0}. {1}'.format(datasetSpec.datasetName,
                                                                                                         diagMap['errMsg']))
                                        allUpdated = False
                                        taskBroken = True
                                        break
                                    elif retDB is None:
                                        # the dataset is locked by another or status is not applicable
                                        allUpdated = False
                                        tmpLog.debug('escape since task or dataset is locked')
                                        break
                                    elif missingFileList != []:
                                        # files are missing
                                        tmpErrStr = '{0} files missing in {1}'.format(len(missingFileList),datasetSpec.datasetName)
                                        tmpLog.debug(tmpErrStr)
                                        taskSpec.setErrDiag(tmpErrStr)
                                        allUpdated = False
                                        taskOnHold = True
                                        missingMap[datasetSpec.datasetName] = {'datasetSpec':datasetSpec,
                                                                               'missingFiles':missingFileList}
                                    else:
                                        # reduce the number of files to be read
                                        if 'nFiles' in taskParamMap:
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
                                    if (noWaitParent or inputPreStaging) \
                                            and diagMap['nActivatedPending'] == 0 \
                                            and not (useScout and nChunksForScout <= 0) \
                                            and tmpMetadata['state'] != 'closed' and datasetSpec.isMaster():
                                        tmpErrStr = 'insufficient inputs are ready. '
                                        tmpErrStr += diagMap['errMsg']
                                        tmpLog.debug(tmpErrStr)
                                        taskSpec.setErrDiag(tmpErrStr)
                                        taskOnHold = True
                                        setFrozenTime = False
                                        break
                            tmpLog.debug('end loop')
                    # no mater input
                    if not taskOnHold and not taskBroken and allUpdated and nFilesMaster == 0 and checkedMaster:
                        tmpErrStr = 'no master input files. input dataset is empty'
                        tmpLog.error(tmpErrStr)
                        taskSpec.setErrDiag(tmpErrStr,None)
                        if taskSpec.allowEmptyInput() or noWaitParent:
                            taskOnHold = True
                        else:
                            taskBroken = True
                    # index consistency
                    if not taskOnHold and not taskBroken and len(datasetsIdxConsistency) > 0:
                        self.taskBufferIF.removeFilesIndexInconsistent_JEDI(jediTaskID,datasetsIdxConsistency)
                    # update task status
                    if taskBroken:
                        # task is broken
                        taskSpec.status = 'tobroken'
                        tmpMsg = 'set task_status={0}'.format(taskSpec.status)
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg,self.msgType)
                        allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,pid=self.pid)
                    # change task status unless the task is running
                    if not runningTask:
                        # send prestaging request
                        if taskSpec.inputPreStaging() and taskSpec.is_first_contents_feed():
                            tmpStat, tmpErrStr = self.send_prestaging_request(taskSpec, taskParamMap, dsList, tmpLog)
                            if tmpStat:
                                taskSpec.set_first_contents_feed(False)
                            else:
                                tmpLog.debug(tmpErrStr)
                                taskSpec.setErrDiag(tmpErrStr)
                                taskOnHold = True
                        if taskOnHold:
                            # go to pending state
                            if taskSpec.status not in ['broken','tobroken']:
                                taskSpec.setOnHold()
                            tmpMsg = 'set task_status={0}'.format(taskSpec.status)
                            tmpLog.info(tmpMsg)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                            allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,taskSpec,pid=self.pid,setFrozenTime=setFrozenTime)
                        elif allUpdated:
                            # all OK
                            allRet,newTaskStatus = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,getTaskStatus=True,pid=self.pid,
                                                                                                       useWorldCloud=taskSpec.useWorldCloud())
                            tmpMsg = 'set task_status={0}'.format(newTaskStatus)
                            tmpLog.info(tmpMsg)
                            tmpLog.sendMsg(tmpMsg,self.msgType)
                        # just unlock
                        retUnlock = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID,self.pid)
                        tmpLog.debug('unlock not-running task with {0}'.format(retUnlock))
                    else:
                        # just unlock
                        retUnlock = self.taskBufferIF.unlockSingleTask_JEDI(jediTaskID,self.pid)
                        tmpLog.debug('unlock task with {0}'.format(retUnlock))
                    tmpLog.debug('done')
            except Exception as e:
                logger.error('{0} failed in runImpl() with {1}: {2}'.format(self.__class__.__name__, str(e),
                                                                            traceback.format_exc()))


    # update dataset
    def updateDatasetStatus(self,datasetSpec,datasetStatus,tmpLog, datasetState=None):
        # update dataset status
        datasetSpec.status   = datasetStatus
        datasetSpec.lockedBy = None
        if datasetState:
            datasetSpec.state = datasetState
        tmpLog.info('update dataset status to {} state to {}'.format(datasetSpec.status, datasetSpec.state))
        self.taskBufferIF.updateDataset_JEDI(datasetSpec,
                                             {'datasetID':datasetSpec.datasetID,
                                              'jediTaskID':datasetSpec.jediTaskID},
                                             lockTask=True)

    # send prestaging request
    def send_prestaging_request(self, task_spec, task_params_map, ds_list, tmp_log):
        try:
            c = iDDS_Client(idds.common.utils.get_rest_host())
            for datasetSpec in ds_list:
                # get rule
                try:
                    tmp_scope, tmp_name = datasetSpec.datasetName.split(':')
                    tmp_name = re.sub('/$', '', tmp_name)
                except Exception:
                    continue
                try:
                    if 'prestagingRuleID' in task_params_map:
                        if tmp_name not in task_params_map['prestagingRuleID']:
                            continue
                        rule_id = task_params_map['prestagingRuleID'][tmp_name]
                    elif 'selfPrestagingRule' in task_params_map:
                        if not datasetSpec.isMaster() or datasetSpec.isPseudo():
                            continue
                        rule_id = self.ddmIF.getInterface(task_spec.vo, task_spec.cloud).make_staging_rule(
                            tmp_scope + ':' + tmp_name, task_params_map['selfPrestagingRule'])
                        if not rule_id:
                            continue
                    else:
                        rule_id = self.ddmIF.getInterface(task_spec.vo, task_spec.cloud).getActiveStagingRule(
                            tmp_scope + ':' + tmp_name)
                        if rule_id is None:
                            continue
                except Exception as e:
                    return False, 'DDM error : {}'.format(str(e))
                # request
                tmp_log.debug('sending request to iDDS for {0}'.format(datasetSpec.datasetName))
                req = {
                    'scope': tmp_scope,
                    'name': tmp_name,
                    'requester': 'panda',
                    'request_type': idds.common.constants.RequestType.StageIn,
                    'transform_tag': idds.common.constants.RequestType.StageIn.value,
                    'status': idds.common.constants.RequestStatus.New,
                    'priority': 0,
                    'lifetime': 30,
                    'request_metadata': {
                    'workload_id': task_spec.jediTaskID,
                    'rule_id': rule_id,
                    },
                }
                tmp_log.debug('req {0}'.format(str(req)))
                ret = c.add_request(**req)
                tmp_log.debug('got requestID={0}'.format(str(ret)))
        except Exception as e:
            return False, 'iDDS error : {0}'.format(str(e))
        return True, None


########## lauch

def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = ContentsFeeder(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
