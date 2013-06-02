import sys
import time
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedirefine import RefinerUtils
from JediKnight import JediKnight

from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to take care of DatasetContents table
class ContentsFeeder (JediKnight):
    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF):
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)


    # main
    def start(self):
        # start base class
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get the list of datasets to feed contents to DB
                tmpList = self.taskBufferIF.getDatasetsToFeedContents_JEDI()
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
                                                   self.taskBufferIF,self.ddmIF)
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


# thread for real worker
class ContentsFeederThread (WorkerThread):

    # constructor
    def __init__(self,taskDsList,threadPool,taskbufferIF,ddmIF):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskDsList = taskDsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF


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
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0}>'.format(jediTaskID))
                    try:
                        # get task parameters
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('task param conversion from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
                        taskBroken = True
                    # loop over all datasets
                    if not taskBroken:
                        for datasetSpec in dsList:
                            tmpLog.info('start for {0}(id={1})'.format(datasetSpec.datasetName,datasetSpec.datasetID))
                            # get dataset metadata
                            tmpLog.info('get metadata')
                            gotMetadata = False
                            stateUpdateTime = datetime.datetime.utcnow()                    
                            try:
                                tmpMetadata = self.ddmIF.getInterface(datasetSpec.vo).getDatasetMetaData(datasetSpec.datasetName)
                                gotMetadata = True
                            except:
                                errtype,errvalue = sys.exc_info()[:2]
                                tmpLog.error('{0} failed due to {1}:{2}'.format(self.__class__.__name__,
                                                                                errtype.__name__,errvalue))
                                if errtype == Interaction.JEDIFatalError:
                                    datasetStatus = 'broken'
                                else:
                                    datasetStatus = 'pending'
                                # update dataset status    
                                self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                            else:
                                # get file list
                                tmpLog.info('get files')
                                try:
                                    tmpRet = self.ddmIF.getInterface(datasetSpec.vo).getFilesInDataset(datasetSpec.datasetName)
                                except:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error('{0} failed due to {1}:{2}'.format(self.__class__.__name__,
                                                                                    errtype.__name__,errvalue))
                                    if errtype == Interaction.JEDIFatalError:
                                        datasetStatus = 'broken'
                                    else:
                                        datasetStatus = 'pending'
                                    # update dataset status    
                                    self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                                else:
                                    # the number of events per file
                                    if datasetSpec.isMaster() and taskParamMap.has_key('nEventsPerFile'):
                                        nEventsPerFile = taskParamMap['nEventsPerFile']
                                        if taskParamMap.has_key('nEventsPerJob'):
                                            nEventsPerJob = taskParamMap['nEventsPerJob']
                                    else:
                                        nEventsPerFile = None
                                        nEventsPerJob  = None
                                    # max attempts
                                    if datasetSpec.isMaster():
                                        if taskParamMap.has_key('maxAttempt'):
                                            maxAttempt = taskParamMap['maxAttempt']
                                        else:
                                            # use default value
                                            maxAttempt = 5
                                    else:
                                        maxAttempt = None
                                    # feed files to the contents table
                                    tmpLog.info('update contents')
                                    retDB = self.taskBufferIF.insertFilesForDataset_JEDI(datasetSpec,tmpRet,
                                                                                         tmpMetadata['state'],
                                                                                         stateUpdateTime,
                                                                                         nEventsPerFile,
                                                                                         nEventsPerJob,
                                                                                         maxAttempt)
                                    if retDB == False:
                                        datasetStatus = 'pending'
                                        # update dataset status    
                                        self.updateDatasetStatus(datasetSpec,datasetStatus,tmpLog)
                                        allUpdated = False
                                    elif retDB == None:
                                        # the dataset is locked by another or status is not applicable
                                        allUpdated = False
                    # update task status
                    if taskBroken:
                        allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID,'broken')
                    elif allUpdated:
                        allRet = self.taskBufferIF.updateTaskStatusByContFeeder_JEDI(jediTaskID)
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
        self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'datasetID':datasetSpec.datasetID},
                                             lockTask=True)

        


########## lauch 
                
def launcher(commuChannel,taskBufferIF,ddmIF):
    p = ContentsFeeder(commuChannel,taskBufferIF,ddmIF)
    p.start()
