import sys
import time
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JediKnight import JediKnight

from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec


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
                    nWorker = 3
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
            loopCycle = 60
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)


# thread for real worker
class ContentsFeederThread (WorkerThread):

    # constructor
    def __init__(self,dsList,threadPool,taskbufferIF,ddmIF):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.dsList = dsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nDataset = 10
                dsList = self.dsList.get(nDataset)
                # no more datasets
                if len(dsList) == 0:
                    self.logger.debug('%s terminating since no more datasets' % self.__class__.__name__)
                    return
                # loop over all datasets
                for datasetSpec in dsList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'datasetID={0}'.format(datasetSpec.datasetID))
                    # get file list
                    tmpLog.info('get files in {0}'.format(datasetSpec.datasetName))
                    try:
                        tmpRet = self.ddmIF.getInterface(datasetSpec.vo).getFilesInDataset(datasetSpec.datasetName)
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('{0} failed due to {1}:{2}'.format(self.__class__.__name__,
                                                                        errtype.__name__,errvalue))
                        datasetStatus = 'failedlookup'
                    else:
                        # feed files to the contents table
                        tmpLog.info('update contents')
                        self.taskBufferIF.insertFilesForDataset_JEDI(datasetSpec,tmpRet)
                        datasetStatus = 'ready'
                    # update dataset status
                    datasetSpec.status   = datasetStatus
                    datasetSpec.lockedBy = None
                    tmpLog.info('update dataset status with {0}'.format(datasetSpec.status))                    
                    self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'datasetID':datasetSpec.datasetID})
                    tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))



########## lauch 
                
def launcher(commuChannel,taskBufferIF,ddmIF):
    p = ContentsFeeder(commuChannel,taskBufferIF,ddmIF)
    p.start()
