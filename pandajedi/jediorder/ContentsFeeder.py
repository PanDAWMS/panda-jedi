import sys
import time
import datetime

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
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
                tmpList = self.taskBufferIF.getDatasetsToFeedContentsJEDI()
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
                if dsList == []:
                    self.logger.debug('%s terminating since no more datasets' % self.__class__.__name__)
                    return
                # loop over all datasets
                for taskID,datasetName,datasetID,vo in dsList:
                    # get file list
                    self.logger.debug('getting files in %s' % datasetName)
                    try:
                        tmpRet = self.ddmIF.getInterface(vo).getFilesInDataset(datasetName)
                    except:
                        self.logger.error('%s failed to get file list for %s due to %s' % \
                                          (self.__class__.__name__,datasetName,tmpRet))
                        datasetStatus = 'failedlookup'
                    else:
                        # feed files to the contents table
                        self.taskBufferIF.insertFilesForDatasetJEDI(taskID,datasetID,tmpRet)
                        datasetStatus = 'ready'
                    # update dataset status
                    tmpDsSpec = JediDatasetSpec()
                    tmpDsSpec.status = datasetStatus
                    tmpDsSpec.lockedBy = None
                    self.taskBufferIF.updateDatasetJEDI(tmpDsSpec,{'datasetID':datasetID})
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('%s failed in runImpl() with %s %s' % (self.__class__.__name__,errtype.__name__,errvalue))



########## lauch 
                
def launcher(commuChannel,taskBufferIF,ddmIF):
    p = ContentsFeeder(commuChannel,taskBufferIF,ddmIF)
    p.start()
