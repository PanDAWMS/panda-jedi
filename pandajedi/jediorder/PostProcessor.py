import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to do post-processing
class PostProcessor (JediKnight):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-post'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)


    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.info('start')
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # prepare tasks to be finished
                        tmpLog.info('preparing tasks to be finished for vo={0} label={1}'.format(vo,prodSourceLabel))
                        tmpRet = self.taskBufferIF.prepareTasksToBeFinished_JEDI(vo,prodSourceLabel,
                                                                                 jedi_config.postprocessor.nTasks)
                        if tmpRet == None:
                            # failed
                            tmpLog.error('failed to prepare tasks')
                        # get tasks to be finished
                        tmpLog.info('getting tasks to be finished') 
                        criteria = {}
                        criteria['status'] = 'prepared'
                        if self.vo != None:
                            criteria['vo'] = self.vo
                        if self.prodSourceLabel != None:
                            criteria['prodSourceLabel'] = self.prodSourceLabel
                        tmpList = self.taskBufferIF.getTasksToBeFinished_JEDI(vo,prodSourceLabel,self.pid,
                                                                              jedi_config.postprocessor.nTasks)
                        if tmpList == None: 
                            # failed
                            tmpLog.error('failed to get tasks to be finished')
                        else:
                            tmpLog.info('got {0} tasks'.format(len(tmpList)))
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # make workers
                            nWorker = jedi_config.postprocessor.nWorkers
                            for iWorker in range(nWorker):
                                thr = PostProcessorThread(taskList,threadPool,
                                                          self.taskBufferIF,
                                                          self.ddmIF)
                                thr.start()
                            # join
                            threadPool.join()
                tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = 60
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)



# thread for real worker
class PostProcessorThread (WorkerThread):

    # constructor
    def __init__(self,taskList,threadPool,taskbufferIF,ddmIF):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # loop over all tasks
                for taskSpec in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'jediTaskID={0}'.format(taskSpec.jediTaskID))
                    tmpLog.info('start')
                    tmpStat = Interaction.SC_SUCCEEDED
                    # loop over all datasets
                    nFiles = 0
                    nFilesFinished = 0
                    for datasetSpec in taskSpec.datasetSpecList:
                        # validation and correction for output/log datasets
                        if datasetSpec.type in ['output','log']:
                            # do something
                            # FIXME
                            # update dataset
                            datasetSpec.status = 'done'
                            self.taskBufferIF.updateDataset_JEDI(datasetSpec,{'datasetID':datasetSpec.datasetID})
                        # count nFiles
                        if datasetSpec.isMaster():
                            nFiles += datasetSpec.nFiles
                            nFilesFinished += datasetSpec.nFilesFinished
                    # update task status
                    taskSpec.lockedBy = None        
                    if nFiles == nFilesFinished:
                        taskSpec.status = 'finished'
                    elif nFilesFinished == 0:
                        taskSpec.status = 'failed'
                    else:
                        taskSpec.status = 'partial'
                    self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})    
                    # done
                    tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
        


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vo=None,prodSourceLabel=None):
    p = PostProcessor(commuChannel,taskBufferIF,ddmIF,vo,prodSourceLabel)
    p.start()
