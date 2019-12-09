import os
import sys
import time
import socket
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.FactoryBase import FactoryBase
from .JediKnight import JediKnight
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to do post-processing
class PostProcessor (JediKnight,FactoryBase):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = '{0}-{1}-post'.format(socket.getfqdn().split('.')[0],os.getpid())
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        FactoryBase.__init__(self,self.vos,self.prodSourceLabels,logger,
                             jedi_config.postprocessor.modConfig)



    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self,self.taskBufferIF,self.ddmIF)
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
                                                                                 jedi_config.postprocessor.nTasks,
                                                                                 pid=self.pid)
                        if tmpRet is None:
                            # failed
                            tmpLog.error('failed to prepare tasks')
                        # get tasks to be finished
                        tmpLog.info('getting tasks to be finished') 
                        tmpList = self.taskBufferIF.getTasksToBeFinished_JEDI(vo,prodSourceLabel,self.pid,
                                                                              jedi_config.postprocessor.nTasks)
                        if tmpList is None: 
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
                                                          self.ddmIF,
                                                          self)
                                thr.start()
                            # join
                            threadPool.join()
                tmpLog.info('done')
            except Exception:
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
    def __init__(self,taskList,threadPool,taskbufferIF,ddmIF,implFactory):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.implFactory = implFactory


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
                    tmpLog = MsgWrapper(self.logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID))
                    tmpLog.info('start')
                    tmpStat = Interaction.SC_SUCCEEDED
                    # get impl
                    impl = self.implFactory.instantiateImpl(taskSpec.vo,taskSpec.prodSourceLabel,None,
                                                            self.taskBufferIF,self.ddmIF)
                    if impl is None:
                        # post processor is undefined
                        tmpLog.error('post-processor is undefined for vo={0} sourceLabel={1}'.format(taskSpec.vo,taskSpec.prodSourceLabel))
                        tmpStat = Interaction.SC_FATAL
                    # execute    
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('post-process with {0}'.format(impl.__class__.__name__))
                        try:
                            impl.doPostProcess(taskSpec,tmpLog)
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('doPostProcess failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FATAL
                    # done
                    if tmpStat == Interaction.SC_FATAL:
                        # task is broken
                        tmpErrStr = 'post-process failed'
                        tmpLog.error(tmpErrStr)
                        taskSpec.status = 'broken'
                        taskSpec.setErrDiag(tmpErrStr)
                        taskSpec.lockedBy = None
                        self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})    
                    elif tmpStat == Interaction.SC_FAILED:
                        tmpErrStr = 'post processing failed'
                        taskSpec.setOnHold()
                        taskSpec.setErrDiag(tmpErrStr,True)
                        taskSpec.lockedBy = None
                        self.taskBufferIF.updateTask_JEDI(taskSpec,{'jediTaskID':taskSpec.jediTaskID})
                        tmpLog.info('set task_status={0} since {1}'.format(taskSpec.status,taskSpec.errorDialog))
                        continue
                    # final procedure
                    try:
                        impl.doFinalProcedure(taskSpec,tmpLog)
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('doFinalProcedure failed with {0}:{1}'.format(errtype.__name__,errvalue))
                    # done
                    tmpLog.info('done')
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
        


########## launch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = PostProcessor(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
