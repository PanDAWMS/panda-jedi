import sys
import time
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


# worker class to refine TASK_PARAM to fill JEDI tables 
class TaskBroker (JediKnight,FactoryBase):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        FactoryBase.__init__(self,self.vos,self.prodSourceLabels,logger,
                             jedi_config.taskbroker.modConfig)


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
                tmpLog.debug('start TaskBroker')
                # get work queue mapper
                workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                resource_types = self.taskBufferIF.load_resource_types()

                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # loop over all work queues
                        for workQueue in workQueueMapper.getAlignedQueueList(vo, prodSourceLabel):
                            for resource_type in resource_types:
                                wq_name = '_'.join(workQueue.queue_name.split(' '))
                                msgLabel = 'vo={0} label={1} queue={2} resource_type={3}: '.\
                                    format(vo, prodSourceLabel, wq_name, resource_type.resource_name)
                                tmpLog.debug(msgLabel+'start')
                                # get the list of tasks to check
                                tmpList = self.taskBufferIF.getTasksToCheckAssignment_JEDI(vo, prodSourceLabel,
                                                                                           workQueue,
                                                                                           resource_type.resource_name)
                                if tmpList is None:
                                    # failed
                                    tmpLog.error(msgLabel+'failed to get the list of tasks to check')
                                else:
                                    tmpLog.debug(msgLabel+'got tasks_to_check={0}'.format(len(tmpList)))
                                    # put to a locked list
                                    taskList = ListWithLock(tmpList)
                                    # make thread pool
                                    threadPool = ThreadPool()
                                    # make workers
                                    nWorker = jedi_config.taskbroker.nWorkers
                                    for iWorker in range(nWorker):
                                        thr = TaskCheckerThread(taskList,threadPool,
                                                                self.taskBufferIF,
                                                                self.ddmIF,self,
                                                                vo,prodSourceLabel)
                                        thr.start()
                                    # join
                                    threadPool.join()
                                # get the list of tasks to assign
                                tmpList = self.taskBufferIF.getTasksToAssign_JEDI(vo, prodSourceLabel,
                                                                                  workQueue, resource_type.resource_name)
                                if tmpList is None:
                                    # failed
                                    tmpLog.error(msgLabel+'failed to get the list of tasks to assign')
                                else:
                                    tmpLog.debug(msgLabel+'got tasks_to_assign={0}'.format(len(tmpList)))
                                    # put to a locked list
                                    taskList = ListWithLock(tmpList)
                                    # make thread pool
                                    threadPool = ThreadPool()
                                    # make workers
                                    nWorker = jedi_config.taskbroker.nWorkers
                                    for iWorker in range(nWorker):
                                        thr = TaskBrokerThread(taskList,threadPool,
                                                               self.taskBufferIF,
                                                               self.ddmIF,self,
                                                               vo,prodSourceLabel,
                                                               workQueue, resource_type.resource_name)
                                        thr.start()
                                    # join
                                    threadPool.join()
                                tmpLog.debug(msgLabel+'done')
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,
                                                                         errtype.__name__,errvalue))
            tmpLog.debug('done')                                
            # sleep if needed
            loopCycle = jedi_config.taskbroker.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)



# thread for real worker
class TaskCheckerThread (WorkerThread):

    # constructor
    def __init__(self,taskList,threadPool,taskbufferIF,ddmIF,implFactory,
                 vo,prodSourceLabel):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF.getInterface(vo)
        self.implFactory = implFactory
        self.vo = vo
        self.prodSourceLabel = prodSourceLabel


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 100
                taskList = self.taskList.get(nTasks)
                totalTasks,idxTasks = self.taskList.stat()
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # make logger
                tmpLog = MsgWrapper(self.logger)
                tmpLog.info('start TaskCheckerThread {0}/{1} for jediTaskID={2}'.format(idxTasks,totalTasks,taskList))
                tmpStat = Interaction.SC_SUCCEEDED
                # get TaskSpecs
                taskSpecList = []
                for jediTaskID in taskList:
                    tmpRet,taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID,False)
                    if tmpRet and taskSpec is not None:
                        taskSpecList.append(taskSpec)
                    else:
                        tmpLog.error('failed to get taskSpec for jediTaskID={0}'.format(jediTaskID))
                if taskSpecList != []:
                    # get impl                    
                    if tmpStat == Interaction.SC_SUCCEEDED:                    
                        tmpLog.info('getting Impl')
                        try:
                            impl = self.implFactory.getImpl(self.vo,self.prodSourceLabel)
                            if impl is None:
                                # task brokerage is undefined
                                tmpLog.error('task broker is undefined for vo={0} sourceLabel={1}'.format(self.vo,self.prodSourceLabel))
                                tmpStat = Interaction.SC_FAILED
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('getImpl failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                    # check
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('check with {0}'.format(impl.__class__.__name__))
                        try:
                            tmpStat,taskCloudMap = impl.doCheck(taskSpecList)
                        except Exception:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('doCheck failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                    # update
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error('failed to check assignment')
                    else:
                        tmpRet = self.taskBufferIF.setCloudToTasks_JEDI(taskCloudMap)
                        tmpLog.info('done with {0} for {1}'.format(tmpRet,str(taskCloudMap)))
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))



# thread for real worker
class TaskBrokerThread (WorkerThread):

    # constructor
    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, implFactory,
                 vo, prodSourceLabel, workQueue, resource_name):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF.getInterface(vo)
        self.implFactory = implFactory
        self.vo = vo
        self.prodSourceLabel = prodSourceLabel
        self.workQueue = workQueue
        self.resource_name = resource_name

    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 100
                taskList = self.taskList.get(nTasks)
                totalTasks,idxTasks = self.taskList.stat()
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # make logger
                tmpLog = MsgWrapper(self.logger)
                tmpLog.info('start TaskBrokerThread {0}/{1} for jediTaskID={2}'.format(idxTasks,totalTasks,taskList))
                tmpStat = Interaction.SC_SUCCEEDED
                # get TaskSpecs
                tmpListToAssign = []
                for tmpTaskItem in taskList:
                    tmpListItem = self.taskBufferIF.getTasksToBeProcessed_JEDI(None, None, None, None, None,
                                                                               simTasks=[tmpTaskItem],
                                                                               readMinFiles=True)
                    if tmpListItem is None:
                        # failed
                        tmpLog.error('failed to get the input chunks for jediTaskID={0}'.format(tmpTaskItem))
                        tmpStat = Interaction.SC_FAILED
                        break
                    tmpListToAssign += tmpListItem
                # get impl                    
                if tmpStat == Interaction.SC_SUCCEEDED:                    
                    tmpLog.info('getting Impl')
                    try:
                        impl = self.implFactory.getImpl(self.vo,self.prodSourceLabel)
                        if impl is None:
                            # task refiner is undefined
                            tmpLog.error('task broker is undefined for vo={0} sourceLabel={1}'.format(self.vo,self.prodSourceLabel))
                            tmpStat = Interaction.SC_FAILED
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('getImpl failed with {0}:{1}'.format(errtype.__name__,errvalue))
                        tmpStat = Interaction.SC_FAILED
                # brokerage
                if tmpStat == Interaction.SC_SUCCEEDED:
                    tmpLog.info('brokerage with {0} for {1} tasks '.format(impl.__class__.__name__,len(tmpListToAssign)))
                    try:
                        tmpStat = impl.doBrokerage(tmpListToAssign, self.vo,
                                                   self.prodSourceLabel, self.workQueue, self.resource_name)
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('doBrokerage failed with {0}:{1}'.format(errtype.__name__,errvalue))
                        tmpStat = Interaction.SC_FAILED
                # register
                if tmpStat != Interaction.SC_SUCCEEDED:
                    tmpLog.error('failed')
                else:
                    tmpLog.info('done')                    
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
        


########## launch
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = TaskBroker(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
